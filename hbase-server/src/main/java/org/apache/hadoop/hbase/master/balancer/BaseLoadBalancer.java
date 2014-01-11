/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.balancer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.Action.Type;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Sets;

/**
 * The base class for load balancers. It provides the the functions used to by
 * {@link AssignmentManager} to assign regions in the edge cases. It doesn't
 * provide an implementation of the actual balancing algorithm.
 *
 */
public abstract class BaseLoadBalancer implements LoadBalancer {
  private static final int MIN_SERVER_BALANCE = 2;
  private volatile boolean stopped = false;

  protected final RegionLocationFinder regionFinder = new RegionLocationFinder();

  private static class DefaultRackManager extends RackManager {
    @Override
    public String getRack(ServerName server) {
      return UNKNOWN_RACK;
    }
  }

  /**
   * An efficient array based implementation similar to ClusterState for keeping
   * the status of the cluster in terms of region assignment and distribution.
   * To be used by LoadBalancers.
   */
  protected static class Cluster {
    ServerName[] servers;
    String[] hosts; // ServerName uniquely identifies a region server. multiple RS can run on the same host
    String[] racks;

    ArrayList<String> tables;
    HRegionInfo[] regions;
    Deque<RegionLoad>[] regionLoads;

    int[][] regionLocations; //regionIndex -> list of serverIndex sorted by locality

    int[]   serverIndexToHostIndex;      //serverIndex -> host index
    int[]   serverIndexToRackIndex;      //serverIndex -> rack index

    int[][] regionsPerServer;            //serverIndex -> region list
    int[][] regionsPerHost;              //hostIndex -> list of regions
    int[][] regionsPerRack;              //rackIndex -> region list
    int[][] regionsByPrimaryPerServer;   //serverIndex -> sorted list of regions by primary region index
    int[][] regionsByPrimaryPerHost;     //hostIndex -> sorted list of regions by primary region index
    int[][] regionsByPrimaryPerRack;     //rackIndex -> sorted list of regions by primary region index

    int[][] serversPerHost;              //hostIndex -> list of server indexes
    int[][] serversPerRack;              //rackIndex -> list of server indexes
    int[]   regionIndexToServerIndex;    //regionIndex -> serverIndex
    int[]   initialRegionIndexToServerIndex;    //regionIndex -> serverIndex (initial cluster state)
    int[]   regionIndexToTableIndex;     //regionIndex -> tableIndex
    int[][] numRegionsPerServerPerTable; //serverIndex -> tableIndex -> # regions
    int[]   numMaxRegionsPerTable;       //tableIndex -> max number of regions in a single RS
    int[]   regionIndexToPrimaryIndex;   //regionIndex -> regionIndex of the primary

    Integer[] serverIndicesSortedByRegionCount;

    Map<String, Integer> serversToIndex;
    Map<String, Integer> hostsToIndex;
    Map<String, Integer> racksToIndex;
    Map<String, Integer> tablesToIndex;
    Map<HRegionInfo, Integer> regionsToIndex;

    int numServers;
    int numHosts;
    int numRacks;
    int numTables;
    int numRegions;

    int numMovedRegions = 0; //num moved regions from the initial configuration
    int numMovedMetaRegions = 0;       //num of moved regions that are META

    protected final RackManager rackManager;

    protected Cluster(
        Map<ServerName, List<HRegionInfo>> clusterState,
        Map<String, Deque<RegionLoad>> loads,
        RegionLocationFinder regionFinder,
        RackManager rackManager) {
      this(null, clusterState, loads, regionFinder, rackManager);
    }

    protected Cluster(
        Collection<HRegionInfo> unassignedRegions,
        Map<ServerName, List<HRegionInfo>> clusterState,
        Map<String, Deque<RegionLoad>> loads,
        RegionLocationFinder regionFinder,
        RackManager rackManager) {

      if (unassignedRegions == null) {
        unassignedRegions = new ArrayList<HRegionInfo>(0);
      }
      serversToIndex = new HashMap<String, Integer>();
      hostsToIndex = new HashMap<String, Integer>();
      racksToIndex = new HashMap<String, Integer>();
      tablesToIndex = new HashMap<String, Integer>();

      //TODO: We should get the list of tables from master
      tables = new ArrayList<String>();
      this.rackManager = rackManager != null ? rackManager : new DefaultRackManager();

      List<List<Integer>> serversPerHostList = new ArrayList<List<Integer>>();
      List<List<Integer>> serversPerRackList = new ArrayList<List<Integer>>();
      // Use servername and port as there can be dead servers in this list. We want everything with
      // a matching hostname and port to have the same index.
      for (ServerName sn : clusterState.keySet()) {
        if (serversToIndex.get(sn.getHostAndPort()) == null) {
          serversToIndex.put(sn.getHostAndPort(), numServers++);
        }
        if (!hostsToIndex.containsKey(sn.getHostname())) {
          hostsToIndex.put(sn.getHostname(), numHosts++);
          serversPerHostList.add(new ArrayList<Integer>(1));
        }

        int serverIndex = serversToIndex.get(sn.getHostAndPort());
        int hostIndex = hostsToIndex.get(sn.getHostname());
        serversPerHostList.get(hostIndex).add(serverIndex);

        String rack = this.rackManager.getRack(sn);
        if (!racksToIndex.containsKey(rack)) {
          racksToIndex.put(rack, numRacks++);
          serversPerRackList.add(new ArrayList<Integer>());
        }
        int rackIndex = racksToIndex.get(rack);
        serversPerRackList.get(rackIndex).add(serverIndex);
      }

      // Count how many regions there are.
      for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        numRegions += entry.getValue().size();
      }
      numRegions += unassignedRegions.size();

      regionsToIndex = new HashMap<HRegionInfo, Integer>(numRegions);
      servers = new ServerName[numServers];
      serversPerHost = new int[numHosts][];
      serversPerRack = new int[numRacks][];
      regions = new HRegionInfo[numRegions];
      regionIndexToServerIndex = new int[numRegions];
      initialRegionIndexToServerIndex = new int[numRegions];
      regionIndexToTableIndex = new int[numRegions];
      regionIndexToPrimaryIndex = new int[numRegions];
      regionLoads = new Deque[numRegions];
      regionLocations = new int[numRegions][];
      serverIndicesSortedByRegionCount = new Integer[numServers];
      serverIndexToHostIndex = new int[numServers];
      serverIndexToRackIndex = new int[numServers];
      regionsPerServer = new int[numServers][];
      regionsPerHost = new int[numHosts][];
      regionsPerRack = new int[numRacks][];
      regionsByPrimaryPerServer = new int[numServers][];
      regionsByPrimaryPerHost = new int[numHosts][];
      regionsByPrimaryPerRack = new int[numRacks][];

      int tableIndex = 0, regionIndex = 0, regionPerServerIndex = 0;

      for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        int serverIndex = serversToIndex.get(entry.getKey().getHostAndPort());

        // keep the servername if this is the first server name for this hostname
        // or this servername has the newest startcode.
        if (servers[serverIndex] == null ||
            servers[serverIndex].getStartcode() < entry.getKey().getStartcode()) {
          servers[serverIndex] = entry.getKey();
        }

        regionsPerServer[serverIndex] = new int[entry.getValue().size()];
        regionsByPrimaryPerServer[serverIndex] = new int[entry.getValue().size()];
        serverIndicesSortedByRegionCount[serverIndex] = serverIndex;
      }

      hosts = new String[numHosts];
      for (Entry<String, Integer> entry : hostsToIndex.entrySet()) {
        hosts[entry.getValue()] = entry.getKey();
      }
      racks = new String[numRacks];
      for (Entry<String, Integer> entry : racksToIndex.entrySet()) {
        racks[entry.getValue()] = entry.getKey();
      }

      for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        int serverIndex = serversToIndex.get(entry.getKey().getHostAndPort());
        regionPerServerIndex = 0;

        int hostIndex = hostsToIndex.get(entry.getKey().getHostname());
        serverIndexToHostIndex[serverIndex] = hostIndex;

        int rackIndex = racksToIndex.get(this.rackManager.getRack(entry.getKey()));
        serverIndexToRackIndex[serverIndex] = rackIndex;

        for (HRegionInfo region : entry.getValue()) {
          registerRegion(region, regionIndex, serverIndex, loads, regionFinder);

          regionsPerServer[serverIndex][regionPerServerIndex++] = regionIndex;
          regionIndex++;
        }
      }
      for (HRegionInfo region : unassignedRegions) {
        registerRegion(region, regionIndex, -1, loads, regionFinder);
        regionIndex++;
      }

      for (int i = 0; i < serversPerHostList.size(); i++) {
        serversPerHost[i] = new int[serversPerHostList.get(i).size()];
        for (int j = 0; j < serversPerHost[i].length; j++) {
          serversPerHost[i][j] = serversPerHostList.get(i).get(j);
        }
      }

      for (int i = 0; i < serversPerRackList.size(); i++) {
        serversPerRack[i] = new int[serversPerRackList.get(i).size()];
        for (int j = 0; j < serversPerRack[i].length; j++) {
          serversPerRack[i][j] = serversPerRackList.get(i).get(j);
        }
      }

      numTables = tables.size();
      numRegionsPerServerPerTable = new int[numServers][numTables];

      for (int i = 0; i < numServers; i++) {
        for (int j = 0; j < numTables; j++) {
          numRegionsPerServerPerTable[i][j] = 0;
        }
      }

      for (int i=0; i < regionIndexToServerIndex.length; i++) {
        if (regionIndexToServerIndex[i] >= 0) {
          numRegionsPerServerPerTable[regionIndexToServerIndex[i]][regionIndexToTableIndex[i]]++;
        }
      }

      numMaxRegionsPerTable = new int[numTables];
      for (int serverIndex = 0 ; serverIndex < numRegionsPerServerPerTable.length; serverIndex++) {
        for (tableIndex = 0 ; tableIndex < numRegionsPerServerPerTable[serverIndex].length; tableIndex++) {
          if (numRegionsPerServerPerTable[serverIndex][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
            numMaxRegionsPerTable[tableIndex] = numRegionsPerServerPerTable[serverIndex][tableIndex];
          }
        }
      }

      for (int i = 0; i < regions.length; i ++) {
        HRegionInfo info = regions[i];
        if (info.isPrimaryReplica()) {
          regionIndexToPrimaryIndex[i] = i;
        } else {
          regionIndexToPrimaryIndex[i] = regionsToIndex.get(info.getPrimaryRegionInfo());
        }
      }

      for (int i = 0; i < regionsPerServer.length; i++) {
        regionsByPrimaryPerServer[i] = new int[regionsPerServer[i].length];
        for (int j = 0; j < regionsPerServer[i].length; j++) {
          int primaryIndex = regionIndexToPrimaryIndex[regionsPerServer[i][j]];
          regionsByPrimaryPerServer[i][j] = primaryIndex;
        }
        // sort the regions by primaries.
        Arrays.sort(regionsByPrimaryPerServer[i]);
      }

      // compute regionsPerHost
      for (int i = 0 ; i < serversPerHost.length; i++) {
        int numRegionsPerHost = 0;
        for (int j = 0; j < serversPerHost[i].length; j++) {
          numRegionsPerHost += regionsPerServer[serversPerHost[i][j]].length;
        }
        regionsPerHost[i] = new int[numRegionsPerHost];
        regionsByPrimaryPerHost[i] = new int[numRegionsPerHost];
      }

      for (int i = 0 ; i < serversPerHost.length; i++) {
        int numRegionPerHostIndex = 0;
        for (int j = 0; j < serversPerHost[i].length; j++) {
          for (int k = 0; k < regionsPerServer[serversPerHost[i][j]].length; k++) {
            int region = regionsPerServer[serversPerHost[i][j]][k];
            regionsPerHost[i][numRegionPerHostIndex] = region;
            int primaryIndex = regionIndexToPrimaryIndex[region];
            regionsByPrimaryPerHost[i][numRegionPerHostIndex] = primaryIndex;
            numRegionPerHostIndex++;
          }
        }
        // sort the regions by primaries.
        Arrays.sort(regionsByPrimaryPerHost[i]);
      }

      // compute regionsPerRack
      for (int i = 0 ; i < serversPerRack.length; i++) {
        int numRegionsPerRack = 0;
        for (int j = 0; j < serversPerRack[i].length; j++) {
          numRegionsPerRack += regionsPerServer[serversPerRack[i][j]].length;
        }
        regionsPerRack[i] = new int[numRegionsPerRack];
        regionsByPrimaryPerRack[i] = new int[numRegionsPerRack];
      }

      for (int i = 0 ; i < serversPerRack.length; i++) {
        int numRegionPerRackIndex = 0;
        for (int j = 0; j < serversPerRack[i].length; j++) {
          for (int k = 0; k < regionsPerServer[serversPerRack[i][j]].length; k++) {
            int region = regionsPerServer[serversPerRack[i][j]][k];
            regionsPerRack[i][numRegionPerRackIndex] = region;
            int primaryIndex = regionIndexToPrimaryIndex[region];
            regionsByPrimaryPerRack[i][numRegionPerRackIndex] = primaryIndex;
            numRegionPerRackIndex++;
          }
        }
        // sort the regions by primaries.
        Arrays.sort(regionsByPrimaryPerRack[i]);
      }
    }

    /** Helper for Cluster constructor to handle a region */
    private void registerRegion(HRegionInfo region, int regionIndex, int serverIndex,
        Map<String, Deque<RegionLoad>> loads, RegionLocationFinder regionFinder) {
      String tableName = region.getTable().getNameAsString();
      if (!tablesToIndex.containsKey(tableName)) {
        tables.add(tableName);
        tablesToIndex.put(tableName, tablesToIndex.size());
      }
      int tableIndex = tablesToIndex.get(tableName);

      regionsToIndex.put(region, regionIndex);
      regions[regionIndex] = region;
      regionIndexToServerIndex[regionIndex] = serverIndex;
      initialRegionIndexToServerIndex[regionIndex] = serverIndex;
      regionIndexToTableIndex[regionIndex] = tableIndex;

      // region load
      if (loads != null) {
        Deque<RegionLoad> rl = loads.get(region.getRegionNameAsString());
        // That could have failed if the RegionLoad is using the other regionName
        if (rl == null) {
          // Try getting the region load using encoded name.
          rl = loads.get(region.getEncodedName());
        }
        regionLoads[regionIndex] = rl;
      }

      if (regionFinder != null) {
        //region location
        List<ServerName> loc = regionFinder.getTopBlockLocations(region);
        regionLocations[regionIndex] = new int[loc.size()];
        for (int i=0; i < loc.size(); i++) {
          regionLocations[regionIndex][i] =
              loc.get(i) == null ? -1 :
                (serversToIndex.get(loc.get(i)) == null ? -1 : serversToIndex.get(loc.get(i)));
        }
      }
    }

    /** An action to move or swap a region */
    public static class Action {
      public static enum Type {
        ASSIGN_REGION,
        MOVE_REGION,
        SWAP_REGIONS,
        NULL,
      }

      public Type type;
      public Action (Type type) {this.type = type;}
      /** Returns an Action which would undo this action */
      public Action undoAction() { return this; }
      @Override
      public String toString() { return type + ":";}
    }

    public static class AssignRegionAction extends Action {
      public int region;
      public int server;
      public AssignRegionAction(int region, int server) {
        super(Type.ASSIGN_REGION);
        this.region = region;
        this.server = server;
      }
      @Override
      public Action undoAction() {
        // TODO fix this
        return this;
      }
      @Override
      public String toString() {
        return type + ": " + region + ":" + server;
      }
    }

    public static class MoveRegionAction extends Action {
      public int region;
      public int fromServer;
      public int toServer;

      public MoveRegionAction(int region, int fromServer, int toServer) {
        super(Type.MOVE_REGION);
        this.fromServer = fromServer;
        this.region = region;
        this.toServer = toServer;
      }
      @Override
      public Action undoAction() {
        return new MoveRegionAction (region, toServer, fromServer);
      }
      @Override
      public String toString() {
        return type + ": " + region + ":" + fromServer + " -> " + toServer;
      }
    }

    public static class SwapRegionsAction extends Action {
      public int fromServer;
      public int fromRegion;
      public int toServer;
      public int toRegion;
      public SwapRegionsAction(int fromServer, int fromRegion, int toServer, int toRegion) {
        super(Type.SWAP_REGIONS);
        this.fromServer = fromServer;
        this.fromRegion = fromRegion;
        this.toServer = toServer;
        this.toRegion = toRegion;
      }
      @Override
      public Action undoAction() {
        return new SwapRegionsAction (fromServer, toRegion, toServer, fromRegion);
      }
      @Override
      public String toString() {
        return type + ": " + fromRegion + ":" + fromServer + " <-> " + toRegion + ":" + toServer;
      }
    }

    public static Action NullAction = new Action(Type.NULL);

    public void doAction(Action action) {
      switch (action.type) {
      case NULL: break;
      case ASSIGN_REGION:
        AssignRegionAction ar = (AssignRegionAction) action;
        regionsPerServer[ar.server] = addRegion(regionsPerServer[ar.server], ar.region);
        regionMoved(ar.region, -1, ar.server);
        break;
      case MOVE_REGION:
        MoveRegionAction mra = (MoveRegionAction) action;
        regionsPerServer[mra.fromServer] = removeRegion(regionsPerServer[mra.fromServer], mra.region);
        regionsPerServer[mra.toServer] = addRegion(regionsPerServer[mra.toServer], mra.region);
        regionMoved(mra.region, mra.fromServer, mra.toServer);
        break;
      case SWAP_REGIONS:
        SwapRegionsAction a = (SwapRegionsAction) action;
        regionsPerServer[a.fromServer] = replaceRegion(regionsPerServer[a.fromServer], a.fromRegion, a.toRegion);
        regionsPerServer[a.toServer] = replaceRegion(regionsPerServer[a.toServer], a.toRegion, a.fromRegion);
        regionMoved(a.fromRegion, a.fromServer, a.toServer);
        regionMoved(a.toRegion, a.toServer, a.fromServer);
        break;
      default:
        throw new RuntimeException("Uknown action:" + action.type);
      }
    }

    /**
     * Return true if the placement of region on server would lower the availability
     * of the region in question
     * @param server
     * @param region
     * @return true or false
     */
    boolean wouldLowerAvailability(HRegionInfo regionInfo, ServerName serverName) {
      if (!serversToIndex.containsKey(serverName.getHostAndPort())) {
        return false; // safeguard against race between cluster.servers and servers from LB method args
      }
      int server = serversToIndex.get(serverName.getHostAndPort());
      int region = regionsToIndex.get(regionInfo);

      int primary = regionIndexToPrimaryIndex[region];

      // there is a subset relation for server < host < rack
      // check server first
      if (contains(regionsByPrimaryPerServer[server], primary)) {
        // check for whether there are other servers that we can place this region
        for (int i = 0; i < regionsByPrimaryPerServer.length; i++) {
          if (i != server && !contains(regionsByPrimaryPerServer[i], primary)) {
            return true; // meaning there is a better server
          }
        }
        return false; // there is not a better server to place this
      }

      // check host
      int host = serverIndexToHostIndex[server];
      if (contains(regionsByPrimaryPerHost[host], primary)) {
        // check for whether there are other hosts that we can place this region
        for (int i = 0; i < regionsByPrimaryPerHost.length; i++) {
          if (i != host && !contains(regionsByPrimaryPerHost[i], primary)) {
            return true; // meaning there is a better host
          }
        }
        return false; // there is not a better host to place this
      }

      // check rack
      int rack = serverIndexToRackIndex[server];
      if (contains(regionsByPrimaryPerRack[rack], primary)) {
        // check for whether there are other racks that we can place this region
        for (int i = 0; i < regionsByPrimaryPerRack.length; i++) {
          if (i != rack && !contains(regionsByPrimaryPerRack[i], primary)) {
            return true; // meaning there is a better rack
          }
        }
        return false; // there is not a better rack to place this
      }
      return false;
    }

    void doAssignRegion(HRegionInfo regionInfo, ServerName serverName) {
      if (!serversToIndex.containsKey(serverName.getHostAndPort())) {
        return;
      }
      int server = serversToIndex.get(serverName.getHostAndPort());
      int region = regionsToIndex.get(regionInfo);
      doAction(new AssignRegionAction(region, server));
    }

    void regionMoved(int region, int oldServer, int newServer) {
      regionIndexToServerIndex[region] = newServer;
      if (initialRegionIndexToServerIndex[region] == newServer) {
        numMovedRegions--; //region moved back to original location
        if (regions[region].isMetaRegion()) {
          numMovedMetaRegions--;
        }
      } else if (oldServer >= 0 && initialRegionIndexToServerIndex[region] == oldServer) {
        numMovedRegions++; //region moved from original location
        if (regions[region].isMetaRegion()) {
          numMovedMetaRegions++;
        }
      }
      int tableIndex = regionIndexToTableIndex[region];
      if (oldServer >= 0) {
        numRegionsPerServerPerTable[oldServer][tableIndex]--;
      }
      numRegionsPerServerPerTable[newServer][tableIndex]++;

      //check whether this caused maxRegionsPerTable in the new Server to be updated
      if (numRegionsPerServerPerTable[newServer][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
        numRegionsPerServerPerTable[newServer][tableIndex] = numMaxRegionsPerTable[tableIndex];
      } else if (oldServer >= 0 && (numRegionsPerServerPerTable[oldServer][tableIndex] + 1)
          == numMaxRegionsPerTable[tableIndex]) {
        //recompute maxRegionsPerTable since the previous value was coming from the old server
        for (int serverIndex = 0 ; serverIndex < numRegionsPerServerPerTable.length; serverIndex++) {
          if (numRegionsPerServerPerTable[serverIndex][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
            numMaxRegionsPerTable[tableIndex] = numRegionsPerServerPerTable[serverIndex][tableIndex];
          }
        }
      }

      // update for hosts
      int primary = regionIndexToPrimaryIndex[region];
      int oldHost = oldServer >= 0 ? serverIndexToHostIndex[oldServer] : -1;
      int newHost = serverIndexToHostIndex[newServer];
      if (newHost != oldHost) {
        regionsPerHost[newHost] = addRegion(regionsPerHost[newHost], region);
        regionsByPrimaryPerHost[newHost] = addRegionSorted(regionsByPrimaryPerHost[newHost], primary);
        if (oldHost >= 0) {
          regionsPerHost[oldHost] = removeRegion(regionsPerHost[oldHost], region);
          regionsByPrimaryPerHost[oldHost] = removeRegion(
            regionsByPrimaryPerHost[oldHost], primary); // will still be sorted
        }
      }

      int oldRack = oldServer >= 0 ? serverIndexToRackIndex[oldServer] : -1;
      int newRack = serverIndexToRackIndex[newServer];
      if (newRack != oldRack) {
        regionsPerRack[newRack] = addRegion(regionsPerRack[newRack], region);
        regionsByPrimaryPerRack[newRack] = addRegionSorted(regionsByPrimaryPerRack[newRack], primary);
        if (oldRack >= 0) {
          regionsPerRack[oldRack] = removeRegion(regionsPerRack[oldRack], region);
          regionsByPrimaryPerRack[oldRack] = removeRegion(
            regionsByPrimaryPerRack[oldRack], primary); // will still be sorted
        }
      }
    }

    int[] removeRegion(int[] regions, int regionIndex) {
      //TODO: this maybe costly. Consider using linked lists
      int[] newRegions = new int[regions.length - 1];
      int i = 0;
      for (i = 0; i < regions.length; i++) {
        if (regions[i] == regionIndex) {
          break;
        }
        newRegions[i] = regions[i];
      }
      System.arraycopy(regions, i+1, newRegions, i, newRegions.length - i);
      return newRegions;
    }

    int[] addRegion(int[] regions, int regionIndex) {
      int[] newRegions = new int[regions.length + 1];
      System.arraycopy(regions, 0, newRegions, 0, regions.length);
      newRegions[newRegions.length - 1] = regionIndex;
      return newRegions;
    }

    int[] addRegionSorted(int[] regions, int regionIndex) {
      int[] newRegions = new int[regions.length + 1];
      int i = 0;
      for (i = 0; i < regions.length; i++) {
        if (regions[i] > regionIndex) {
          newRegions[i] = regionIndex;
          break;
        }
        newRegions[i] = regions[i];
      }
      if (i == regions.length) {
        newRegions[i] = regionIndex;
      } else {
        System.arraycopy(regions, i, newRegions, i+1, regions.length - i);
      }
      return newRegions;
    }

    int[] replaceRegion(int[] regions, int regionIndex, int newRegionIndex) {
      int i = 0;
      for (i = 0; i < regions.length; i++) {
        if (regions[i] == regionIndex) {
          regions[i] = newRegionIndex;
          break;
        }
      }
      return regions;
    }

    void sortServersByRegionCount() {
      Arrays.sort(serverIndicesSortedByRegionCount, numRegionsComparator);
    }

    int getNumRegions(int server) {
      return regionsPerServer[server].length;
    }

    boolean contains(int[] arr, int val) {
      return Arrays.binarySearch(arr, val) >= 0;
    }

    private Comparator<Integer> numRegionsComparator = new Comparator<Integer>() {
      @Override
      public int compare(Integer integer, Integer integer2) {
        return Integer.valueOf(getNumRegions(integer)).compareTo(getNumRegions(integer2));
      }
    };

    @Override
    public String toString() {
      String desc = "Cluster{" +
          "servers=[";
          for(ServerName sn:servers) {
             desc += sn.getHostAndPort() + ", ";
          }
          desc +=
          ", serverIndicesSortedByRegionCount="+
          Arrays.toString(serverIndicesSortedByRegionCount) +
          ", regionsPerServer=[";

          for (int[]r:regionsPerServer) {
            desc += Arrays.toString(r);
          }
          desc += "]" +
          ", numMaxRegionsPerTable=" +
          Arrays.toString(numMaxRegionsPerTable) +
          ", numRegions=" +
          numRegions +
          ", numServers=" +
          numServers +
          ", numTables=" +
          numTables +
          ", numMovedRegions=" +
          numMovedRegions +
          ", numMovedMetaRegions=" +
          numMovedMetaRegions +
          '}';
      return desc;
    }
  }

  // slop for regions
  protected float slop;
  private Configuration config;
  protected RackManager rackManager;
  private static final Random RANDOM = new Random(System.currentTimeMillis());
  private static final Log LOG = LogFactory.getLog(BaseLoadBalancer.class);

  protected final MetricsBalancer metricsBalancer = new MetricsBalancer();
  protected MasterServices services;

  @Override
  public void setConf(Configuration conf) {
    setSlop(conf);
    if (slop < 0) slop = 0;
    else if (slop > 1) slop = 1;

    this.config = conf;
    this.rackManager = new RackManager(getConf());
    regionFinder.setConf(conf);
  }

  protected void setSlop(Configuration conf) {
    this.slop = conf.getFloat("hbase.regions.slop", (float) 0.2);
  }

  @Override
  public Configuration getConf() {
    return this.config;
  }

  @Override
  public void setClusterStatus(ClusterStatus st) {
    regionFinder.setClusterStatus(st);
  }

  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.services = masterServices;
    this.regionFinder.setServices(masterServices);
  }

  public void setRackManager(RackManager rackManager) {
    this.rackManager = rackManager;
  }

  protected boolean needsBalance(ClusterLoadState cs) {
    if (cs.getNumServers() < MIN_SERVER_BALANCE) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not running balancer because only " + cs.getNumServers()
            + " active regionserver(s)");
      }
      return false;
    }
    // TODO: check for same host replicas

    // Check if we even need to do any load balancing
    // HBASE-3681 check sloppiness first
    float average = cs.getLoadAverage(); // for logging
    int floor = (int) Math.floor(average * (1 - slop));
    int ceiling = (int) Math.ceil(average * (1 + slop));
    if (!(cs.getMinLoad() > ceiling || cs.getMaxLoad() < floor)) {
      NavigableMap<ServerAndLoad, List<HRegionInfo>> serversByLoad = cs.getServersByLoad();
      if (LOG.isTraceEnabled()) {
        // If nothing to balance, then don't say anything unless trace-level logging.
        LOG.trace("Skipping load balancing because balanced cluster; " +
          "servers=" + cs.getNumServers() + " " +
          "regions=" + cs.getNumRegions() + " average=" + average + " " +
          "mostloaded=" + serversByLoad.lastKey().getLoad() +
          " leastloaded=" + serversByLoad.firstKey().getLoad());
      }
      return false;
    }
    return true;
  }

  /**
   * Generates a bulk assignment plan to be used on cluster startup using a
   * simple round-robin assignment.
   * <p>
   * Takes a list of all the regions and all the servers in the cluster and
   * returns a map of each server to the regions that it should be assigned.
   * <p>
   * Currently implemented as a round-robin assignment. Same invariant as load
   * balancing, all servers holding floor(avg) or ceiling(avg).
   *
   * TODO: Use block locations from HDFS to place regions with their blocks
   *
   * @param regions all regions
   * @param servers all servers
   * @return map of server to the regions it should take, or null if no
   *         assignment is possible (ie. no regions or no servers)
   */
  @Override
  public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) {
    metricsBalancer.incrMiscInvocations();

    if (regions.isEmpty() || servers.isEmpty()) {
      return null;
    }

    // TODO: instead of retainAssignment() and roundRobinAssignment(), we should just run the
    // normal LB.balancerCluster() with unassignedRegions. We only need to have a candidate
    // generator for AssignRegionAction. The LB will ensure the regions are mostly local
    // and balanced.

    Map<ServerName, List<HRegionInfo>> assignments = new TreeMap<ServerName, List<HRegionInfo>>();
    // Get the snapshot of the current assignments for the regions in question, and then create
    // a cluster out of it. Note that we might have replicas already assigned to some servers
    // earlier. So we want to get the snapshot to see those assignments.
    Map<ServerName, List<HRegionInfo>> clusterState = getRegionAssignmentsByServer();
    Cluster cluster = new Cluster(regions, clusterState, null, this.regionFinder, rackManager);

    int numRegions = regions.size();
    int numServers = servers.size();
    int max = (int) Math.ceil((float) numRegions / numServers);
    int serverIdx = 0;
    if (numServers > 1) {
      serverIdx = RANDOM.nextInt(numServers);
    }
    int regionIdx = 0;
    List<HRegionInfo> unassignedRegions = new ArrayList<HRegionInfo>();
    for (int j = 0; j < numServers; j++) {
      ServerName server = servers.get((j + serverIdx) % numServers);
      List<HRegionInfo> serverRegions = new ArrayList<HRegionInfo>(max);
      for (int i = regionIdx; i < numRegions; i += numServers) {
        HRegionInfo region = regions.get(i % numRegions);
        if (cluster.wouldLowerAvailability(region, server)) {
          unassignedRegions.add(region);
        } else {
          serverRegions.add(region);
          cluster.doAssignRegion(region, server);
        }
      }
      assignments.put(server, serverRegions);
      regionIdx++;
    }
    List<HRegionInfo> lastFewRegions = new ArrayList<HRegionInfo>();
    // assign the remaining by going through the list and try to assign to servers one-by-one
    serverIdx = RANDOM.nextInt(numServers);
    for (HRegionInfo region : unassignedRegions) {
      for (int j = 0; j < numServers; j++) { // try all servers one by one
        ServerName serverName = servers.get((j + serverIdx) % numServers);
        if (!cluster.wouldLowerAvailability(region, serverName)) {
          assignments.get(serverName).add(region);
          cluster.doAssignRegion(region, serverName);
          serverIdx = (j + serverIdx + 1) % numServers; //remain from next server
          break;
        }
      }
    }
    // just sprinkle the rest of the regions on random regionservers. The balanceCluster will
    // make it optimal later. We should not need this if wouldLowerAvailability() is correct
    for (HRegionInfo region : lastFewRegions) {
      int i = RANDOM.nextInt(numServers);
      assignments.get(servers.get(i)).add(region);
    }
    return assignments;
  }

  boolean wouldLowerAvailability(Map<ServerName, Set<HRegionInfo>> serverToRegionAssignments,
      Map<String, Set<HRegionInfo>> rackToRegionAssignments,
      int uniqueRacks,
      ServerName server, HRegionInfo region) {
    HRegionInfo hri = region.getPrimaryRegionInfo();
    Set<HRegionInfo> regions = serverToRegionAssignments.get(server);
    if (regions == null) return false;
    if (regions.contains(hri)) return true;
    //TODO: add same host checks (if more than 1 host,then don't place replicas on the same host)
    // also availability would be lowered if the balancer chooses the node to move to a
    // node in the same rack (if there were multiple racks to choose from)
    if (uniqueRacks > 1) {
      String destRack = rackManager.getRack(server);
      regions = rackToRegionAssignments.get(destRack);
      if (regions == null) return false;
      if (rackToRegionAssignments.get(destRack).contains(hri)) {
        // the destination rack contains a replica (primary or secondary)
        return true;
      }
    }
    return false;
  }

  /**
   * Generates an immediate assignment plan to be used by a new master for
   * regions in transition that do not have an already known destination.
   *
   * Takes a list of regions that need immediate assignment and a list of all
   * available servers. Returns a map of regions to the server they should be
   * assigned to.
   *
   * This method will return quickly and does not do any intelligent balancing.
   * The goal is to make a fast decision not the best decision possible.
   *
   * Currently this is random.
   *
   * @param regions
   * @param servers
   * @return map of regions to the server it should be assigned to
   */
  @Override
  public Map<HRegionInfo, ServerName> immediateAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) {
    metricsBalancer.incrMiscInvocations();

    Map<HRegionInfo, ServerName> assignments = new TreeMap<HRegionInfo, ServerName>();
    for (HRegionInfo region : regions) {
      assignments.put(region, randomAssignment(region, servers));
    }
    return assignments;
  }

  /**
   * Used to assign a single region to a random server.
   */
  @Override
  public ServerName randomAssignment(HRegionInfo regionInfo, List<ServerName> servers) {
    metricsBalancer.incrMiscInvocations();

    if (servers == null || servers.isEmpty()) {
      LOG.warn("Wanted to do random assignment but no servers to assign to");
      return null;
    }
    List<HRegionInfo> unassignedRegions = new ArrayList<HRegionInfo>();
    unassignedRegions.add(regionInfo);
    // Get the snapshot of the current assignments for the regions in question, and then create
    // a cluster out of it. Note that we might have replicas already assigned to some servers
    // earlier. So we want to get the snapshot to see those assignments.
    Map<ServerName, List<HRegionInfo>> clusterState = getRegionAssignmentsByServer();
    Cluster cluster = new Cluster(unassignedRegions, clusterState, null, this.regionFinder, rackManager);

    ServerName server = randomAssignment(cluster, regionInfo, servers);
    return server;
  }

  protected ServerName randomAssignment(Cluster cluster, HRegionInfo regionInfo, List<ServerName> servers) {
    ServerName server = null;
    final int maxIterations = servers.size();
    int iterations = 0;
    do {
      server = servers.get(RANDOM.nextInt(servers.size()));
    } while (servers.size() != 1
        && cluster.wouldLowerAvailability(regionInfo, server)
        && iterations++ < maxIterations);
    cluster.doAssignRegion(regionInfo, server);
    return server;
  }

  /**
   * Generates a bulk assignment startup plan, attempting to reuse the existing
   * assignment information from META, but adjusting for the specified list of
   * available/online servers available for assignment.
   * <p>
   * Takes a map of all regions to their existing assignment from META. Also
   * takes a list of online servers for regions to be assigned to. Attempts to
   * retain all assignment, so in some instances initial assignment will not be
   * completely balanced.
   * <p>
   * Any leftover regions without an existing server to be assigned to will be
   * assigned randomly to available servers.
   *
   * @param regions regions and existing assignment from meta
   * @param servers available servers
   * @return map of servers and regions to be assigned to them
   */
  @Override
  public Map<ServerName, List<HRegionInfo>> retainAssignment(Map<HRegionInfo, ServerName> regions,
      List<ServerName> servers) {
    // Update metrics
    metricsBalancer.incrMiscInvocations();

    // Group all of the old assignments by their hostname.
    // We can't group directly by ServerName since the servers all have
    // new start-codes.

    // Group the servers by their hostname. It's possible we have multiple
    // servers on the same host on different ports.
    ArrayListMultimap<String, ServerName> serversByHostname = ArrayListMultimap.create();
    for (ServerName server : servers) {
      serversByHostname.put(server.getHostname(), server);
    }

    // Now come up with new assignments
    Map<ServerName, List<HRegionInfo>> assignments = new TreeMap<ServerName, List<HRegionInfo>>();

    for (ServerName server : servers) {
      assignments.put(server, new ArrayList<HRegionInfo>());
    }

    // Collection of the hostnames that used to have regions
    // assigned, but for which we no longer have any RS running
    // after the cluster restart.
    Set<String> oldHostsNoLongerPresent = Sets.newTreeSet();

    int numRandomAssignments = 0;
    int numRetainedAssigments = 0;
    // Get the snapshot of the current assignments for the regions in question, and then create
    // a cluster out of it. Note that we might have replicas already assigned to some servers
    // earlier. So we want to get the snapshot to see those assignments.
    Map<ServerName, List<HRegionInfo>> clusterState = getRegionAssignmentsByServer();
    Cluster cluster = new Cluster(regions.keySet(), clusterState, null, this.regionFinder, rackManager);

    for (Map.Entry<HRegionInfo, ServerName> entry : regions.entrySet()) {
      HRegionInfo region = entry.getKey();
      ServerName oldServerName = entry.getValue();
      List<ServerName> localServers = new ArrayList<ServerName>();
      if (oldServerName != null) {
        localServers = serversByHostname.get(oldServerName.getHostname());
      }
      if (localServers.isEmpty()) {
        // No servers on the new cluster match up with this hostname,
        // assign randomly.
        ServerName randomServer = randomAssignment(cluster, region, servers);

        assignments.get(randomServer).add(region);
        // also update the assignments for checking availability
        numRandomAssignments++;
        if (oldServerName != null) oldHostsNoLongerPresent.add(oldServerName.getHostname());
      } else if (localServers.size() == 1) {
        // the usual case - one new server on same host
        ServerName target = localServers.get(0);
        assignments.get(localServers.get(0)).add(region);
        cluster.doAssignRegion(region, target);
        numRetainedAssigments++;
      } else {
        // multiple new servers in the cluster on this same host
        int size = localServers.size();
        ServerName target =
            localServers.contains(oldServerName) ? oldServerName : localServers.get(RANDOM
                .nextInt(size));
        assignments.get(target).add(region);
        cluster.doAssignRegion(region, target);
        numRetainedAssigments++;
      }
    }

    String randomAssignMsg = "";
    if (numRandomAssignments > 0) {
      randomAssignMsg =
          numRandomAssignments + " regions were assigned "
              + "to random hosts, since the old hosts for these regions are no "
              + "longer present in the cluster. These hosts were:\n  "
              + Joiner.on("\n  ").join(oldHostsNoLongerPresent);
    }

    LOG.info("Reassigned " + regions.size() + " regions. " + numRetainedAssigments
        + " retained the pre-restart assignment. " + randomAssignMsg);
    return assignments;
  }

  @Override
  public void initialize() throws HBaseIOException{
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  @Override
  public void stop(String why) {
    LOG.info("Load Balancer stop requested: "+why);
    stopped = true;
  }

  protected Map<ServerName, List<HRegionInfo>> getRegionAssignmentsByServer() {
    if (this.services != null) {
      return this.services.getAssignmentManager().getRegionAssignmentsByServer();
    } else {
      return new HashMap<ServerName, List<HRegionInfo>>();
    }
  }
}
