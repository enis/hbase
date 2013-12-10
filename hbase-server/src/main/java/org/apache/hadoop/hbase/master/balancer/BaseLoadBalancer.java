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
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.NavigableMap;

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
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.annotations.VisibleForTesting;
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

  /**
   * An efficient array based implementation similar to ClusterState for keeping
   * the status of the cluster in terms of region assignment and distribution.
   * To be used by LoadBalancers.
   */
  protected static class Cluster {
    ServerName[] servers;
    ArrayList<String> tables;
    HRegionInfo[] regions;
    Deque<RegionLoad>[] regionLoads;
    int[][] regionLocations; //regionIndex -> list of serverIndex sorted by locality

    int[][] regionsPerServer;            //serverIndex -> region list
    int[]   regionIndexToServerIndex;    //regionIndex -> serverIndex
    int[]   initialRegionIndexToServerIndex;    //regionIndex -> serverIndex (initial cluster state)
    int[]   regionIndexToTableIndex;     //regionIndex -> tableIndex
    int[][] numRegionsPerServerPerTable; //serverIndex -> tableIndex -> # regions
    int[]   numMaxRegionsPerTable;       //tableIndex -> max number of regions in a single RS

    Integer[] serverIndicesSortedByRegionCount;

    Map<String, Integer> serversToIndex;
    Map<String, Integer> tablesToIndex;
    Map<Integer, Set<HRegionInfo>> serverToReplicaMap;
    Map<String, Set<HRegionInfo>> rackToReplicaMap;

    int numRegions;
    int numServers;
    int numTables;

    int numMovedRegions = 0; //num moved regions from the initial configuration
    int numMovedMetaRegions = 0;       //num of moved regions that are META
    RackManager rackManager;
    int uniqueRacks;

    protected Cluster(Map<ServerName, List<HRegionInfo>> clusterState,  Map<String, Deque<RegionLoad>> loads,
        RegionLocationFinder regionFinder, RackManager rackManager) {

      serversToIndex = new HashMap<String, Integer>();
      tablesToIndex = new HashMap<String, Integer>();
      //regionsToIndex = new HashMap<HRegionInfo, Integer>();

      //TODO: We should get the list of tables from master
      tables = new ArrayList<String>();


      numRegions = 0;
      this.rackManager = rackManager;

      int serverIndex = 0;

      // Use servername and port as there can be dead servers in this list. We want everything with
      // a matching hostname and port to have the same index.
      for (ServerName sn:clusterState.keySet()) {
        if (serversToIndex.get(sn.getHostAndPort()) == null) {
          serversToIndex.put(sn.getHostAndPort(), serverIndex++);
        }
      }

      // Count how many regions there are.
      for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        numRegions += entry.getValue().size();
      }

      numServers = serversToIndex.size();
      regionsPerServer = new int[serversToIndex.size()][];

      servers = new ServerName[numServers];
      regions = new HRegionInfo[numRegions];
      regionIndexToServerIndex = new int[numRegions];
      initialRegionIndexToServerIndex = new int[numRegions];
      regionIndexToTableIndex = new int[numRegions];
      regionLoads = new Deque[numRegions];
      regionLocations = new int[numRegions][];
      serverIndicesSortedByRegionCount = new Integer[numServers];
      serverToReplicaMap = new HashMap<Integer, Set<HRegionInfo>>();
      rackToReplicaMap = new HashMap<String, Set<HRegionInfo>>();

      int tableIndex = 0, regionIndex = 0, regionPerServerIndex = 0;

      for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        serverIndex = serversToIndex.get(entry.getKey().getHostAndPort());

        // keep the servername if this is the first server name for this hostname
        // or this servername has the newest startcode.
        if (servers[serverIndex] == null ||
            servers[serverIndex].getStartcode() < entry.getKey().getStartcode()) {
          servers[serverIndex] = entry.getKey();
        }

        regionsPerServer[serverIndex] = new int[entry.getValue().size()];
        serverIndicesSortedByRegionCount[serverIndex] = serverIndex;
      }
      Set<String>racks = new HashSet<String>();
      for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        String rack = null;
        if (rackManager != null) {
          rack = rackManager.getRack(entry.getKey());
          if (!racks.contains(rack)) {
            uniqueRacks++;
            racks.add(rack);
          }
        }
        serverIndex = serversToIndex.get(entry.getKey().getHostAndPort());
        regionPerServerIndex = 0;

        for (HRegionInfo region : entry.getValue()) {
          String tableName = region.getTable().getNameAsString();
          Integer idx = tablesToIndex.get(tableName);
          if (idx == null) {
            tables.add(tableName);
            idx = tableIndex;
            tablesToIndex.put(tableName, tableIndex++);
          }

          regions[regionIndex] = region;
          regionIndexToServerIndex[regionIndex] = serverIndex;
          initialRegionIndexToServerIndex[regionIndex] = serverIndex;
          regionIndexToTableIndex[regionIndex] = idx;
          regionsPerServer[serverIndex][regionPerServerIndex++] = regionIndex;

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

          Set<HRegionInfo> replicaMap = serverToReplicaMap.get(serverIndex);
          if (replicaMap == null) {
            replicaMap = new HashSet<HRegionInfo>();
            serverToReplicaMap.put(serverIndex, replicaMap);
          }
          //add the primary replica for easy comparison later
          HRegionInfo h = region.getPrimaryRegionInfo();
          replicaMap.add(h);
          if (rack != null) {
            // maintain the rack information as well
            Set<HRegionInfo> perRackReplicaMap = rackToReplicaMap.get(rack);
            if (perRackReplicaMap == null) {
              perRackReplicaMap = new HashSet<HRegionInfo>();
              rackToReplicaMap.put(rack, perRackReplicaMap);
            }
            perRackReplicaMap.add(h);
          }

          regionIndex++;
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
        numRegionsPerServerPerTable[regionIndexToServerIndex[i]][regionIndexToTableIndex[i]]++;
      }

      numMaxRegionsPerTable = new int[numTables];
      for (serverIndex = 0 ; serverIndex < numRegionsPerServerPerTable.length; serverIndex++) {
        for (tableIndex = 0 ; tableIndex < numRegionsPerServerPerTable[serverIndex].length; tableIndex++) {
          if (numRegionsPerServerPerTable[serverIndex][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
            numMaxRegionsPerTable[tableIndex] = numRegionsPerServerPerTable[serverIndex][tableIndex];
          }
        }
      }
    }

    public void moveOrSwapRegion(int lServer, int rServer, int lRegion, int rRegion) {
      //swap
      if (rRegion >= 0 && lRegion >= 0) {
        regionMoved(rRegion, rServer, lServer);
        regionsPerServer[rServer] = replaceRegion(regionsPerServer[rServer], rRegion, lRegion);
        regionMoved(lRegion, lServer, rServer);
        regionsPerServer[lServer] = replaceRegion(regionsPerServer[lServer], lRegion, rRegion);
        updateReplicaMap(rServer, lServer, rRegion);
        updateReplicaMap(lServer, rServer, lRegion);
      } else if (rRegion >= 0) { //move rRegion
        regionMoved(rRegion, rServer, lServer);
        regionsPerServer[rServer] = removeRegion(regionsPerServer[rServer], rRegion);
        regionsPerServer[lServer] = addRegion(regionsPerServer[lServer], rRegion);
        updateReplicaMap(rServer, lServer, rRegion);
      } else if (lRegion >= 0) { //move lRegion
        regionMoved(lRegion, lServer, rServer);
        regionsPerServer[lServer] = removeRegion(regionsPerServer[lServer], lRegion);
        regionsPerServer[rServer] = addRegion(regionsPerServer[rServer], lRegion);
        updateReplicaMap(rServer, lServer, lRegion);
      }
    }

    // remove region from lServer and put it in rServer. Also update the racks information
    @VisibleForTesting
    void updateReplicaMap(int lServer, int rServer, int region) {
      Set<HRegionInfo> regionSet = serverToReplicaMap.get(lServer);
      if (regionSet != null) {
        regionSet.remove(regions[region]);
      }
      regionSet = serverToReplicaMap.get(rServer);
      if (regionSet == null) {
        regionSet = new HashSet<HRegionInfo>();
        serverToReplicaMap.put(rServer, regionSet);
      }
      regionSet.add(regions[region]);
      // update the racks info
      if (rackManager != null) {
        String srcRack = rackManager.getRack(servers[lServer]);
        String destRack = rackManager.getRack(servers[rServer]);
        regionSet = rackToReplicaMap.get(srcRack);
        if (regionSet != null) {
          regionSet.remove(regions[region]);
        }
        regionSet = rackToReplicaMap.get(destRack);
        if (regionSet == null) {
          regionSet = new HashSet<HRegionInfo>();
          rackToReplicaMap.put(destRack, regionSet);
        }
        regionSet.add(regions[region]);
      }
    }
    

    /**
     * Return true if the placement of region on server would lower the availability
     * of the region in question
     * @param server
     * @param region
     * @return true or false
     */
    boolean wouldLowerAvailability(int server, int region) {
      // availability would be lowered if the balancer chooses the node to move to as the node
      // where the replica is
      Set<HRegionInfo> set = serverToReplicaMap.get(server);
      HRegionInfo hri = regions[region].getPrimaryRegionInfo();
      if (set.contains(hri)) return true;
      //TODO: add same host checks (if more than 1 host,then don't place replicas on the same host)
      // also availability would be lowered if the balancer chooses the node to move to a
      // node in the same rack (if there were multiple racks to choose from)
      if (uniqueRacks > 1) {
        String destRack = rackManager.getRack(servers[server]);
        if (rackToReplicaMap.get(destRack).contains(hri)) {
          // the destination rack contains a replica (primary or secondary)
          return true;
        }
        //TODO: handle the case of more than one replica
      }
      return false;
    }

    /** Region moved out of the server */
    void regionMoved(int regionIndex, int oldServerIndex, int newServerIndex) {
      regionIndexToServerIndex[regionIndex] = newServerIndex;
      if (initialRegionIndexToServerIndex[regionIndex] == newServerIndex) {
        numMovedRegions--; //region moved back to original location
        if (regions[regionIndex].isMetaRegion()) {
          numMovedMetaRegions--;
        }
      } else if (initialRegionIndexToServerIndex[regionIndex] == oldServerIndex) {
        numMovedRegions++; //region moved from original location
        if (regions[regionIndex].isMetaRegion()) {
          numMovedMetaRegions++;
        }
      }
      int tableIndex = regionIndexToTableIndex[regionIndex];
      numRegionsPerServerPerTable[oldServerIndex][tableIndex]--;
      numRegionsPerServerPerTable[newServerIndex][tableIndex]++;

      //check whether this caused maxRegionsPerTable in the new Server to be updated
      if (numRegionsPerServerPerTable[newServerIndex][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
        numRegionsPerServerPerTable[newServerIndex][tableIndex] = numMaxRegionsPerTable[tableIndex];
      } else if ((numRegionsPerServerPerTable[oldServerIndex][tableIndex] + 1)
          == numMaxRegionsPerTable[tableIndex]) {
        //recompute maxRegionsPerTable since the previous value was coming from the old server
        for (int serverIndex = 0 ; serverIndex < numRegionsPerServerPerTable.length; serverIndex++) {
          if (numRegionsPerServerPerTable[serverIndex][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
            numMaxRegionsPerTable[tableIndex] = numRegionsPerServerPerTable[serverIndex][tableIndex];
          }
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
  private RackManager rackManager;
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
  }

  protected void setSlop(Configuration conf) {
    this.slop = conf.getFloat("hbase.regions.slop", (float) 0.2);
  }

  @Override
  public Configuration getConf() {
    return this.config;
  }

  public void setClusterStatus(ClusterStatus st) {
    // Not used except for the StocasticBalancer
  }

  public void setMasterServices(MasterServices masterServices) {
    this.services = masterServices;
  }

  protected boolean needsBalance(ClusterLoadState cs) {
    if (cs.getNumServers() < MIN_SERVER_BALANCE) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not running balancer because only " + cs.getNumServers()
            + " active regionserver(s)");
      }
      return false;
    }
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
  public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) {
    metricsBalancer.incrMiscInvocations();

    if (regions.isEmpty() || servers.isEmpty()) {
      return null;
    }
    Map<ServerName, List<HRegionInfo>> assignments = new TreeMap<ServerName, List<HRegionInfo>>();
    // Get the snapshot of the current assignments for the regions in question, and then create
    // a cluster out of it. Note that we might have replicas already assigned to some servers
    // earlier. So we want to get the snapshot to see those assignments.
    Pair<Map<ServerName, List<HRegionInfo>>, Map<String, List<HRegionInfo>>> currentAssignments =
      getCurrentAssignmentSnapshot(servers, regions);
    int uniqueRacks = getUniqueRacks(servers);
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
        if (wouldLowerAvailability(currentAssignments.getFirst(), currentAssignments.getSecond(),
            uniqueRacks, server, region)) {
          unassignedRegions.add(region);
        } else {
          serverRegions.add(region);
        }
        // also update the assignments for checking availability
        updateAvailabilityCheckerMaps(serverRegions, server, currentAssignments.getFirst(),
            currentAssignments.getSecond());
      }
      assignments.put(server, serverRegions);
      regionIdx++;
    }
    List<HRegionInfo> lastFewRegions = new ArrayList<HRegionInfo>();
    lastFewRegions.addAll(unassignedRegions);
    // assign the unassigned regions by going through the list and doing straight
    // round robin
    for (HRegionInfo hri : unassignedRegions) {
      //TODO: remember the index every iteration (don't start from zero)
      for (int j = 0; j < numServers; j++) {
        if (!wouldLowerAvailability(currentAssignments.getFirst(), currentAssignments.getSecond(),
            uniqueRacks, servers.get(j), hri)) {
          List<HRegionInfo> hris = assignments.get(servers.get(j));
          if (hris == null) {
            hris = new ArrayList<HRegionInfo>();
          }
          hris.add(hri);
          assignments.put(servers.get(j), hris);
          // also update the assignments for checking availability
          List<HRegionInfo> h = new ArrayList<HRegionInfo>();
          h.add(hri);
          updateAvailabilityCheckerMaps(h, servers.get(j), currentAssignments.getFirst(),
              currentAssignments.getSecond());
          lastFewRegions.remove(hri);
          break;
        }
      }
    }
    // just sprinkle the rest of the regions on random regionservers. The balanceCluster will
    // make it optimal later
    for (HRegionInfo hri : lastFewRegions) {
      int i = RANDOM.nextInt(numServers);
      List<HRegionInfo> hris = assignments.get(servers.get(i));
      if (hris == null) {
        hris = new ArrayList<HRegionInfo>();
      }
      hris.add(hri);
      assignments.put(servers.get(i), hris);
    }
    return assignments;
  }

  boolean wouldLowerAvailability(Map<ServerName, List<HRegionInfo>> serverToRegionAssignments,
      Map<String, List<HRegionInfo>> rackToRegionAssignments,
      int uniqueRacks,
      ServerName server, HRegionInfo region) {
    HRegionInfo hri = region.getPrimaryRegionInfo();
    List<HRegionInfo> regions = serverToRegionAssignments.get(server);
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

  private void updateAvailabilityCheckerMaps(List<HRegionInfo>serverRegions, ServerName server,
      Map<ServerName, List<HRegionInfo>> serverToRegions,
      Map<String, List<HRegionInfo>> rackToRegions) {
    List<HRegionInfo> hris = serverToRegions.get(server);
    if (hris == null) {
      hris = new ArrayList<HRegionInfo>();
      serverToRegions.put(server, hris);
    }
    // add the primary regions - since for the comparisons in wouldLowerAvailability
    // we use the primary regioninfo
    for (HRegionInfo hri : serverRegions) {
      hris.add(hri.getPrimaryRegionInfo());
    }
    if (rackManager != null) {
      hris = rackToRegions.get(rackManager.getRack(server));
      if (hris == null) {
        hris = new ArrayList<HRegionInfo>();
        rackToRegions.put(rackManager.getRack(server), hris);
      }
      for (HRegionInfo hri : serverRegions) {
        hris.add(hri.getPrimaryRegionInfo());
      }
    }
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
  public ServerName randomAssignment(HRegionInfo regionInfo, List<ServerName> servers) {
    metricsBalancer.incrMiscInvocations();

    if (servers == null || servers.isEmpty()) {
      LOG.warn("Wanted to do random assignment but no servers to assign to");
      return null;
    }
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
    regions.add(regionInfo);
    // Get the snapshot of the current assignments for the regions in question, and then create
    // a cluster out of it. Note that we might have replicas already assigned to some servers
    // earlier. So we want to get the snapshot to see those assignments.
    Pair<Map<ServerName, List<HRegionInfo>>, Map<String, List<HRegionInfo>>> currentAssignments =
            getCurrentAssignmentSnapshot(servers, regions);
    int uniqueRacks = getUniqueRacks(servers);
    ServerName server = null;
    do {
      server = servers.get(RANDOM.nextInt(servers.size()));
    } while (servers.size() != 1 && wouldLowerAvailability(currentAssignments.getFirst(),
        currentAssignments.getSecond(), uniqueRacks, server, regionInfo));
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
    Pair<Map<ServerName, List<HRegionInfo>>, Map<String, List<HRegionInfo>>> currentAssignments =
            getCurrentAssignmentSnapshot(servers, new ArrayList<HRegionInfo>(regions.keySet()));
    int uniqueRacks = getUniqueRacks(servers);
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
        ServerName randomServer = null;
        do {
          randomServer = servers.get(RANDOM.nextInt(servers.size()));
        } while (servers.size() != 1 && wouldLowerAvailability(currentAssignments.getFirst(),
            currentAssignments.getSecond(), uniqueRacks, randomServer, region));
        assignments.get(randomServer).add(region);
        // also update the assignments for checking availability
        updateAvailabilityCheckerMaps(assignments.get(randomServer), randomServer,
            currentAssignments.getFirst(), currentAssignments.getSecond());
        numRandomAssignments++;
        if (oldServerName != null) oldHostsNoLongerPresent.add(oldServerName.getHostname());
      } else if (localServers.size() == 1) {
        // the usual case - one new server on same host
        assignments.get(localServers.get(0)).add(region);
        numRetainedAssigments++;
      } else {
        // multiple new servers in the cluster on this same host
        int size = localServers.size();
        ServerName target = null;
        do {
          target = localServers.get(RANDOM.nextInt(size));
        } while (localServers.size() != 1 && wouldLowerAvailability(currentAssignments.getFirst(),
            currentAssignments.getSecond(), uniqueRacks, target, region));
        assignments.get(target).add(region);
        // also update the assignments for checking availability
        updateAvailabilityCheckerMaps(assignments.get(target), target,
            currentAssignments.getFirst(), currentAssignments.getSecond());
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

  private Pair<Map<ServerName, List<HRegionInfo>>, Map<String, List<HRegionInfo>>>
  getCurrentAssignmentSnapshot(List<ServerName> servers, List<HRegionInfo> regions) {
    Pair<Map<ServerName, List<HRegionInfo>>, Map<String, List<HRegionInfo>>> currentAssignments;
    if (this.services != null) {
      currentAssignments = this.services.getAssignmentManager().getSnapShotOfAssignment(regions);
    } else { //create empty datastructures .. lot of code.
      currentAssignments = new Pair<Map<ServerName, List<HRegionInfo>>,
          Map<String, List<HRegionInfo>>>();
      Map<ServerName, List<HRegionInfo>> a = new HashMap<ServerName, List<HRegionInfo>>(0);
      Map<String, List<HRegionInfo>> b = new HashMap<String, List<HRegionInfo>>(0);
      currentAssignments.setFirst(a);
      currentAssignments.setSecond(b);
    }
    return currentAssignments;
  }

  private int getUniqueRacks(List<ServerName> servers) {
    //for this round of assignment, how many racks are we talking about (TODO: need to access
    //the rackManager via the master or something)
    int uniqueRacks = 1;
    if (rackManager != null) {
      List<String> racks = rackManager.getRack(servers);
      Set<String> rackSet = new HashSet<String>(racks);
      uniqueRacks = rackSet.size();
    }
    return uniqueRacks;
  }

  @Override
  public void initialize() throws HBaseIOException{
    rackManager = new RackManager(getConf());
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
}
