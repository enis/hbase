/**
 *
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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMasterReplicaRegions {
  final static Log LOG = LogFactory.getLog(TestRegionPlacement.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static HBaseAdmin admin;
  private static int numSlaves = 2;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    TEST_UTIL.startMiniCluster(numSlaves);
    admin = new HBaseAdmin(conf);
    while(admin.getClusterStatus().getServers().size() != numSlaves) {
      Thread.sleep(100);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCreateTableWithMultipleReplicas() throws Exception {
    final TableName table = TableName.valueOf("fooTable");
    final int numRegions = 3;
    final int numReplica = 2;
    HTableDescriptor desc = new HTableDescriptor(table);
    desc.setNumRegionReplicas(numReplica);
    desc.addFamily(new HColumnDescriptor("family"));
    admin.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), numRegions);
    assert(admin.tableExists(table));
    CatalogTracker ct = new CatalogTracker(TEST_UTIL.getConfiguration());
    List<HRegionInfo> hris = MetaReader.getTableRegions(ct, table);
    assert(hris.size() == numRegions); //only primary regions
    // check that the master created expected number of RegionState objects
    for (int i = 0; i < numRegions; i++) {
      for (int j = 0; j < numReplica; j++) {
        HRegionInfo replica = hris.get(i).getRegionInfoForReplica(j);
        RegionState state = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionState(replica);
        assert (state != null);
      }
    }
    TEST_UTIL.waitTableEnabled(table.getName());
    List<Result> metaRows = MetaReader.fullScan(ct);
    int numRows = 0;
    for (Result result : metaRows) {
      Pair<HRegionInfo, ServerName[]> pair = HRegionInfo.getServerNamesFromMetaRowResult(result);
      HRegionInfo hri = pair.getFirst();
      if (!hri.getTable().equals(table)) continue;
      numRows += 1;
      ServerName[] servers = pair.getSecond();
      // have two locations for the replicas of a region, and the locations should be different
      assert(servers.length == 2);
      assert(!servers[0].equals(servers[1]));
    }
    assert(numRows == numRegions);
    
    // The same verification of the meta as above but with the SnapshotOfRegionAssignmentFromMeta
    // class
    SnapshotOfRegionAssignmentFromMeta snapshot = new SnapshotOfRegionAssignmentFromMeta(ct);
    snapshot.initialize();
    Map<HRegionInfo, ServerName> regionToServerMap = snapshot.getRegionToRegionServerMap();
    assert(regionToServerMap.size() == numRegions * numReplica + 1); //'1' for the namespace
    Map<ServerName, List<HRegionInfo>> serverToRegionMap = snapshot.getRegionServerToRegionMap();
    for (Map.Entry<ServerName, List<HRegionInfo>> entry : serverToRegionMap.entrySet()) {
      List<HRegionInfo> regions = entry.getValue();
      Set<byte[]> setOfStartKeys = new HashSet<byte[]>();
      for (HRegionInfo region : regions) {
        byte[] startKey = region.getStartKey();
        if (region.getTable().equals(table)) setOfStartKeys.add(startKey); //ignore namespace reg
      }
      assert(setOfStartKeys.size() == numRegions);
    }

    // Now kill the master, restart it and see if the assignments are kept
    ServerName master = TEST_UTIL.getHBaseClusterInterface().getClusterStatus().getMaster();
    TEST_UTIL.getHBaseClusterInterface().stopMaster(master);
    TEST_UTIL.getHBaseClusterInterface().waitForMasterToStop(master, 30000);
    TEST_UTIL.getHBaseClusterInterface().startMaster(master.getHostname());
    TEST_UTIL.getHBaseClusterInterface().waitForActiveAndReadyMaster();
    for (int i = 0; i < numRegions; i++) {
      for (int j = 0; j < numReplica; j++) {
        HRegionInfo replica = hris.get(i).getRegionInfoForReplica(j);
        RegionState state = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionState(replica);
        assert (state != null);
      }
    }
    snapshot = new SnapshotOfRegionAssignmentFromMeta(ct);
    snapshot.initialize();
    regionToServerMap = snapshot.getRegionToRegionServerMap();
    assert(regionToServerMap.size() == numRegions * numReplica + 1); //'1' for the namespace
    serverToRegionMap = snapshot.getRegionServerToRegionMap();
    for (Map.Entry<ServerName, List<HRegionInfo>> entry : serverToRegionMap.entrySet()) {
      List<HRegionInfo> regions = entry.getValue();
      Set<byte[]> setOfStartKeys = new HashSet<byte[]>();
      for (HRegionInfo region : regions) {
        byte[] startKey = region.getStartKey();
        if (region.getTable().equals(table)) setOfStartKeys.add(startKey); //ignore namespace reg
      }
      assert(setOfStartKeys.size() == numRegions);
    }

    // Now shut the whole cluster down, and verify the assignments are retained
    // TODO: fix the retainAssignment in BaseLoadBalancer
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.startup.retainassign", true);
    TEST_UTIL.shutdownMiniHBaseCluster();
    TEST_UTIL.startMiniHBaseCluster(1, numSlaves);
    TEST_UTIL.waitTableEnabled(table.getName());
    ct = new CatalogTracker(TEST_UTIL.getConfiguration());
    snapshot = new SnapshotOfRegionAssignmentFromMeta(ct);
    snapshot.initialize();
    regionToServerMap = snapshot.getRegionToRegionServerMap();
    assert(regionToServerMap.size() == numRegions * numReplica + 1); //'1' for the namespace
    serverToRegionMap = snapshot.getRegionServerToRegionMap();
    for (Map.Entry<ServerName, List<HRegionInfo>> entry : serverToRegionMap.entrySet()) {
      List<HRegionInfo> regions = entry.getValue();
      Set<byte[]> setOfStartKeys = new HashSet<byte[]>();
      for (HRegionInfo region : regions) {
        byte[] startKey = region.getStartKey();
        if (region.getTable().equals(table)) setOfStartKeys.add(startKey); //ignore namespace reg
      }
      assertEquals("setOfStartKeys.size() " + setOfStartKeys.size() + " numRegions " + numRegions + " " +
      printRegions(regions),
          setOfStartKeys.size(), numRegions);
    }
  }

  private String printRegions(List<HRegionInfo> regions) {
    StringBuffer strBuf = new StringBuffer();
    for (HRegionInfo r : regions) {
      strBuf.append(" ____ " + r.toString());
    }
    return strBuf.toString();
  }
}
