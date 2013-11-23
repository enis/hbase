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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMasterReplicaRegions {
  final static Log LOG = LogFactory.getLog(TestRegionPlacement.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static HBaseAdmin admin;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    TEST_UTIL.startMiniCluster(1);
    admin = new HBaseAdmin(conf);
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
    List<byte[]> rows = TEST_UTIL.getMetaTableRows(table);
    assert(rows.size() == numRegions); //only primary regions
    for (byte[] row : rows) {
      LOG.info(row);
    }
    assert(TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
        .getAssignmentsByTable().size() == numRegions * numReplica);
    //assert(false);
    //SnapshotOfRegionAssignmentFromMeta snapShot = new SnapshotOfRegionAssignmentFromMeta(tracker)
  }
}
