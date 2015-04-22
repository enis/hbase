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

package org.apache.hadoop.hbase.region;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.region.EmbeddedDatabase.EmbeddedTable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestEmbeddedDatabase {

  private static final Log LOG = LogFactory.getLog(TestEmbeddedDatabase.class);

  // test WALs are deleted
  // test files are deleted

  private static HBaseTestingUtility util;

  private EmbeddedDatabase db;
  private ServerName serverName;
  private Path rootDir;
  private HMaster master;

  private static final byte[] FAMILY = Bytes.toBytes("fam");

  @BeforeClass
  public static void setupClass() throws Exception {
    util = new HBaseTestingUtility();

    Configuration conf = util.getConfiguration();

    // less number of retries
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 10);
    conf.setLong(HConstants.HBASE_CLIENT_PAUSE, 5);

    util.startMiniCluster(1); // EmbeddedDb can only run inside an RS for now.
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException {
    serverName = createServerName();
    rootDir = new Path(util.getDataTestDirOnTestFS("embeddeddb"), UUID.randomUUID().toString());

    master = util.getMiniHBaseCluster().hbaseCluster.getActiveMaster();

    db = new EmbeddedDatabase(util.getConfiguration(), serverName,
      master, master, master, rootDir);

    db.startAndWait();
  }

  @After
  public void tearDown() throws InterruptedException, ExecutionException, TimeoutException {
    db.stop().get(60, TimeUnit.SECONDS);
  }

  private ServerName createServerName() {
    return ServerName.valueOf("localhost", RandomUtils.nextInt(65000),
      Math.abs(RandomUtils.nextLong()));
  }

  private HTableDescriptor createTableDescriptor(String tableName) {
    HTableDescriptor htd
    = new HTableDescriptor(TableName.valueOf(tableName))
      .addFamily(new HColumnDescriptor(FAMILY));
    return htd;
  }

  @Test
  public void testCreateTable() throws IOException {
    try (Connection connection = db.createConnection();
        Admin admin = connection.getAdmin()) {

      for (int i = 0; i < 10; i++) {
        HTableDescriptor htd = createTableDescriptor("hbase:testCreateTable-" + i);
        admin.createTable(htd);

        assertTrue(admin.isTableEnabled(htd.getTableName()));
        assertTrue(admin.isTableAvailable(htd.getTableName()));
      }

      HTableDescriptor[] htd = admin.listTables();
      assertEquals(10, htd.length);
    }
  }

  public void readWrite(String tableName) throws IOException {
    HTableDescriptor htd = createTableDescriptor(tableName);
    try (Connection connection = db.createConnection();
        Admin admin = connection.getAdmin()) {
      admin.createTable(htd);

      try (Table table = connection.getTable(htd.getTableName())) {
        // Basic put
        util.loadNumericRows(table, FAMILY, 0, 100);

        // Basic get
        util.verifyNumericRows(table, FAMILY, 0, 100, 0);

        // scan
        assertEquals(100, util.countRows(table));

        // multi put
        util.loadNumericRowsMultiPut(table, FAMILY, 100, 200);
        util.verifyNumericRows(table, FAMILY, 0, 200, 0);
        assertEquals(200, util.countRows(table));
      }
    }
  }

  @Test
  public void testReadWrite() throws IOException {
    readWrite("hbase:testReadWrite");
  }

  /**
   * Tests durability using WAL
   * @throws IOException
   */
  @Test
  public void testDurability() throws IOException {
    LOG.info("Writing some data");
    readWrite("hbase:testDurability");

    LOG.info("Stopping embedded db");
    db.stopAndWait();

    LOG.info("Opening another embedded db");
    EmbeddedDatabase db1 = new EmbeddedDatabase(util.getConfiguration(),
      createServerName(), master, master, master, rootDir);
    db1.startAndWait();

    try (Connection connection = db1.createConnection();
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf("hbase:testDurability"))) {

      admin.isTableEnabled(TableName.valueOf("hbase:testDurability"));

      // verify data is still there
      assertEquals(200, util.countRows(table));
    }

    db1.stopAndWait();
  }

  @Test
  public void testFencing() throws IOException {
    LOG.info("Writing some data");
    readWrite("hbase:testFencing");

    db.stopAndWait();
    LOG.info("Opening embedded db");
    db = new EmbeddedDatabase(util.getConfiguration(),
      createServerName(), null, null, master, rootDir); // pass in null as abortable so that mini
                                                        // cluster is not aborted
    db.startAndWait();

    LOG.info("Opening another embedded db");
    EmbeddedDatabase db1 = new EmbeddedDatabase(util.getConfiguration(),
      createServerName(), master, master, master, rootDir);
    db1.startAndWait();

    // ensure that we cannot write to db anymore. It is fenced.

    try (Connection connection = db.createConnection();
        Table table = connection.getTable(TableName.valueOf("hbase:testFencing"))) {

      try {
        // now should not not be able to write to the original db
        util.loadNumericRows(table, FAMILY, 200, 300);
        fail("Should have failed to write");
      } catch (Exception ex) {
        // expected
      }

      try (Connection connection1 = db1.createConnection();
          Table table1 = connection1.getTable(TableName.valueOf("hbase:testFencing"))) {
        // verify data is still there
        assertEquals(200, util.countRows(table1));

        // verify we can still write
        util.loadNumericRows(table1, FAMILY, 200, 300);
        assertEquals(300, util.countRows(table1));
      }
    }

    db.stopAndWait();
    try (Connection connection1 = db1.createConnection();
        Table table1 = connection1.getTable(TableName.valueOf("hbase:testFencing"))) {
      // verify we can still write
      util.loadNumericRows(table1, FAMILY, 300, 400);
      assertEquals(400, util.countRows(table1));
    }

    db1.stopAndWait();
  }

  private void reopen(Configuration conf) throws IOException {
    db.stopAndWait();
    db = new EmbeddedDatabase(conf,
      createServerName(), master, master, master, rootDir);
    db.startAndWait();
  }

  /**
   * Test that we can flush normally
   */
  @Test
  public void testFlushes() throws IOException {
    Configuration conf = new Configuration(util.getConfiguration());
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 30);
    conf.setLong(HConstants.HBASE_CLIENT_PAUSE, 100);
    // reopen
    reopen(conf);

    readWrite("hbase:testFlushes");

    try (Connection connection = db.createConnection();
        Table table = connection.getTable(TableName.valueOf("hbase:testFlushes"))) {

      util.loadNumericRows(table, FAMILY, 200, 300);
      Region region = ((EmbeddedTable)table).getRegion();
      LOG.info("numStoreFiles=" + region.getStore(FAMILY).getStorefilesCount());
      assertTrue(region.getStore(FAMILY).getStorefilesCount() > 1);

      util.verifyNumericRows(table, FAMILY, 0, 300, 0);
    }
  }

  /**
   * Test that we can compact normally
   */
  @Test
  public void testCompactions() throws IOException {
    Configuration conf = new Configuration(util.getConfiguration());
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 30);
    conf.setLong(HConstants.HBASE_CLIENT_PAUSE, 100);
    // reopen
    reopen(conf);

    readWrite("hbase:testCompactions");

    try (Connection connection = db.createConnection();
        Table table = connection.getTable(TableName.valueOf("hbase:testCompactions"))) {

      Region region = ((EmbeddedTable)table).getRegion();
      int start = 200;
      boolean compacted = false;
      while (start < 10000 && !compacted) {
        int storeFileCount = region.getStore(FAMILY).getStorefilesCount();
        LOG.info("numStoreFiles=" + storeFileCount);

        // put some data to trigger a flush
        util.loadNumericRows(table, FAMILY, start, start+100);

        // wait for some time to see whether we will compact
        for (int i = 0; i < 5; i++) {
          if (region.getStore(FAMILY).getStorefilesCount() < storeFileCount) {
            // then we have compacted
            compacted = true;
            break;
          }
          Threads.sleep(100);
        }
        start += 100;
      }

      assertTrue(compacted);
      util.verifyNumericRows(table, FAMILY, 0, start, 0);
    }
  }

  @Test
  public void testWALRolling() throws Exception {
    Configuration conf = new Configuration(util.getConfiguration());
    conf.setInt("hbase.regionserver.hlog.blocksize", 1024);
    // reopen
    reopen(conf);

    readWrite("hbase:testWALRolling");

    try (Connection connection = db.createConnection();
        Table table = connection.getTable(TableName.valueOf("hbase:testWALRolling"))) {

      Region region = ((EmbeddedTable)table).getRegion();

      WAL wal = ((HRegion)region).getWAL();
      final WALActionsListener listener = mock(WALActionsListener.class);
      wal.registerWALActionsListener(listener);

      util.loadNumericRows(table, FAMILY, 200, 300);

      verify(listener, atLeast(1)).logRollRequested(anyBoolean());

      // wait until WAL roll
      util.waitFor(30000, new Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          try {
            verify(listener, atLeast(1)).postLogRoll((Path)any(), (Path)any());
            return true;
          } catch (Throwable t) {
            return false;
          }
        };
      });
    }
  }

  /**
   * Tests a failover of the database.
   */
  @Test
  public void testFailover() throws IOException {
    // write some data
    readWrite("hbase:testFailover");

    LOG.info("ABORTING MASTER");
    // abort the abortable (master)
    MiniHBaseCluster cluster = util.getMiniHBaseCluster();
    ServerName server = cluster.getMaster().getServerName();
    cluster.abortMaster(0);
    cluster.waitForMasterToStop(server, 60000);
    // this stop will trigger abort code path since abortable is aborted
    LOG.info("ABORTING EmbeddedDB");
    db.stopAndWait();


    // start master again
    LOG.info("STARTING MASTER");
    cluster.startMaster();
    cluster.waitForActiveAndReadyMaster();
    master = util.getMiniHBaseCluster().hbaseCluster.getActiveMaster();

    db = new EmbeddedDatabase(util.getConfiguration(),
      createServerName(), master, master, master, rootDir);
    db.startAndWait();

    try (Connection connection = db.createConnection();
        Table table = connection.getTable(TableName.valueOf("hbase:testFailover"))) {
      util.verifyNumericRows(table, FAMILY, 0, 200, 0);
    }
  }
}
