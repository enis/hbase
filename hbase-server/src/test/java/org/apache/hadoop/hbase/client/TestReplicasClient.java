package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for region replicas. Sad that we cannot isolate these without bringing up a whole
 * cluster. See {@link org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster}.
 */
public class TestReplicasClient {
  private static final Log LOG = LogFactory.getLog(TestReplicasClient.class);

  private static final int NB_SERVERS = 2;
  private static HTable table;
  private static final byte[] row = TestReplicasClient.class.getName().getBytes();

  private static HRegionInfo hriPrimary;
  private static HRegionInfo hriSecondary;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;


  /**
   * This copro will slow down the main replica only.
   */
  public static class SlowMeCopro extends BaseRegionObserver {
    static final AtomicLong sleepTime = new AtomicLong(0);
    static final AtomicReference<CountDownLatch> cdl =
        new AtomicReference<CountDownLatch>(new CountDownLatch(0));

    public SlowMeCopro() {
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
                         final Get get, final List<Cell> results) throws IOException {

      if (e.getEnvironment().getRegion().getRegionInfo().isPrimaryReplica()) {
        CountDownLatch latch = cdl.get();
        try {
          if (sleepTime.get() > 0) {
            LOG.info("Sleeping for " + sleepTime.get() + " ms");
            Thread.sleep(sleepTime.get());
          } else if (latch.getCount() > 0) {
            LOG.info("Waiting for the counterCountDownLatch");
            latch.await(2, TimeUnit.MINUTES); // To help the tests to finish.
            if (latch.getCount() > 0) {
              throw new RuntimeException("Can't wait more");
            }
          }
        } catch (InterruptedException e1) {
          Thread.interrupted();
        }
      } else {
        LOG.info("We're not the primary replicas."); // Hum. Do we have copro on the secondary region?
      }
    }
  }

  @BeforeClass
  public static void before() throws Exception {
    HTU.startMiniCluster(NB_SERVERS);

    // Create table then get the single region for our new table.
    HTableDescriptor hdt = HTU.createTableDescriptor(TestReplicasClient.class.getSimpleName());
    hdt.addCoprocessor(SlowMeCopro.class.getName());
    table = HTU.createTable(hdt, new byte[][]{f}, HTU.getConfiguration());

    hriPrimary = table.getRegionLocation(row, false).getRegionInfo();

    for (int i = 0; i < 100; i++) {
      HTU.getHBaseAdmin().getConnection().clearRegionCache();
      hriPrimary = table.getRegionLocation(row, false).getRegionInfo();
    }

    // mock a secondary region info to open
    hriSecondary = new HRegionInfo(hriPrimary.getTable(), hriPrimary.getStartKey(),
        hriPrimary.getEndKey(), hriPrimary.isSplit(), hriPrimary.getRegionId(), 1);

    // No master
    LOG.info("Master is going to be stopped");
    HTU.getHBaseCluster().getMaster().stopMaster();
    Configuration c = new Configuration(HTU.getConfiguration());
    c.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    HBaseAdmin ha = new HBaseAdmin(c);
    for (boolean masterRuns = true; masterRuns; ) {
      Thread.sleep(100);
      try {
        masterRuns = false;
        masterRuns = ha.isMasterRunning();
      } catch (MasterNotRunningException ignored) {
      }
    }
    LOG.info("Master has stopped");
    Thread.sleep(1000);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    Thread.sleep(1000);
    table.close();
    HTU.shutdownMiniCluster();
  }

  @After
  public void after() throws Exception {
    // Clean the state if the test failed before cleaning the znode
    // It does not manage all bad failures, so if there are multiple failures, only
    //  the first one should be looked at.
    }

  private HRegionServer getRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(0);
  }

  private void openRegion(HRegionInfo hri) throws Exception {
    ZKAssign.createNodeOffline(HTU.getZooKeeperWatcher(), hri, getRS().getServerName());
    // first version is '0'
    AdminProtos.OpenRegionRequest orr = RequestConverter.buildOpenRegionRequest(hri, 0, null);
    AdminProtos.OpenRegionResponse responseOpen = getRS().openRegion(null, orr);
    Assert.assertTrue(responseOpen.getOpeningStateCount() == 1);
    Assert.assertTrue(responseOpen.getOpeningState(0).
        equals(AdminProtos.OpenRegionResponse.RegionOpeningState.OPENED));
    checkRegionIsOpened(hri);
  }

  private void closeRegion(HRegionInfo hri) throws Exception {
    ZKAssign.createNodeClosing(HTU.getZooKeeperWatcher(), hri, getRS().getServerName());

    AdminProtos.CloseRegionRequest crr = RequestConverter.buildCloseRegionRequest(
        hri.getEncodedName(), true);
    AdminProtos.CloseRegionResponse responseClose = getRS().closeRegion(null, crr);
    Assert.assertTrue(responseClose.getClosed());

    checkRegionIsClosed(hri);

    ZKAssign.deleteClosedNode(HTU.getZooKeeperWatcher(), hri.getEncodedName());
  }

  private void checkRegionIsOpened(HRegionInfo hri) throws Exception {

    while (!getRS().getRegionsInTransitionInRS().isEmpty()) {
      Thread.sleep(1);
    }

    Assert.assertTrue(ZKAssign.deleteOpenedNode(HTU.getZooKeeperWatcher(), hri.getEncodedName()));
  }


  private void checkRegionIsClosed(HRegionInfo hri) throws Exception {

    while (!getRS().getRegionsInTransitionInRS().isEmpty()) {
      Thread.sleep(1);
    }

    Assert.assertTrue(getRS().getOnlineRegion(hri.getEncodedNameAsBytes()) == null);

    Threads.sleep(10000);
    // We don't delete the znode here, because there is not always a znode.
  }


  @Test(timeout = 60000)
  public void testUseRegionWithoutReplica() throws Exception {
    byte[] b1 = "testUseRegionWithoutReplica".getBytes();

    Get g = new Get(b1);
    Result r = table.get(g);
    Assert.assertFalse(r.isStale());
  }

  @Test(timeout = 60000)
  public void testLocations() throws Exception {
    byte[] b1 = "testLocations".getBytes();
    HConnection hc = HTU.getHBaseAdmin().getConnection();

    hc.clearRegionCache();
    HRegionLocation hrl = hc.getRegionLocation(table.getName(), b1, true);
    Assert.assertTrue(hrl.getServerName() + ", getSecondaryServers" +  hrl.getSecondaryServers(), hrl.getSecondaryServers() == null || hrl.getSecondaryServers().isEmpty());

    hc.clearRegionCache();
    hrl = hc.getRegionLocation(table.getName(), b1, false);
    Assert.assertTrue(hrl.getSecondaryServers() == null || hrl.getSecondaryServers().isEmpty());

    // Same tests, with the data in the cache this time.
    hrl = hc.getRegionLocation(table.getName(), b1, false);
    Assert.assertTrue(hrl.getSecondaryServers() == null || hrl.getSecondaryServers().isEmpty());

    hrl = hc.getRegionLocation(table.getName(), b1, true);
    Assert.assertTrue(hrl.getSecondaryServers() == null || hrl.getSecondaryServers().isEmpty());

    openRegion(hriSecondary);
    try {
      hc.clearRegionCache();
      hrl = hc.getRegionLocation(table.getName(), b1, false);
      Assert.assertTrue(hrl.getSecondaryServers() != null);
      Assert.assertTrue(hrl.getSecondaryServers().size() == 1);
    } finally {
      closeRegion(hriSecondary);
    }
  }

  @Test(timeout = 60000)
  public void testGetNoResultNoStaleRegionWithReplica() throws Exception {
    openRegion(hriSecondary);
    byte[] b1 = "testUseRegionWithReplica".getBytes();

    try {
      // A get works and is not stale
      Get g = new Get(b1);
      Result r = table.get(g);
      Assert.assertFalse(r.isStale());
    } finally {
      closeRegion(hriSecondary);
    }
  }


  @Test
  public void testGetNoResultStaleRegionWithReplica() throws Exception {
    byte[] b1 = "testGetNoResultStaleRegionWithReplica".getBytes();

    openRegion(hriSecondary);
    try {
      SlowMeCopro.cdl.set(new CountDownLatch(1));

      Get g = new Get(b1);
      g.setConsistency(Consistency.EVENTUAL);
      Result r = table.get(g);
      Assert.assertTrue(r.isStale());

    } finally {
      SlowMeCopro.cdl.get().countDown();
      closeRegion(hriSecondary);
    }
  }

  @Test
  public void testGetNoResultNotStaleSleepRegionWithReplica() throws Exception {
    openRegion(hriSecondary);
    byte[] b1 = "testGetNoResultNotStaleSleepRegionWithReplica".getBytes();

    try {
      // We sleep; but we won't go to the stale region as we don't get the stale by default.
      SlowMeCopro.sleepTime.set(2000);
      Get g = new Get(b1);
      Result r = table.get(g);
      Assert.assertFalse(r.isStale());

    } finally {
      SlowMeCopro.sleepTime.set(0);
      closeRegion(hriSecondary);
    }
  }

  //@Test() // does not work without a master.
  public void testMove() throws Exception {
    openRegion(hriSecondary);
    try {
      HTU.getHBaseAdmin().move(hriPrimary.getEncodedNameAsBytes(), null);
      Get g = new Get("a get to be sure the move is finished".getBytes());
      table.get(g);
    } finally {
      closeRegion(hriSecondary);
    }
  }

  @Test()
  public void testFlushTable() throws Exception {
    openRegion(hriSecondary);
    try {
      HTU.getHBaseAdmin().flush(table.getTableName());

      Put p = new Put(row);
      p.add(f, row, row);
      table.put(p);

      HTU.getHBaseAdmin().flush(table.getTableName());
    } finally {
      Delete d = new Delete(row);
      table.delete(d);
      closeRegion(hriSecondary);
    }
  }

  @Test()
  public void testFlushPrimary() throws Exception {
    openRegion(hriSecondary);
    try {
      HTU.getHBaseAdmin().flush(hriPrimary.getEncodedNameAsBytes());

      Put p = new Put(row);
      p.add(f, row, row);
      table.put(p);

      HTU.getHBaseAdmin().flush(hriPrimary.getEncodedNameAsBytes());
    } finally {
      Delete d = new Delete(row);
      table.delete(d);
      closeRegion(hriSecondary);
    }
  }

  @Test()
  public void testFlushSecondary() throws Exception {
    openRegion(hriSecondary);
    try {
      HTU.getHBaseAdmin().flush(hriSecondary.getEncodedNameAsBytes());

      Put p = new Put(row);
      p.add(f, row, row);
      table.put(p);

      HTU.getHBaseAdmin().flush(hriSecondary.getEncodedNameAsBytes());
    } catch (TableNotFoundException expected) {
    } finally {
      Delete d = new Delete(row);
      table.delete(d);
      closeRegion(hriSecondary);
    }
  }

  @Test
  public void testUseRegionWithReplica() throws Exception {
    byte[] b1 = "testUseRegionWithReplica".getBytes();

    openRegion(hriSecondary);
    try {
      // A simple put works, even if there here a second replica
      Put p = new Put(b1);
      p.add(f, b1, b1);
      table.put(p);
      LOG.info("Put done");

      // A get works and is not stale
      Get g = new Get(b1);
      Result r = table.get(g);
      Assert.assertFalse(r.isStale());
      Assert.assertFalse(r.getColumnCells(f, b1).isEmpty());
      LOG.info("works and is not stale done");

      // Even if it we have to wait a little on the main region
      SlowMeCopro.sleepTime.set(2000);
      g = new Get(b1);
      r = table.get(g);
      Assert.assertFalse(r.isStale());
      Assert.assertFalse(r.getColumnCells(f, b1).isEmpty());
      SlowMeCopro.sleepTime.set(0);
      LOG.info("sleep and is not stale done");

      // But if we ask for stale we will get it
      SlowMeCopro.sleepTime.set(2000); // todo: ad cdl does not work here. Why?
      g = new Get(b1);
      g.setConsistency(Consistency.EVENTUAL);
      r = table.get(g);
      Assert.assertTrue(r.isStale());
      Assert.assertTrue(r.getColumnCells(f, b1).isEmpty());
      SlowMeCopro.sleepTime.set(0);

      LOG.info("stale done");

      // exists works and is not stale
      g = new Get(b1);
      g.setCheckExistenceOnly(true);
      r = table.get(g);
      Assert.assertFalse(r.isStale());
      Assert.assertTrue(r.getExists());
      LOG.info("exists not stale done");

      // exists works on stale but don't see the put
      SlowMeCopro.cdl.set(new CountDownLatch(1));
      g = new Get(b1);
      g.setCheckExistenceOnly(true);
      g.setConsistency(Consistency.EVENTUAL);
      r = table.get(g);
      Assert.assertTrue(r.isStale());
      Assert.assertFalse("The secondary has stale data", r.getExists());
      SlowMeCopro.cdl.get().countDown();
      LOG.info("exists stale before flush done");

      HTU.getHBaseAdmin().flush(table.getTableName());
      LOG.info("flush done");

      // get works and is not stale
      SlowMeCopro.cdl.set(new CountDownLatch(1));
      g = new Get(b1);
      g.setConsistency(Consistency.EVENTUAL);
      r = table.get(g);
      Assert.assertTrue(r.isStale());
      //  Assert.assertFalse(r.isEmpty()); // todo: how can we know that the flush was seen by the replica?
      SlowMeCopro.cdl.get().countDown();
      LOG.info("stale done");

      // exists works on stale and we see the put after the flush
      SlowMeCopro.cdl.set(new CountDownLatch(1));
      g = new Get(b1);
      g.setCheckExistenceOnly(true);
      g.setConsistency(Consistency.EVENTUAL);
      r = table.get(g);
      Assert.assertTrue(r.isStale());
      // Assert.assertTrue(r.getExists());   // todo: how can we know that the flush was seen by the replica?
      SlowMeCopro.cdl.get().countDown();
      LOG.info("exists stale after flush done");

    } finally {
      SlowMeCopro.cdl.get().countDown();
      SlowMeCopro.sleepTime.set(0);
      Delete d = new Delete(b1);
      table.delete(d);
      closeRegion(hriSecondary);
    }
  }
}
