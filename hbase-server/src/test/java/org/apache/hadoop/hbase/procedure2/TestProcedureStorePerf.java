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

package org.apache.hadoop.hbase.procedure2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.wal.*;
import org.apache.hadoop.hbase.procedure2.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

public class TestProcedureStorePerf {
  private static final Log LOG = LogFactory.getLog(ProcedureStoreTest.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private Path rootDir;
  private FileSystem fs;

  private static void setupConf(Configuration conf) {
//    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024 * 1024 * 32);
//    conf.setLong("hbase.hstore.compaction.min", 20);
//    conf.setLong("hbase.hstore.compaction.max", 39);
//    conf.setLong("hbase.hstore.blockingStoreFiles", 40);
    conf.setInt("hbase.regionserver.handler.count", 50);
  }

  @Before
  public void setup() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(1, 1);

    UTIL.waitUntilAllRegionsAssigned(TableName.META_TABLE_NAME);

    fs = UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    rootDir = UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    assertTrue(rootDir != null);
    assertTrue(fs != null);
  }

  @After
  public void tearDown() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  public static class DummyProcedure extends Procedure {

    public int compareTo(Object o) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    protected Procedure[] execute(Object env) throws ProcedureYieldException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected void rollback(Object env) throws IOException {
      // TODO Auto-generated method stub

    }

    @Override
    protected boolean abort(Object env) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    protected void serializeStateData(OutputStream stream) throws IOException {
      // TODO Auto-generated method stub

    }

    @Override
    protected void deserializeStateData(InputStream stream) throws IOException {
      // TODO Auto-generated method stub

    }

  }

  class Worker implements Runnable {
    final long start;
    public Worker(long start) {
      this.start = start;
    }
    @Override
    public void run() {
      while (true) {
        long procId = procIds.incrementAndGet();
        if (procId >= numProcs) {
          break;
        }
        //System.out.println("submitting:" + procId);
        try {
          Procedure proc = new DummyProcedure();
          proc.setProcId(procId);
          store.insert(proc, null);

          store.delete(procId);

          if (procId % 10000 == 0) {
            long ms = System.currentTimeMillis() - start;
            LOG.debug("Wrote " + procId + " procedures in " + StringUtils.humanTimeDiff(ms) +
                      String.format(" %.3fsec", ms / 1000.0f));
          }
        } catch (Throwable t) {
          t.printStackTrace();
          throw t;
        }
      }
    }
  }

  private int numProcs;
  private ProcedureStore store;
  private AtomicLong procIds;

  private long run(int numThreads) throws InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    long start = System.currentTimeMillis();
    for (int i = 0; i < numThreads; i++) {
      executor.submit(new Worker(start));
    }
    executor.shutdown();
    executor.awaitTermination(600, TimeUnit.SECONDS);
    return System.currentTimeMillis() - start;
  }

  private void runTest(final int numThreads, final int numProcs,
      final boolean useProcV2Wal, final boolean useHsync)
      throws Exception {
    final Path logDir = new Path(rootDir, "testLogs");
    long ms;
    try {
      UTIL.getConfiguration().setBoolean("hbase.procedure.store.wal.use.hsync", useHsync);
      if (useProcV2Wal) {
        store = new WALProcedureStore(UTIL.getConfiguration(), fs, logDir,
            new WALProcedureStore.LeaseRecovery() {
          @Override
          public void recoverFileLease(FileSystem fs, Path path) throws IOException {
            // no-op
          }
        });

        // YOU MUST SPECIFY THE NUMBER OF THREADS THAT ARE PUSHING STUFF TO MAKE THE WAL FAST
        fs.delete(logDir, true);
        store.start(Math.max(1, numThreads / UNDER_TUNE_WAL_THREAD_DIV));
        store.recoverLease();
      } else {
        store = UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getStore();
      }

//      if (true) {
//        store = new FSHLogProcedureStore(
//          UTIL.getMiniHBaseCluster().getMaster().getBootstrapTableService().getEmbeededDatabase().getWalContainer());
//        store.start(Math.max(1, numThreads / UNDER_TUNE_WAL_THREAD_DIV));
//      }

      procIds = new AtomicLong(0);
      this.numProcs = numProcs;

      ms = run(numThreads);
      LOG.info("Wrote " + numProcs + " procedures in " + StringUtils.humanTimeDiff(ms));
    } finally {
      store.stop(false);
    }
    fail("Wrote " + numProcs + " procedures with " + numThreads +
         " threads with useProcV2Wal=" + useProcV2Wal +
         " hsync=" + useHsync + " in " +
         StringUtils.humanTimeDiff(ms) +
         String.format(" (%.3fsec)", ms / 1000.0f));
  }

  private static final int UNDER_TUNE_WAL_THREAD_DIV = 1;
  private static final int ONE_M = 1000000;
  private static final int TEN_K = 100000;


  @Test
  public void runTestWith5ThreadsAndProcV2Wal() throws Exception {
    runTest(5, ONE_M, true, false);
  }

  @Test
  public void runTestWith10ThreadsAndProcV2Wal() throws Exception {
    runTest(10, ONE_M, true, false);
  }

  @Test
  public void runTestWith30ThreadsAndProcV2Wal() throws Exception {
    runTest(30, ONE_M, true, false);
  }

  @Test
  public void runTestWith50ThreadsAndProcV2Wal() throws Exception {
    runTest(50, ONE_M, true, false);
  }

  @Test
  public void runTestWith150ThreadsAndProcV2Wal() throws Exception {
    runTest(150, ONE_M, true, false);
  }
/*
  @Test
  public void runTestWith50ThreadsAndProcV2WalAndHSync() throws Exception {
    runTest(256, TEN_K, true, true);
  }
*/

  @Test
  public void runTestWith5ThreadsAndRegionStore() throws Exception {
    runTest(5, ONE_M, false, false);
  }

  @Test
  public void runTestWith10ThreadsAndRegionStore() throws Exception {
    runTest(10, ONE_M, false, false);
  }

  @Test
  public void runTestWith30ThreadsAndRegionStore() throws Exception {
    runTest(30, ONE_M, false, false);
  }

  @Test
  public void runTestWith50ThreadsAndRegionStore() throws Exception {
    runTest(50, ONE_M, false, false);
  }

  @Test
  public void runTestWith150ThreadsAndRegionStore() throws Exception {
    runTest(150, ONE_M, false, false);
  }
}