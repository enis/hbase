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

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;

public class ProcedureStoreTest {

  private final int numProcs;

  private final ProcedureStore store;
  private final AtomicLong procIds;
  private HBaseTestingUtility util = new HBaseTestingUtility();

  public ProcedureStoreTest() throws Exception {
    util.startMiniCluster();
    //util.getConfiguration().setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024 * 1024 * 4);
    ProcedureExecutor executor = util.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    store = executor.getStore();

    procIds = new AtomicLong(0);
    numProcs = 1000000;
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
            System.out.println("Wrote " + procId + " procedures in " + ms + " ms");
          }
        } catch (Throwable t) {
          t.printStackTrace();
          //throw t;
        }
      }
    }
  }

  private void stop() throws Exception {
    util.shutdownMiniCluster();
  }

  private void run() throws InterruptedException {
    int numThreads = 10;

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    long start = System.currentTimeMillis();
    for (int i = 0; i < numThreads; i++) {
      executor.submit(new Worker(start));
    }
    executor.shutdown();
    executor.awaitTermination(600, TimeUnit.SECONDS);
    long ms = System.currentTimeMillis() - start;

    System.out.println("Wrote " + numProcs + " procedures in " + ms + " ms");
  }

  public static void main(String[] args) throws Exception {
    ProcedureStoreTest test = new ProcedureStoreTest();
    test.run();
    test.stop();
  }

}
