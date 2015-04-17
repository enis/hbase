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

package org.apache.hadoop.hbase.procedure2.store;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.hbase.procedure2.Procedure;

/**
 * In memory store for testing
 */
public class InMemoryProcedureStore implements ProcedureStore {

  private final CopyOnWriteArrayList<ProcedureStoreListener> listeners =
      new CopyOnWriteArrayList<ProcedureStoreListener>();

  private Map<Long, Procedure> procedures = new ConcurrentHashMap<>();

  @Override
  public void registerListener(ProcedureStoreListener listener) {
    listeners.add(listener);
  }

  @Override
  public boolean unregisterListener(ProcedureStoreListener listener) {
    return listeners.remove(listener);
  }

  @Override
  public void start(int numThreads) throws IOException {
  }

  @Override
  public void stop(boolean abort) {
  }

  @Override
  public boolean isRunning() {
    return true;
  }

  @Override
  public int getNumThreads() {
    return 0;
  }

  @Override
  public void recoverLease() throws IOException {
  }

  @Override
  public Iterator<Procedure> load() throws IOException {
    return procedures.values().iterator();
  }

  @Override
  public void insert(Procedure proc, Procedure[] subprocs) {
    procedures.put(proc.getProcId(), proc);
    if (subprocs == null) {
      return;
    }
    for (Procedure p : subprocs) {
      procedures.put(p.getProcId(), p);
    }
  }

  @Override
  public void update(Procedure proc) {
    procedures.put(proc.getProcId(), proc);
  }

  @Override
  public void delete(long procId) {
    procedures.remove(procId);
  }

}
