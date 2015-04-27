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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.procedure2.Procedure;

import com.google.common.collect.Lists;

/**
 * In memory store for testing
 */
public class InMemoryProcedureStore extends ProcedureStoreBase {

  private Map<Long, Procedure> procedures = new ConcurrentHashMap<>();

  @Override
  public void start(int numThreads) throws IOException {
    if (running.getAndSet(true)) {
      return;
    }
  }

  @Override
  public void stop(boolean abort) {
    if (!running.getAndSet(false)) {
      return;
    }
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
    // clone on the way out as well so that originals don't change
    List<Procedure> list = Lists.newArrayList();
    for (Procedure p : procedures.values()) {
      list.add(cloneProc(p));
    }
    return list.iterator();
  }

  @Override
  public void insert(Procedure proc, Procedure[] subprocs) {
    procedures.put(proc.getProcId(), cloneProc(proc));
    if (subprocs == null) {
      return;
    }
    for (Procedure p : subprocs) {
      procedures.put(p.getProcId(), cloneProc(p));
    }
  }

  /**
   * Clones the proc state so that further changes to the in memory proc are not reflected in the
   * store
   * @param proc
   * @return
   */
  private Procedure cloneProc(Procedure proc) {
    try {
      return Procedure.convert(Procedure.convert(proc));
    } catch (Exception ex) {
      throw new RuntimeException(ex); // should not happen
    }
  }

  @Override
  public void update(Procedure proc) {
    procedures.put(proc.getProcId(), cloneProc(proc));
  }

  @Override
  public void delete(long procId) {
    procedures.remove(procId);
  }

}
