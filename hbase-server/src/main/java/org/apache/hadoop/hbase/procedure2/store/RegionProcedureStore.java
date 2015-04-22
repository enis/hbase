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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A ProcedureStore implementation where we keep the state of the procedures in a single region.
 * The procId is the row key, and the state of the procedure is serialized in info:data column
 */
@InterfaceAudience.Private
public class RegionProcedureStore implements ProcedureStore {

  private static final Log LOG = LogFactory.getLog(RegionProcedureStore.class);

  public static TableName PROCEDURES_TABLE_NAME = TableName.valueOf(
    NamespaceDescriptor.SYSTEM_NAMESPACE_NAME, Bytes.toBytes("procedures"));

  private static byte[] FAMILY = Bytes.toBytes("info");
  private static byte[] QUALIFIER = Bytes.toBytes("data");

  private static HTableDescriptor TABLE_DESCRIPTOR
    = new HTableDescriptor(RegionProcedureStore.PROCEDURES_TABLE_NAME)
      .addFamily(new HColumnDescriptor(RegionProcedureStore.FAMILY));

  private Connection connection;
  private Table table;
  private AtomicBoolean running = new AtomicBoolean();

  private final CopyOnWriteArrayList<ProcedureStoreListener> listeners =
      new CopyOnWriteArrayList<ProcedureStoreListener>();

  public static HTableDescriptor getTableDescriptor() {
    return TABLE_DESCRIPTOR;
  }

  public RegionProcedureStore(Connection connection) throws IOException {
    this.connection = connection;
    this.table = connection.getTable(PROCEDURES_TABLE_NAME);
  }

  @Override
  public void registerListener(ProcedureStoreListener listener) {
    listeners.add(listener);
  }

  @Override
  public boolean unregisterListener(ProcedureStoreListener listener) {
    return listeners.remove(listener);
  }

  @Override
  public void recoverLease() throws IOException {
    // no op. Already done in BoostrapTableService
  }

  @Override
  public void start(int numThreads) throws IOException {
    // TODO: check bootstrap service
    if (running.getAndSet(true)) {
      return;
    }
  }

  @Override
  public int getNumThreads() {
    return 1;
  }

  @Override
  public void stop(boolean abort) {
    if (!running.getAndSet(false)) {
      return;
    }
    try {
      connection.close();
    } catch (IOException e) {
      LOG.warn("Received IOException closing the connection", e);
    }
  }

  @Override
  public boolean isRunning() {
    // TODO: check bootstrap service
    return running.get();
  }

  private void sendAbortProcessSignal() {
    if (!this.listeners.isEmpty()) {
      for (ProcedureStoreListener listener : this.listeners) {
        listener.abortProcess();
      }
    }
  }

  @Override
  public Iterator<Procedure> load() throws IOException {
    ResultScanner scanner = table.getScanner(new Scan());

    final Iterator raw = scanner.iterator();

    return new Iterator() {
      @Override
      public boolean hasNext() {
        return raw.hasNext();
      }

      @Override
      public Object next() {
        Result result = (Result) raw.next();
        if (result == null) {
          return null;
        }
        Procedure proc;
        try {
          proc = deserializeProcedure(result);
          return proc;
        } catch (IOException e) {
          LOG.warn("Received exception, skipping Procedure", e);
          if (hasNext()) {
            return next();
          } else {
            return null;
          }
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public void insert(Procedure proc, Procedure[] subprocs) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("insert " + proc + " subproc=" + Arrays.toString(subprocs));
    }

    try {
      Put put = serializeProcedure(proc);
      List<Put> puts;
      if (subprocs != null) {
        puts = new ArrayList<Put>(subprocs.length + 1);
        puts.add(put);
        for (int i = 0; i < subprocs.length; ++i) {
          puts.add(serializeProcedure(subprocs[i]));
        }
        table.put(puts);  // Assume atomic update using HRegion.mutateRowsWithLocks()
      } else {
        assert !proc.hasParent();
        table.put(put);
      }
    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      LOG.fatal("Unable to serialize one of the procedure: proc=" + proc +
                " subprocs=" + Arrays.toString(subprocs), e);
      sendAbortProcessSignal();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void update(Procedure proc) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("update " + proc);
    }
    try {
      Put put = serializeProcedure(proc);
      table.put(put);
    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      LOG.fatal("Unable to serialize the procedure: proc=" + proc, e);
      sendAbortProcessSignal();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(long procId) {
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace("delete " + procId);
      }
      Delete delete = new Delete(Bytes.toBytes(procId));
      table.delete(delete);
    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      LOG.fatal("Unable to serialize the procedure: " + procId, e);
      sendAbortProcessSignal();
      throw new RuntimeException(e);
    }
  }

  protected Put serializeProcedure(Procedure proc)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Procedure.convert(proc).writeDelimitedTo(out);

    Put put = new Put(Bytes.toBytes(proc.getProcId()));
    put.addImmutable(FAMILY, QUALIFIER, out.toByteArray());

    // TODO: We do not have FSYNC WAL implemented yet (it does hflush). Still specify it so that
    // once we have it implemented, we start using it.
    put.setDurability(Durability.FSYNC_WAL);

    return put;
  }

  protected Procedure deserializeProcedure(Result result)
      throws IOException {

    long procId = Bytes.toLong(result.getRow());
    Cell cell = result.getColumnLatestCell(FAMILY, QUALIFIER);
    if (cell == null) {
      throw new IOException("Deserializing of procedure failed from Result: " + result);
    }
    InputStream in = new ByteArrayInputStream(cell.getValueArray(),
      cell.getValueOffset(), cell.getValueLength());

    return Procedure.convert(ProcedureProtos.Procedure.parseDelimitedFrom(in));
  }
}
