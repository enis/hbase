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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALContainer;
import org.apache.hadoop.hbase.wal.WALKey;

import com.google.common.annotations.VisibleForTesting;

public class FSHLogProcedureStore extends ProcedureStoreBase implements Abortable {

  private static final Log LOG = LogFactory.getLog(FSHLogProcedureStore.class);

  /** Similar to first meta region, regions of embedded tables are bootstrapped with region_id = 1*/
  private static final long DEFAULT_REGION_ID = 1L;

  public static TableName PROCEDURES_TABLE_NAME = TableName.valueOf(
    NamespaceDescriptor.SYSTEM_NAMESPACE_NAME, Bytes.toBytes("procedures"));

  private static byte[] FAMILY = Bytes.toBytes("i");
  private static byte[] QUALIFIER = Bytes.toBytes("d");

  private static HTableDescriptor TABLE_DESCRIPTOR
    = new HTableDescriptor(PROCEDURES_TABLE_NAME)
      .addFamily(new HColumnDescriptor(FAMILY));

//  private Configuration conf;
//  private Server server;
//  private ServerName serverName;
//  private WALContainer walContainer;
//  private final UncaughtExceptionHandler uncaughtExceptionHandler;

  private HTableDescriptor tableDescriptor;
  private HRegionInfo regionInfo;

  private WALContainer walContainer;
  private WAL wal;
  private final AtomicLong sequenceId;

  public FSHLogProcedureStore(WALContainer walContainer) throws IOException {
    this.walContainer = walContainer;
    this.sequenceId = new AtomicLong();

    this.tableDescriptor = TABLE_DESCRIPTOR;
    this.regionInfo = getRegionInfo(tableDescriptor);
  }

  /** Returns the only possible regionInfo for the table */
  private HRegionInfo getRegionInfo(HTableDescriptor htd) {
    return new HRegionInfo(htd.getTableName(), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
      false, DEFAULT_REGION_ID);
  }

  @VisibleForTesting
  WALContainer getWalContainer() {
    return walContainer;
  }

  @Override
  public void start(int numThreads) throws IOException {
    wal = walContainer.getWAL(null);
  }

  @Override
  public void abort(String why, Throwable e) {
//    if (this.abortable != null) {
//      abortable.abort(why, e);
//    }
  }

  @Override
  public void insert(Procedure proc, Procedure[] subprocs) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("insert " + proc + " subproc=" + Arrays.toString(subprocs));
    }

    WALKey walKey = new WALKey(regionInfo.getEncodedNameAsBytes(), tableDescriptor.getTableName());
    WALEdit walEdits = new WALEdit();

    try {
      List<Cell> cells = new ArrayList<Cell>(1);
      cells.add(serializeProcedure(proc));

      for (Cell c : cells) {
        walEdits.add(c);
      }

      long trxId = wal.append(tableDescriptor, regionInfo, walKey, walEdits, sequenceId, false, cells);
      wal.sync(trxId);
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
    insert(proc, null);
  }

  @Override
  public void delete(long procId) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("delete " + procId);
    }

    WALKey walKey = new WALKey(regionInfo.getEncodedNameAsBytes(), tableDescriptor.getTableName());
    WALEdit walEdits = new WALEdit();

    try {
      List<Cell> cells = new ArrayList<Cell>(1);
      cells.add(getDelete(procId));

      for (Cell c : cells) {
        walEdits.add(c);
      }

      long trxId = wal.append(tableDescriptor, regionInfo, walKey, walEdits, sequenceId, false, cells);
      wal.sync(trxId);

    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      LOG.fatal("Unable to serialize the procedure: " + procId, e);
      sendAbortProcessSignal();
      throw new RuntimeException(e);
    }
  }

  protected Cell serializeProcedure(Procedure proc)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Procedure.convert(proc).writeDelimitedTo(out);

    return new KeyValue(Bytes.toBytes(proc.getProcId()), FAMILY, QUALIFIER,
      EnvironmentEdgeManager.currentTime(), out.toByteArray());
  }

  protected Cell getDelete(long procId)
      throws IOException {
    return new KeyValue(Bytes.toBytes(procId), FAMILY, QUALIFIER,
      EnvironmentEdgeManager.currentTime(), Type.Delete, null);
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

  @Override
  public void stop(boolean abort) {
    // TODO Auto-generated method stub

  }

  @Override
  public void recoverLease() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public Iterator<Procedure> load() throws IOException {
    return new ArrayList().iterator();
  }

  @Override
  public boolean isAborted() {
    return false;
  }

}
