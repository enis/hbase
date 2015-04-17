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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.ClientSideRegionScanner;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.regionserver.CompactionRequestor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager;
import org.apache.hadoop.hbase.regionserver.OnlineRegions;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ServerNonceManager;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.wal.WALContainer;
import org.apache.hadoop.hbase.wal.WALServices;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.google.protobuf.Descriptors.MethodDescriptor;

/**
 * An EmbeddedDatabase can be thought of as a "mini instance" of HBase with a configured root
 * directory under HDFS or local FS. All data is kept in the configured root dir with table layout
 * and WAL etc layout similar to regular deployments. Some of the files and folders might be missing
 * in the embedded root dir (like version file, etc).
 *
 * <p> There is no hbase:meta table for now. Every table is assumed to be a single region, and the
 * region name is pre-determined. Tables cannot be split.
 *
 * <p>EmbeddedDatabase requires a passed in RegionServerServices for MemstoreFlusher and Compaction
 * threads. This means that is can only run inside a region server for now. We can change it so that
 * it can be used on its own.
 *
 * <p>Usage:
 * <pre>
 * EmbeddedDatabase db = new EmbeddedDatabase(..);
 * db.start().get();
 * Connection connection = db.createConnection();
 * Admin admin = db.getAdmin();
 * admin.createTable(myHtd);
 * admin.close();
 *
 * Table table = connection.getTable(TableName.valueOf("myTable"));
 * table.put(myPut);
 * table.get(myGet);
 *
 * </pre>
 */
public class EmbeddedDatabase extends AbstractService implements
  RegionServices, Abortable, Stoppable, Server {
  private static final Log LOG = LogFactory.getLog(EmbeddedDatabase.class);

  /** Similar to first meta region, regions of embedded tables are bootstrapped with region_id = 1*/
  private static final long DEFAULT_REGION_ID = 1L;

  private final HFileSystem fs;
  private final Path rootDir;
  private final Configuration conf;
  private final ServerName serverName;
  private final MasterFileSystem masterFs;
  private final FSTableDescriptors tableDescriptors;
  private EmbeddedRegionService regionService;
  private WALContainer walContainer;
  private final Abortable abortable;
  private final Stoppable stoppable;
  private final RegionServerServices regionServerServices;
  private final RetryCounterFactory retryCounterFactory;
  private final UncaughtExceptionHandler uncaughtExceptionHandler;
  private AtomicBoolean running = new AtomicBoolean(true);

  public EmbeddedDatabase(Configuration conf, ServerName serverName,
      Abortable abortable, Stoppable stoppable,
      RegionServerServices regionServerServices, Path rootDir)
      throws IOException {

    this.serverName = serverName;
    this.abortable = abortable;
    this.stoppable = stoppable;

    this.conf = new Configuration(conf); // clone the conf since we are going to change it

    // pass the procedure directory as the root directory for the WAL
    this.conf.set(HConstants.HBASE_DIR, rootDir.toString());
    this.rootDir = FSUtils.getRootDir(this.conf);

    boolean useHBaseChecksum = this.conf.getBoolean(HConstants.HBASE_CHECKSUM_VERIFICATION, true);
    this.fs = new HFileSystem(this.conf, useHBaseChecksum);

    this.masterFs = new MasterFileSystem(this, null, false, null);
    this.tableDescriptors = new FSTableDescriptors(this.conf, this.fs, this.rootDir);

    int retries = conf.getInt(
      HConstants.HBASE_CLIENT_RETRIES_NUMBER, HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    int pause = (int)conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
      HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.retryCounterFactory = new RetryCounterFactory(retries, pause);

    uncaughtExceptionHandler = new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        abort("Uncaught exception in service thread " + t.getName(), e);
      }
    };

    // Hosting regions with WALs require some number of services (flush, compaction, log rolling,
    // WAL cleanup and hfile cleanup.
    // In cases EmbeddedDatabase is run inside a region server, we can share some of the services
    // from the RS. This is done through using passed regionServerServices. We will reuse
    // MemstoreFlusher and CompactSplitThread from the passed one instead of spinning one of our own
    // However, we have our own WAL, LogRoller, and cleanup threads.
    this.regionServerServices = regionServerServices;
  }

  @Override
  protected void doStart() {
    try {
      bootstrapFsLayout();

      setupWAL();

      recoverWALs();

      startRegionService();

      Collection<TableDescriptor> existingTables = readTablesFromFs();

      openRegions(existingTables);

      notifyStarted();
    } catch (IOException ex) {
      notifyFailed(ex);
    }
  }

  @Override
  protected void doStop() {
    this.running.getAndSet(false);
    if (regionService != null) {
      try {
        regionService.stopAndWait();
      } catch (Exception ex) {
        LOG.error("Received exception while closing regin service", ex);
      }
    }

    if (walContainer != null) {
      boolean isAborted = abortable != null && abortable.isAborted();
      walContainer.shutdownWAL(!isAborted);
    }

    walContainer.stopServiceThreads();

    // do not close the FS

    notifyStopped();
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public FlushRequester getFlushRequester() {
    return regionServerServices.getFlushRequester();
  }

  @Override
  public CompactionRequestor getCompactionRequester() {
    return regionServerServices.getCompactionRequester();
  }

  @Override
  public FileSystem getFileSystem() {
    return fs;
  }

  @Override
  public HeapMemoryManager getHeapMemoryManager() {
    return regionServerServices.getHeapMemoryManager();
  }

  @Override
  public RegionServerAccounting getRegionServerAccounting() {
    return regionServerServices.getRegionServerAccounting();
  }

  @Override
  public ServerNonceManager getNonceManager() {
    return regionServerServices.getNonceManager();
  }

  @Override
  public Map<String, Region> getRecoveringRegions() {
    return regionServerServices.getRecoveringRegions();
  }

  private void bootstrapFsLayout() throws IOException {
    checkAndCreateDirectory(rootDir);

    // all embedded tables should be in hbase namespace (for now)
    Path nsDir = FSUtils.getNamespaceDir(rootDir, NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR);
    checkAndCreateDirectory(nsDir);
  }

  private void recoverWALs() throws IOException {
    // Only one embedded database should be active at the cluster at a single point in time for
    // the configured root directory. We guarantee this by doing fencing for all the other servers
    // that we find from the WALs dir.
    // We use the regular old fencing mechanism used for region servers and regions. We have 4
    // events regarding fencing:
    // (add)     -> Add our ServerName to the WAL directory (via initializing WALFactory)
    // (list)    -> List all ServerNames in WAL directory
    // (recover) -> Call recover lease for all ServerNames obtained from (list) minus our ServerName
    //              (recover) should be idempotent.
    // (add_data)-> Put some data to the region

    // We have to (add) as a first step, since otherwise, 2 servers can run do this:
    //
    //   SERVER-1      SERVER-2
    //    |              |
    //    (list)         |
    //    |              (list)
    //    (recover)      |
    //    |              (recover)
    //    (add)          |
    //    |              (add)
    //    (add_data)     (add_data)
    //
    // Above case won't achieve proper fencing, since neither server sees each other in (list),
    // they continue creating their directories and happily (add_data)

    // Thus, we should do (add) before list, which guarantees that if we reach (add_data), all other
    // known servers cannot add data. The only downside is that, two servers can fence each other:
    //   SERVER-1      SERVER-2
    //    |              |
    //    (add)          |
    //    |              (add)
    //    (list)         |
    //    |              |
    //    (recover)      (list)
    //    |              (recover)
    //    (add_data)     |
    //
    // if that happens, both servers (masters) will abort due to not (add_data). At this point, some
    // other server, or a restart of the servers will take over.
    // TODO: EmbeddedDatabase also can do DDL operations on the file system. Those operations
    // are not fenced as of now. We can do that by writing a dummy WAL entry before every DDL step.

    // Obtain a list of all previous servers. Only 1 server can be actively hosting.
    Set<ServerName> previousServers = masterFs.getFailedServersFromLogFolders();

    // we have already created our WAL file. Remove us from the list.
    previousServers.remove(serverName);

    // Rename WAL directories to append "-splitting" so that WALs cannot be rolled
    List<Path> walDirs = masterFs.renameWALDirs(previousServers);

    FileStatus[] walFiles = SplitLogManager.getFileList(conf, walDirs, null);
    if (walFiles == null || walFiles.length == 0) {
      return;
    }

    for (FileStatus walFile : walFiles) {
      // TODO: Progress reporter
      // Do not send the HFileSystem to WALSplitter
      if (WALSplitter.splitLogFile(rootDir, walFile,
            fs.getBackingFs(), conf, null, null, null, RecoveryMode.LOG_SPLITTING,
            walContainer.getWALFactory())) {
        WALSplitter.finishSplitLogFile(walFile.getPath().toString(), conf);
      }
    }

    for (Path walDir : walDirs) {
      LOG.debug("Cleaning up WAL directory:" + walDir);
      try {
        if (fs.exists(walDir) && !fs.delete(walDir, false)) {
          LOG.warn("Unable to delete log src dir. Ignoring. " + walDir);
        }
      } catch (IOException ioe) {
        FileStatus[] files = fs.listStatus(walDir);
        if (files != null && files.length > 0) {
          LOG.warn("returning success without actually splitting and "
              + "deleting all the WAL files in path " + walDir);
        } else {
          LOG.warn("Unable to delete WAL src dir. Ignoring. " + walDir, ioe);
        }
      }
    }
  }

  private void setupWAL() throws IOException {
    walContainer = new WALContainer(this, this, uncaughtExceptionHandler);
    walContainer.checkServerWALDir(fs, rootDir, serverName);
    walContainer.setupWAL(conf, serverName, null);
    walContainer.startServiceThreads("embedded-db-" + serverName);
  }

  private void startRegionService() {
    this.regionService = new EmbeddedRegionService(conf, rootDir, regionServerServices,
      walContainer, this);
    this.regionService.startAndWait();
  }

  /**
   * Checks whether the directory exists. Creates it otherwise
   * @param dir the directory to create
   * @throws IOException
   */
  private void checkAndCreateDirectory(Path dir) throws IOException {
    if (!fs.exists(dir)) {
      LOG.info("Creating directory: " + dir);
      try {
        fs.mkdirs(dir);
      } catch (FileAlreadyExistsException ex) {
        LOG.warn(ex);
        // Ignore. It should not happen though
      }
    }
  }

  /** Returns the only possible regionInfo for the table */
  private HRegionInfo getRegionInfo(HTableDescriptor htd) {
    return new HRegionInfo(htd.getTableName(), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
      false, DEFAULT_REGION_ID);
  }

  public Connection createConnection() {
    if (!this.isRunning()) {
      throw new RuntimeException("EmbeddedDatabase is not running");
    }
    return new EmbeddedConnection(conf, regionService, retryCounterFactory);
  }

  /**
   * Called at the start to read existing tables from FS
   */
  private synchronized Collection<TableDescriptor> readTablesFromFs() throws IOException {
    Path tempdir = masterFs.getTempDir();

    // clean up temp dir coming from previous executions
    fs.delete(tempdir, true);

    return tableDescriptors.getAllDescriptors().values();
  }

  /** Creates the table (with 1 region) on the FS */
  private synchronized HRegionInfo createTable(HTableDescriptor htd) throws IOException {
    TableName tableName = htd.getTableName();
    if (tableDescriptors.get(tableName) != null) {
      throw new TableExistsException("Table " + htd.getTableName() + " already exists");
    }

    sanityCheckTableDescriptor(htd);

    // We create the table directory and region contents in a tmp dir, and move it to the
    // actual location as a final step.

    Path tempdir = masterFs.getTempDir();

    TableDescriptor underConstruction = new TableDescriptor(htd);
    Path tempTableDir = FSUtils.getTableDir(tempdir, tableName);

    // clean up temp table dir from previous executions
    fs.delete(tempTableDir, true);

    ((FSTableDescriptors)tableDescriptors)
      .createTableDescriptorForTableDirectory(
      tempTableDir, underConstruction, false);
    Path tableDir = FSUtils.getTableDir(masterFs.getRootDir(), tableName);

    // Create Regions
    HRegionInfo regionInfo = getRegionInfo(htd);
    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tempTableDir, regionInfo);
    if (!fs.exists(regionFs.getRegionDir())) {
      LOG.info("Creating directory: " + regionFs.getRegionDir());
      HRegionFileSystem.createRegionOnFileSystem(conf, fs, tempTableDir, regionInfo);
    }

    //  Move Table temp directory to the hbase root location
    if (!fs.rename(tempTableDir, tableDir)) {
      throw new IOException("Unable to move table from temp=" + tempTableDir +
        " to hbase root=" + tableDir);
    }

    // populate descriptors cache to be visible in getAll
    tableDescriptors.get(tableName);

    return regionInfo;
  }

  private synchronized void createAndOpenTable(HTableDescriptor htd) throws IOException {
    HRegionInfo region = createTable(htd);
    regionService.openRegion(htd, region);
  }

  private void openRegions(Collection<TableDescriptor> tables) throws IOException {
    for (TableDescriptor tableDesc : tables) {
      HTableDescriptor htd = tableDesc.getHTableDescriptor();
      HRegionInfo regionInfo = getRegionInfo(htd);
      regionService.openRegion(htd, regionInfo);
    }
  }

  private void sanityCheckTableDescriptor(HTableDescriptor htd) {
    if (!htd.getTableName().isSystemTable()) {
      throw new UnsupportedOperationException("All embedded tables should be in the hbase ns");
    }

    // disable splitting for these tables
    htd.setRegionSplitPolicyClassName(
      "org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy");
  }

  @VisibleForTesting
  class EmbeddedTable implements Table {
    private Region region;
    private RetryCounterFactory retryCounterFactory;

    EmbeddedTable(Region region, RetryCounterFactory retryCounterFactory) {
      this.region = region;
      this.retryCounterFactory = retryCounterFactory;
    }

    @VisibleForTesting
    Region getRegion() {
      return region;
    }

    @Override
    public TableName getName() {
      return region.getTableDesc().getTableName();
    }

    @Override
    public Configuration getConfiguration() {
      return null;
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
      return region.getTableDesc();
    }

    @Override
    public boolean exists(Get get) throws IOException {
      get.setCheckExistenceOnly(true);
      Result r = get(get);
      assert r.getExists() != null;
      return r.getExists();
    }

    @Override
    public boolean[] existsAll(List<Get> gets) throws IOException {
      boolean[] ret = new boolean[gets.size()];
      for (int i = 0; i < ret.length; i++) {
        ret[i] = exists(gets.get(i));
      }
      return ret;
    }

    @Override
    public void batch(List<? extends Row> actions, Object[] results) throws IOException,
        InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions, Object[] results,
        Callback<R> callback) throws IOException, InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public <R> Object[] batchCallback(List<? extends Row> actions, Callback<R> callback)
        throws IOException, InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Result get(Get get) throws IOException {
      return region.get(get);
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
      Result[] results = new Result[gets.size()];
      for (int i = 0; i < gets.size(); i++) {
        results[i] = region.get(gets.get(i));
      }
      return results;
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
      return new ClientSideRegionScanner((HRegion) region, scan);
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
      return getScanner(new Scan().addFamily(family));
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
      return getScanner(new Scan().addColumn(family, qualifier));
    }

    /**
     * Execute the callable with retries. In case the region is flushing and there is no memory
     * or a temporary FS problem, retries work similar to client -> server level retries from
     * hbase-client. Only write operations need retry.
     * @param callable the callable to call
     * @return execution result
     * @throws IOException
     */
    private <T> T executeCallableWithRetries(Callable<T> callable) throws IOException {
      RetryCounter counter = retryCounterFactory.create();
      List<RetriesExhaustedException.ThrowableWithExtraContext> exceptions = null;
      while (counter.shouldRetry() && isRunning()) {
        try {
          return callable.call();
        } catch (DoNotRetryIOException ex) {
          throw ex;
        } catch (Throwable t) { // It maybe RegionTooBusyException
          ExceptionUtil.rethrowIfInterrupt(t);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received " + t + " " + counter);
          }
          if (exceptions == null) {
            exceptions = new ArrayList<RetriesExhaustedException.ThrowableWithExtraContext>();
          }
          RetriesExhaustedException.ThrowableWithExtraContext qt =
              new RetriesExhaustedException.ThrowableWithExtraContext(t,
                  EnvironmentEdgeManager.currentTime(), toString());
          exceptions.add(qt);
          try {
            counter.sleepUntilNextRetry();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException(e.getMessage());
          }
        }
      }
      if (exceptions == null) {
        throw new RetriesExhaustedException("Retries exhausted or embedded database is stopped");
      }
      throw new RetriesExhaustedException(counter.getAttemptTimes(), exceptions);
    }

    @Override
    public void put(final Put put) throws IOException {
      executeCallableWithRetries(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          region.put(put);
          return null;
        }
      });
    }

    /**
     * Performs an atomic multi-put
     * @param puts
     * @throws IOException
     */
    @Override
    public void put(List<Put> puts) throws IOException {
      atomicMultiMutate(puts);
    }

    /**
     * Do an atomic multi row mutation. The code is cp'ed from MultiRowMutationEndpoint
     * @param mutations
     * @throws IOException
     */
    private void atomicMultiMutate(final Collection<? extends Mutation> mutations) throws IOException {
      // set of rows to lock, sorted to avoid deadlocks
      final SortedSet<byte[]> rowsToLock = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

      for (Mutation m : mutations) {
        rowsToLock.add(m.getRow());
      }

      executeCallableWithRetries(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          // call utility method on region
          region.mutateRowsWithLocks(mutations, rowsToLock,
            HConstants.NO_NONCE, HConstants.NO_NONCE);
          return null;
        }
      });
    }

    @Override
    public boolean checkAndPut(final byte[] row, final byte[] family, final byte[] qualifier,
        final byte[] value, final Put put)
        throws IOException {
      return checkAndPut(row, family, qualifier, CompareOp.EQUAL, value, put);
    }

    @Override
    public boolean checkAndPut(final byte[] row, final byte[] family, final byte[] qualifier,
        final CompareOp compareOp, final byte[] value, final Put put) throws IOException {
      return executeCallableWithRetries(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return region.checkAndMutate(row, family, qualifier, compareOp,
            new BinaryComparator(value), put, true);
        }
      });
    }

    @Override
    public void delete(final Delete delete) throws IOException {
      executeCallableWithRetries(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          region.delete(delete);
          return null;
        }
      });
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
      atomicMultiMutate(deletes);
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
        Delete delete) throws IOException {
      return checkAndDelete(row, family, qualifier, CompareOp.EQUAL, value, delete);
    }

    @Override
    public boolean checkAndDelete(final byte[] row, final byte[] family, final byte[] qualifier,
        final CompareOp compareOp, final byte[] value, final Delete delete) throws IOException {
      return executeCallableWithRetries(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return region.checkAndMutate(row, family, qualifier, compareOp,
            new BinaryComparator(value), delete, true);
        }
      });
    }

    @Override
    public void mutateRow(final RowMutations rm) throws IOException {
      executeCallableWithRetries(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          region.mutateRow(rm);
          return null;
        }
      });
    }

    @Override
    public Result append(final Append append) throws IOException {
      return executeCallableWithRetries(new Callable<Result>() {
        @Override
        public Result call() throws Exception {
          return region.append(append, HConstants.NO_NONCE, HConstants.NO_NONCE);
        }
      });
    }

    @Override
    public Result increment(final Increment increment) throws IOException {
      return executeCallableWithRetries(new Callable<Result>() {
        @Override
        public Result call() throws Exception {
          return region.increment(increment, HConstants.NO_NONCE, HConstants.NO_NONCE);
        }
      });
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
        throws IOException {
      return incrementColumnValue(row, family, qualifier, amount, Durability.SYNC_WAL);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
        Durability durability) throws IOException {
      Increment increment = new Increment(row).addColumn(family, qualifier, amount);
      Result result = increment(increment);
      return Long.valueOf(Bytes.toLong(result.getValue(family, qualifier)));
    }

    @Override
    public void close() throws IOException {
      // noop
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
      throw new UnsupportedOperationException();
    }


    @Override
    public <T extends com.google.protobuf.Service, R> Map<byte[], R> coprocessorService(
        Class<T> service, byte[] startKey, byte[] endKey, Call<T, R> callable)
        throws ServiceException, Throwable {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T extends com.google.protobuf.Service, R> void coprocessorService(Class<T> service,
        byte[] startKey, byte[] endKey, Call<T, R> callable, Callback<R> callback)
        throws ServiceException, Throwable {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getWriteBufferSize() {
      return 0;
    }

    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
      // no op
    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(
        MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey,
        R responsePrototype) throws ServiceException, Throwable {
      throw new UnsupportedOperationException();
    }

    @Override
    public <R extends Message> void batchCoprocessorService(MethodDescriptor methodDescriptor,
        Message request, byte[] startKey, byte[] endKey, R responsePrototype, Callback<R> callback)
        throws ServiceException, Throwable {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
        byte[] value, RowMutations mutation) throws IOException {
      return region.checkAndRowMutate(row, family, qualifier, compareOp,
        new BinaryComparator(value), mutation, true);
    }
  }

  @VisibleForTesting
  class EmbeddedConnection implements Connection {
    private final Configuration conf;
    private final EmbeddedRegionService regionService;
    private RetryCounterFactory retryCounterFactory;

    public EmbeddedConnection(Configuration conf, EmbeddedRegionService regionService,
        RetryCounterFactory retryCounterFactory) {
      this.conf = conf;
      this.regionService = regionService;
      this.retryCounterFactory = retryCounterFactory;
    }

    @Override
    public void abort(String why, Throwable e) {
    }

    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public Table getTable(TableName tableName) throws IOException {
      Region region = regionService.tableRegions.get(tableName);
      if (region == null) {
        throw new TableNotFoundException(tableName);
      }
      return new EmbeddedTable(region, retryCounterFactory);
    }

    @Override
    public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
      return getTable(tableName);
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Admin getAdmin() throws IOException {
      return new EmbeddedAdmin(this);
    }

    @Override
    public void close() throws IOException {
      // noop
    }

    @Override
    public boolean isClosed() {
      return false;
    }
  }

  @VisibleForTesting
  class EmbeddedAdmin implements Admin {
    private EmbeddedConnection connection;

    public EmbeddedAdmin(EmbeddedConnection connection) {
      this.connection = connection;
    }

    @Override
    public int getOperationTimeout() {
      return 0;
    }

    @Override
    public void abort(String why, Throwable e) {
    }

    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public Connection getConnection() {
      return connection;
    }

    @Override
    public boolean tableExists(TableName tableName) throws IOException {
      return tableDescriptors.getDescriptor(tableName) != null;
    }

    @Override
    public HTableDescriptor[] listTables() throws IOException {
      Collection<HTableDescriptor> descs = tableDescriptors.getAll().values();
      return descs.toArray(new HTableDescriptor[descs.size()]);
    }

    @Override
    public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public HTableDescriptor[] listTables(String regex) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public HTableDescriptor[] listTables(Pattern pattern, boolean includeSysTables)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public HTableDescriptor[] listTables(String regex, boolean includeSysTables) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public TableName[] listTableNames() throws IOException {
      Collection<HTableDescriptor> descs = tableDescriptors.getAll().values();
      TableName[] tableNames = new TableName[descs.size()];
      int i = 0;
      for (HTableDescriptor htd : descs) {
        tableNames[i++] = htd.getTableName();
      }
      return tableNames;
    }

    @Override
    public TableName[] listTableNames(Pattern pattern) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public TableName[] listTableNames(String regex) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public TableName[] listTableNames(Pattern pattern, boolean includeSysTables) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public TableName[] listTableNames(String regex, boolean includeSysTables) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public HTableDescriptor getTableDescriptor(TableName tableName) throws TableNotFoundException,
        IOException {
      return tableDescriptors.get(tableName);
    }

    @Override
    public void createTable(HTableDescriptor desc) throws IOException {
      EmbeddedDatabase.this.createAndOpenTable(desc);
    }

    @Override
    public void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
        throws IOException {
      throw new UnsupportedOperationException("Only table with 1 region are supported");
    }

    @Override
    public void createTable(HTableDescriptor desc, byte[][] splitKeys) throws IOException {
      throw new UnsupportedOperationException("Only table with 1 region are supported");
    }

    @Override
    public void createTableAsync(HTableDescriptor desc, byte[][] splitKeys) throws IOException {
      throw new UnsupportedOperationException("Only table with 1 region are supported");
    }

    @Override
    public void deleteTable(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public HTableDescriptor[] deleteTables(String regex) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void truncateTable(TableName tableName, boolean preserveSplits) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void enableTable(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void enableTableAsync(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public HTableDescriptor[] enableTables(String regex) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void disableTableAsync(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void disableTable(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public HTableDescriptor[] disableTables(String regex) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isTableEnabled(TableName tableName) throws IOException {
      return tableExists(tableName);
    }

    @Override
    public boolean isTableDisabled(TableName tableName) throws IOException {
      return false;
    }

    @Override
    public boolean isTableAvailable(TableName tableName) throws IOException {
      return tableExists(tableName);
    }

    @Override
    public boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws IOException {
      return tableExists(tableName);
    }

    @Override
    public Pair<Integer, Integer> getAlterStatus(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Pair<Integer, Integer> getAlterStatus(byte[] tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addColumn(TableName tableName, HColumnDescriptor column) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteColumn(TableName tableName, byte[] columnName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void modifyColumn(TableName tableName, HColumnDescriptor descriptor) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void closeRegion(String regionname, String serverName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void closeRegion(byte[] regionname, String serverName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean closeRegionWithEncodedRegionName(String encodedRegionName, String serverName)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void closeRegion(ServerName sn, HRegionInfo hri) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<HRegionInfo> getOnlineRegions(ServerName sn) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void flush(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void flushRegion(byte[] regionName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void compact(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void compactRegion(byte[] regionName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void compact(TableName tableName, byte[] columnFamily) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void compactRegion(byte[] regionName, byte[] columnFamily) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void majorCompact(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void majorCompactRegion(byte[] regionName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void majorCompact(TableName tableName, byte[] columnFamily) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void majorCompactRegion(byte[] regionName, byte[] columnFamily) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void compactRegionServer(ServerName sn, boolean major) throws IOException,
        InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void move(byte[] encodedRegionName, byte[] destServerName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void assign(byte[] regionName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void unassign(byte[] regionName, boolean force) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void offline(byte[] regionName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean setBalancerRunning(boolean on, boolean synchronous) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean balancer() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean enableCatalogJanitor(boolean enable) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int runCatalogScan() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCatalogJanitorEnabled() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void mergeRegions(byte[] encodedNameOfRegionA, byte[] encodedNameOfRegionB,
        boolean forcible) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void split(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void splitRegion(byte[] regionName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void split(TableName tableName, byte[] splitPoint) throws IOException {
      throw new UnsupportedOperationException();    }

    @Override
    public void splitRegion(byte[] regionName, byte[] splitPoint) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void modifyTable(TableName tableName, HTableDescriptor htd) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void stopMaster() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void stopRegionServer(String hostnamePort) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ClusterStatus getClusterStatus() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Configuration getConfiguration() {
      return connection.getConfiguration();
    }

    @Override
    public void createNamespace(NamespaceDescriptor descriptor) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void modifyNamespace(NamespaceDescriptor descriptor) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteNamespace(String name) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public NamespaceDescriptor getNamespaceDescriptor(String name) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public HTableDescriptor[] listTableDescriptorsByNamespace(String name) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public TableName[] listTableNamesByNamespace(String name) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<HRegionInfo> getTableRegions(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      // no op
    }

    @Override
    public HTableDescriptor[] getTableDescriptorsByTableName(List<TableName> tableNames)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public HTableDescriptor[] getTableDescriptors(List<String> names) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void rollWALWriter(ServerName serverName) throws IOException, FailedLogCloseException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String[] getMasterCoprocessors() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompactionState getCompactionState(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompactionState getCompactionStateForRegion(byte[] regionName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLastMajorCompactionTimestamp(TableName tableName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void snapshot(String snapshotName, TableName tableName) throws IOException,
        SnapshotCreationException, IllegalArgumentException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void snapshot(byte[] snapshotName, TableName tableName) throws IOException,
        SnapshotCreationException, IllegalArgumentException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void snapshot(String snapshotName, TableName tableName, Type type) throws IOException,
        SnapshotCreationException, IllegalArgumentException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void snapshot(SnapshotDescription snapshot) throws IOException,
        SnapshotCreationException, IllegalArgumentException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SnapshotResponse takeSnapshotAsync(SnapshotDescription snapshot) throws IOException,
        SnapshotCreationException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSnapshotFinished(SnapshotDescription snapshot) throws IOException,
        HBaseSnapshotException, UnknownSnapshotException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void restoreSnapshot(byte[] snapshotName) throws IOException, RestoreSnapshotException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void restoreSnapshot(String snapshotName) throws IOException, RestoreSnapshotException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void restoreSnapshot(byte[] snapshotName, boolean takeFailSafeSnapshot)
        throws IOException, RestoreSnapshotException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot)
        throws IOException, RestoreSnapshotException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void cloneSnapshot(byte[] snapshotName, TableName tableName) throws IOException,
        TableExistsException, RestoreSnapshotException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void cloneSnapshot(String snapshotName, TableName tableName) throws IOException,
        TableExistsException, RestoreSnapshotException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void execProcedure(String signature, String instance, Map<String, String> props)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[]
        execProcedureWithRet(String signature, String instance, Map<String, String> props)
            throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean
        isProcedureFinished(String signature, String instance, Map<String, String> props)
            throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<SnapshotDescription> listSnapshots() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<SnapshotDescription> listSnapshots(String regex) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteSnapshot(byte[] snapshotName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteSnapshot(String snapshotName) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteSnapshots(String regex) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteSnapshots(Pattern pattern) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setQuota(QuotaSettings quota) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public QuotaRetriever getQuotaRetriever(QuotaFilter filter) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public CoprocessorRpcChannel coprocessorService() {
      throw new UnsupportedOperationException();
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(ServerName sn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateConfiguration(ServerName server) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateConfiguration() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getMasterInfoPort() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBalancerEnabled() throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  @VisibleForTesting
  static class EmbeddedRegionService extends AbstractService implements Service, OnlineRegions {

    private static final Log LOG = LogFactory.getLog(EmbeddedRegionService.class);

    // Assumes one region per table for now. This is our meta table
    private final Map<TableName, Region> tableRegions = new ConcurrentHashMap<>();

    private final Map<String, Region> onlineRegions = new ConcurrentHashMap<>();

    private final WALServices walServices;
    private final Configuration conf;
    private final Path rootDir;
    private final Abortable abortable;
    private final RegionServerServices regionServerServices;

    public EmbeddedRegionService(Configuration conf, Path rootDir,
        RegionServerServices regionServerServices, WALServices walServices,
        Abortable abortable) {
      this.conf = conf;
      this.rootDir = rootDir;
      this.regionServerServices = regionServerServices;
      this.walServices = walServices;
      this.abortable = abortable;
    }

    @Override
    protected void doStart() {
      notifyStarted();
    }

    @Override
    protected void doStop() {
      // sync for now
      closeAllRegions(false);

      notifyStopped();
    }

    // we use synchronized as a coarse lock for region states.
    private synchronized void closeAllRegions(boolean abort) {
      for (Region region : onlineRegions.values()) {
        closeRegionIgnoreErrors(region, abort);
      }
    }

    /**
     * Try to close the region, logs a warning on failure but continues.
     * @param region Region to close
     */
    private synchronized void closeRegionIgnoreErrors(Region region, final boolean abort) {
      try {
        if (removeFromOnlineRegions(region, null)) {
          closeRegion(region, abort);
        }
      } catch (IOException e) {
        LOG.warn("Failed to close " + region.getRegionInfo().getRegionNameAsString() +
            " - ignoring and continuing", e);
      }
    }

    private void closeRegion(Region region, final boolean abort) throws IOException {
      try {
        if (((HRegion)region).close(abort) == null) {
          // This region got closed.  Most likely due to a split.
          // The split message will clean up the master state.
          LOG.warn("Can't close region: was already closed during close(): " +
            region.getRegionInfo().getRegionNameAsString());
          return;
        }
      } catch (IOException ioe) {
        // An IOException here indicates that we couldn't successfully flush the
        // memstore before closing. So, we need to abort the server and allow
        // the master to split our logs in order to recover the data.
        if (abortable != null) {
          abortable.abort("Unrecoverable exception while closing region " +
              region.getRegionInfo().getRegionNameAsString() + ", still finishing close", ioe);
        }
        throw ioe;
      }
    }

    public synchronized void openRegion(HTableDescriptor htd, HRegionInfo regionInfo)
        throws IOException {
      // TODO: WAL file cleaning?

      if (onlineRegions.containsKey(regionInfo.getEncodedName())) {
        throw new IOException("Cannot open region " + regionInfo.getRegionNameAsString() +
          "  since it is already opened");
      }

      // TODO: periodic flusher, global flusher etc cannot see these regions
      HRegion region = HRegion.openHRegion(rootDir, regionInfo, htd,
        walServices.getWAL(regionInfo), conf, regionServerServices, null);

      /*
       * We are putting the region into a separate onlineRegions than the one in RegionServer. This
       * prevents the regions being visible from clients as well as hbck, ui, meta and rest of
       * master / regionserver.
       */
      addToOnlineRegions(region);
    }

    @Override
    public synchronized void addToOnlineRegions(Region r) {
      onlineRegions.put(r.getRegionInfo().getEncodedName(), r);
      tableRegions.put(r.getTableDesc().getTableName(), r);
    }

    @Override
    public synchronized boolean removeFromOnlineRegions(Region r, ServerName destination) {
      boolean removed = onlineRegions.remove(r.getRegionInfo().getEncodedName()) != null;
      if (removed) {
        tableRegions.remove(r.getTableDesc().getTableName());
      }
      return removed;
    }

    @Override
    public Region getFromOnlineRegions(String encodedRegionName) {
      return onlineRegions.get(encodedRegionName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized List<Region> getOnlineRegions(TableName tableName) throws IOException {
      Region region = tableRegions.get(tableName);
      return region == null ? Collections.EMPTY_LIST : Collections.singletonList(region);
    }
  }

  @Override
  public ServerName getServerName() {
    return serverName;
  }

  @Override
  public void abort(String why, Throwable e) {
    if (this.abortable != null) {
      abortable.abort(why, e);
    }
  }

  @Override
  public boolean isAborted() {
    if (this.abortable != null) {
      return this.abortable.isAborted();
    }
    return false;
  }

  @Override
  public void stop(String why) {
    stop();
  }

  @Override
  public boolean isStopped() {
    // Guava service and our ad-hoc stoppable/Abortable does not play well with each other
    // we need this since in AbstractService.doStop() locks the state() causing a deadlock if we
    // wait for some other thread which depends on isStopped() to return true.
    return !running.get();
  }

  @Override
  public boolean isStopping() {
    return state() == State.STOPPING;
  }

  // Below are for forwarding to EmbeddedRegionService
  @Override
  public void addToOnlineRegions(Region r) {
    regionService.addToOnlineRegions(r);
  }

  @Override
  public boolean removeFromOnlineRegions(Region r, ServerName destination) {
    return regionService.removeFromOnlineRegions(r, destination);
  }

  @Override
  public Region getFromOnlineRegions(String encodedRegionName) {
    return regionService.getFromOnlineRegions(encodedRegionName);
  }

  @Override
  public List<Region> getOnlineRegions(TableName tableName) throws IOException {
    return regionService.getOnlineRegions(tableName);
  }

  // Below are for implementing Server interface. We should not need these, but they are coming
  // from Server interface unfortunately.
  @Override
  public ChoreService getChoreService() {
    return null;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return null;
  }

  @Override
  public ClusterConnection getConnection() {
    return null;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return null;
  }

  @Override
  public MetaTableLocator getMetaTableLocator() {
    return null;
  }
}
