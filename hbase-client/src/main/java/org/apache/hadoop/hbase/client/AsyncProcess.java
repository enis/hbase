/*
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

package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.htrace.Trace;
import com.google.common.annotations.VisibleForTesting;

/**
 * This class  allows a continuous flow of requests. It's written to be compatible with a
 * synchronous caller such as HTable.
 * <p>
 * The caller sends a buffer of operation, by calling submit. This class extract from this list
 * the operations it can send, i.e. the operations that are on region that are not considered
 * as busy. The process is asynchronous, i.e. it returns immediately when if has finished to
 * iterate on the list. If, and only if, the maximum number of current task is reached, the call
 * to submit will block. Alternatively, the caller can call submitAll, in which case all the
 * operations will be sent. Each call to submit returns a future-like object that can be used
 * to track operation progress.
 * </p>
 * <p>
 * The class manages internally the retries.
 * </p>
 * <p>
 * The class can be constructed in regular mode, or "global error" mode. In global error mode,
 * AP tracks errors across all calls (each "future" also has global view of all errors). That
 * mode is necessary for backward compat with HTable behavior, where multiple submissions are
 * made and the errors can propagate using any put/flush call, from previous calls.
 * In "regular" mode, the errors are tracked inside the Future object that is returned.
 * The results are always tracked inside the Future object and can be retrieved when the call
 * has finished. Partial results can also be retrieved if some part of multi-request failed.
 * </p>
 * <p>
 * This class is thread safe in regular mode; in global error code, submitting operations and
 * retrieving errors from different threads may be not thread safe.
 * Internally, the class is thread safe enough to manage simultaneously new submission and results
 * arising from older operations.
 * </p>
 * <p>
 * Internally, this class works with {@link Row}, this mean it could be theoretically used for
 * gets as well.
 * </p>
 */
class AsyncProcess {
  protected static final Log LOG = LogFactory.getLog(AsyncProcess.class);
  protected static final AtomicLong COUNTER = new AtomicLong();

  public static final String PRIMARY_CALL_TIMEOUT_KEY = "hbase.client.primaryCallTimeout.multiget";

  /**
   * The context used to wait for results from one submit call.
   * 1) If AsyncProcess is set to track errors globally, and not per call (for HTable puts),
   *    then errors and failed operations in this object will reflect global errors.
   * 2) If submit call is made with needResults false, results will not be saved.
   *  */
  public static interface AsyncRequestFuture {
    public boolean hasError();
    public RetriesExhaustedWithDetailsException getErrors();
    public List<? extends Row> getFailedOperations();
    public Object[] getResults() throws InterruptedIOException;
    /** Wait until all tasks are executed, successfully or not. */
    public void waitUntilDone() throws InterruptedIOException;
  }

  /** Return value from a submit that didn't contain any requests. */
  private static final AsyncRequestFuture NO_REQS_RESULT = new AsyncRequestFuture() {
    public final Object[] result = new Object[0];
    @Override
    public boolean hasError() { return false; }
    @Override
    public RetriesExhaustedWithDetailsException getErrors() { return null; }
    @Override
    public List<? extends Row> getFailedOperations() { return null; }
    @Override
    public Object[] getResults() { return result; }
    @Override
    public void waitUntilDone() throws InterruptedIOException {}
  };

  /** Sync point for calls to multiple replicas for the same user request (Get).
   * Created and put in the results array (we assume replica calls require results) when
   * the replica calls are launched. See results for details of this process.
   * POJO, all fields are public. To modify them, the object itself is locked. */
  private static class ReplicaResultState {
    public ReplicaResultState(int callCount) {
      this.callCount = callCount;
    }

    /** Number of calls outstanding, or 0 if a call succeeded (even with others outstanding). */
    int callCount;
    /** Call that succeeds sets the count to 0 and sets this to result. Call that fails but
     * is not last, adds error to list. If all calls fail the last one sets this to list. */
    Object result = null;
    /** Errors for which it is not decided whether we will report them to user. If one of the
     * calls succeeds, we will discard the errors that may have happened in the other calls. */
    BatchErrors replicaErrors = null;
  }


  // TODO: many of the fields should be made private
  protected final long id;

  protected final ClusterConnection hConnection;
  protected final RpcRetryingCallerFactory rpcCallerFactory;
  protected final RpcControllerFactory rpcFactory;
  protected final BatchErrors globalErrors;
  protected final ExecutorService pool;

  protected final AtomicLong tasksInProgress = new AtomicLong(0);
  protected final ConcurrentMap<byte[], AtomicInteger> taskCounterPerRegion =
      new ConcurrentSkipListMap<byte[], AtomicInteger>(Bytes.BYTES_COMPARATOR);
  protected final ConcurrentMap<ServerName, AtomicInteger> taskCounterPerServer =
      new ConcurrentHashMap<ServerName, AtomicInteger>();

  // Start configuration settings.
  private final int startLogErrorsCnt;

  /**
   * The number of tasks simultaneously executed on the cluster.
   */
  protected final int maxTotalConcurrentTasks;

  /**
   * The number of tasks we run in parallel on a single region.
   * With 1 (the default) , we ensure that the ordering of the queries is respected: we don't start
   * a set of operations on a region before the previous one is done. As well, this limits
   * the pressure we put on the region server.
   */
  protected final int maxConcurrentTasksPerRegion;

  /**
   * The number of task simultaneously executed on a single region server.
   */
  protected final int maxConcurrentTasksPerServer;
  protected final long pause;
  protected int numTries;
  protected int serverTrackerTimeout;
  protected int timeout;
  protected long primaryCallTimeoutMicroseconds;
  // End configuration settings.

  protected static class BatchErrors {
    private final List<Throwable> throwables = new ArrayList<Throwable>();
    private final List<Row> actions = new ArrayList<Row>();
    private final List<String> addresses = new ArrayList<String>();

    public synchronized void add(Throwable ex, Row row, ServerName serverName) {
      if (row == null){
        throw new IllegalArgumentException("row cannot be null. location=" + serverName);
      }

      throwables.add(ex);
      actions.add(row);
      addresses.add(serverName != null ? serverName.toString() : "null");
    }

    public boolean hasErrors() {
      return !throwables.isEmpty();
    }

    private synchronized RetriesExhaustedWithDetailsException makeException() {
      return new RetriesExhaustedWithDetailsException(
          new ArrayList<Throwable>(throwables),
          new ArrayList<Row>(actions), new ArrayList<String>(addresses));
    }

    public synchronized void clear() {
      throwables.clear();
      actions.clear();
      addresses.clear();
    }

    public synchronized void merge(BatchErrors other) {
      throwables.addAll(other.throwables);
      actions.addAll(other.actions);
      addresses.addAll(other.addresses);
    }
  }

  public AsyncProcess(ClusterConnection hc, Configuration conf, ExecutorService pool,
      RpcRetryingCallerFactory rpcCaller, boolean useGlobalErrors, RpcControllerFactory rpcFactory) {
    if (hc == null) {
      throw new IllegalArgumentException("HConnection cannot be null.");
    }

    this.hConnection = hc;
    this.pool = pool;
    this.globalErrors = useGlobalErrors ? new BatchErrors() : null;

    this.id = COUNTER.incrementAndGet();

    this.pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.numTries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.timeout = conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
        HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    this.primaryCallTimeoutMicroseconds = conf.getInt(PRIMARY_CALL_TIMEOUT_KEY, 10000);

    this.maxTotalConcurrentTasks = conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
      HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS);
    this.maxConcurrentTasksPerServer = conf.getInt(HConstants.HBASE_CLIENT_MAX_PERSERVER_TASKS,
          HConstants.DEFAULT_HBASE_CLIENT_MAX_PERSERVER_TASKS);
    this.maxConcurrentTasksPerRegion = conf.getInt(HConstants.HBASE_CLIENT_MAX_PERREGION_TASKS,
          HConstants.DEFAULT_HBASE_CLIENT_MAX_PERREGION_TASKS);

    // A few failure is fine: region moved, then is not opened, then is overloaded. We try
    //  to have an acceptable heuristic for the number of errors we don't log.
    //  9 was chosen because we wait for 1s at this stage.
    this.startLogErrorsCnt = conf.getInt("hbase.client.start.log.errors.counter", 9);

    if (this.maxTotalConcurrentTasks <= 0) {
      throw new IllegalArgumentException("maxTotalConcurrentTasks=" + maxTotalConcurrentTasks);
    }
    if (this.maxConcurrentTasksPerServer <= 0) {
      throw new IllegalArgumentException("maxConcurrentTasksPerServer=" +
          maxConcurrentTasksPerServer);
    }
    if (this.maxConcurrentTasksPerRegion <= 0) {
      throw new IllegalArgumentException("maxConcurrentTasksPerRegion=" +
          maxConcurrentTasksPerRegion);
    }

    // Server tracker allows us to do faster, and yet useful (hopefully), retries.
    // However, if we are too useful, we might fail very quickly due to retry count limit.
    // To avoid this, we are going to cheat for now (see HBASE-7659), and calculate maximum
    // retry time if normal retries were used. Then we will retry until this time runs out.
    // If we keep hitting one server, the net effect will be the incremental backoff, and
    // essentially the same number of retries as planned. If we have to do faster retries,
    // we will do more retries in aggregate, but the user will be none the wiser.
    this.serverTrackerTimeout = 0;
    for (int i = 0; i < this.numTries; ++i) {
      serverTrackerTimeout += ConnectionUtils.getPauseTime(this.pause, i);
    }

    this.rpcCallerFactory = rpcCaller;
    this.rpcFactory = rpcFactory;
  }

  private ExecutorService getPool(ExecutorService pool) {
    if (pool != null) return pool;
    if (this.pool != null) return this.pool;
    throw new RuntimeException("Neither AsyncProcess nor request have ExecutorService");
  }

  /**
   * See {@link #submit(ExecutorService, TableName, List, boolean, org.apache.hadoop.hbase.client.coprocessor.Batch.Callback, boolean)}.
   * Uses default ExecutorService for this AP (must have been created with one).
   */
  public <CResult> AsyncRequestFuture submit(TableName tableName, List<? extends Row> rows,
      boolean atLeastOne, Batch.Callback<CResult> callback, boolean needResults) throws InterruptedIOException {
    return submit(null, tableName, rows, atLeastOne, callback, needResults);
  }

  /**
   * Extract from the rows list what we can submit. The rows we can not submit are kept in the
   * list. Does not send requests to replicas (not currently used for anything other
   * than streaming puts anyway).
   *
   * @param pool ExecutorService to use.
   * @param tableName The table for which this request is needed.
   * @param callback Batch callback. Only called on success (94 behavior).
   * @param needResults Whether results are needed, or can be discarded.
   * @param rows - the submitted row. Modified by the method: we remove the rows we took.
   * @param atLeastOne true if we should submit at least a subset.
   */
  public <CResult> AsyncRequestFuture submit(ExecutorService pool, TableName tableName,
      List<? extends Row> rows, boolean atLeastOne, Batch.Callback<CResult> callback,
      boolean needResults) throws InterruptedIOException {
    if (rows.isEmpty()) {
      return NO_REQS_RESULT;
    }

    Map<ServerName, MultiAction<Row>> actionsByServer =
        new HashMap<ServerName, MultiAction<Row>>();
    List<Action<Row>> retainedActions = new ArrayList<Action<Row>>(rows.size());

    NonceGenerator ng = this.hConnection.getNonceGenerator();
    long nonceGroup = ng.getNonceGroup(); // Currently, nonce group is per entire client.

    // Location errors that happen before we decide what requests to take.
    List<Exception> locationErrors = null;
    List<Integer> locationErrorRows = null;
    do {
      // Wait until there is at least one slot for a new task.
      waitForMaximumCurrentTasks(maxTotalConcurrentTasks - 1);

      // Remember the previous decisions about regions or region servers we put in the
      //  final multi.
      Map<Long, Boolean> regionIncluded = new HashMap<Long, Boolean>();
      Map<ServerName, Boolean> serverIncluded = new HashMap<ServerName, Boolean>();

      int posInList = -1;
      Iterator<? extends Row> it = rows.iterator();
      while (it.hasNext()) {
        Row r = it.next();
        HRegionLocation loc;
        try {
          loc = findDestLocation(tableName, r, true).getDefaultRegionLocation();
        } catch (IOException ex) {
          locationErrors = new ArrayList<Exception>();
          locationErrorRows = new ArrayList<Integer>();
          LOG.error("Failed to get region location ", ex);
          // This action failed before creating ars. Add it to retained but do not add to submit list.
          // We will then add it to ars in an already-failed state.
          retainedActions.add(new Action<Row>(r, ++posInList));
          locationErrors.add(ex);
          locationErrorRows.add(posInList);
          it.remove();
          break; // Backward compat: we stop considering actions on location error.
        }

        if (canTakeOperation(loc, regionIncluded, serverIncluded)) {
          Action<Row> action = new Action<Row>(r, ++posInList);
          setNonce(ng, r, action);
          retainedActions.add(action);
          // TODO: replica-get is not supported on this path
          byte[] regionName = loc.getRegionInfo().getRegionName();
          addAction(loc.getServerName(), regionName, action, actionsByServer, nonceGroup);
          it.remove();
        }
      }
    } while (retainedActions.isEmpty() && atLeastOne && (locationErrors == null));

    if (retainedActions.isEmpty()) return NO_REQS_RESULT;

    AsyncRequestFutureImpl<CResult> ars = createAsyncRequestFuture(
        tableName, retainedActions, nonceGroup, pool, callback, null, needResults);
    // Add location errors if any
    if (locationErrors != null) {
      for (int i = 0; i < locationErrors.size(); ++i) {
        int originalIndex = locationErrorRows.get(i);
        Row row = retainedActions.get(originalIndex).getAction();
        ars.manageError(originalIndex, row, false, locationErrors.get(i), null);
      }
    }
    ars.sendMultiAction(actionsByServer, 1, null);
    return ars;
  }

  /**
   * Helper that is used when grouping the actions per region server.
   *
   * @param loc - the destination. Must not be null.
   * @param action - the action to add to the multiaction
   * @param actionsByServer the multiaction per server
   * @param nonceGroup Nonce group.
   */
  private void addAction(ServerName server, byte[] regionName, Action<Row> action,
      Map<ServerName, MultiAction<Row>> actionsByServer, long nonceGroup) {
    MultiAction<Row> multiAction = actionsByServer.get(server);
    if (multiAction == null) {
      multiAction = new MultiAction<Row>();
      actionsByServer.put(server, multiAction);
    }
    if (action.hasNonce() && !multiAction.hasNonceGroup()) {
      multiAction.setNonceGroup(nonceGroup);
    }

    multiAction.add(regionName, action);
  }

  /**
   * Find the destination.
   * @param tableName the requisite table.
   * @param row the row
   * @return the destination.
   */
  private RegionLocations findDestLocation(
      TableName tableName, Row row, boolean checkPrimary) throws IOException {
    if (row == null) throw new IllegalArgumentException("#" + id + ", row cannot be null");
    RegionLocations loc = hConnection.locateRegionAll(tableName, row.getRow());
    if (loc == null
        || (checkPrimary && (loc.isEmpty() || loc.getDefaultRegionLocation() == null))) {
      throw new IOException("#" + id + ", no location found, aborting submit for" +
          " tableName=" + tableName + " rowkey=" + Arrays.toString(row.getRow()));
    }
    return loc;
  }

  /**
   * Check if we should send new operations to this region or region server.
   * We're taking into account the past decision; if we have already accepted
   * operation on a given region, we accept all operations for this region.
   *
   * @param loc; the region and the server name we want to use.
   * @return true if this region is considered as busy.
   */
  protected boolean canTakeOperation(HRegionLocation loc,
                                     Map<Long, Boolean> regionsIncluded,
                                     Map<ServerName, Boolean> serversIncluded) {
    long regionId = loc.getRegionInfo().getRegionId();
    Boolean regionPrevious = regionsIncluded.get(regionId);

    if (regionPrevious != null) {
      // We already know what to do with this region.
      return regionPrevious;
    }

    Boolean serverPrevious = serversIncluded.get(loc.getServerName());
    if (Boolean.FALSE.equals(serverPrevious)) {
      // It's a new region, on a region server that we have already excluded.
      regionsIncluded.put(regionId, Boolean.FALSE);
      return false;
    }

    AtomicInteger regionCnt = taskCounterPerRegion.get(loc.getRegionInfo().getRegionName());
    if (regionCnt != null && regionCnt.get() >= maxConcurrentTasksPerRegion) {
      // Too many tasks on this region already.
      regionsIncluded.put(regionId, Boolean.FALSE);
      return false;
    }

    if (serverPrevious == null) {
      // The region is ok, but we need to decide for this region server.
      int newServers = 0; // number of servers we're going to contact so far
      for (Map.Entry<ServerName, Boolean> kv : serversIncluded.entrySet()) {
        if (kv.getValue()) {
          newServers++;
        }
      }

      // Do we have too many total tasks already?
      boolean ok = (newServers + tasksInProgress.get()) < maxTotalConcurrentTasks;

      if (ok) {
        // If the total is fine, is it ok for this individual server?
        AtomicInteger serverCnt = taskCounterPerServer.get(loc.getServerName());
        ok = (serverCnt == null || serverCnt.get() < maxConcurrentTasksPerServer);
      }

      if (!ok) {
        regionsIncluded.put(regionId, Boolean.FALSE);
        serversIncluded.put(loc.getServerName(), Boolean.FALSE);
        return false;
      }

      serversIncluded.put(loc.getServerName(), Boolean.TRUE);
    } else {
      assert serverPrevious.equals(Boolean.TRUE);
    }

    regionsIncluded.put(regionId, Boolean.TRUE);

    return true;
  }

  /**
   * See {@link #submitAll(ExecutorService, TableName, List, org.apache.hadoop.hbase.client.coprocessor.Batch.Callback, Object[])}.
   * Uses default ExecutorService for this AP (must have been created with one).
   */
  public <CResult> AsyncRequestFuture submitAll(TableName tableName,
      List<? extends Row> rows, Batch.Callback<CResult> callback, Object[] results) {
    return submitAll(null, tableName, rows, callback, results);
  }

  /**
   * Submit immediately the list of rows, whatever the server status. Kept for backward
   * compatibility: it allows to be used with the batch interface that return an array of objects.
   *
   * @param pool ExecutorService to use.
   * @param tableName name of the table for which the submission is made.
   * @param rows the list of rows.
   * @param callback the callback.
   * @param results Optional array to return the results thru; backward compat.
   */
  public <CResult> AsyncRequestFuture submitAll(ExecutorService pool, TableName tableName,
      List<? extends Row> rows, Batch.Callback<CResult> callback, Object[] results) {
    List<Action<Row>> actions = new ArrayList<Action<Row>>(rows.size());

    // The position will be used by the processBatch to match the object array returned.
    int posInList = -1;
    NonceGenerator ng = this.hConnection.getNonceGenerator();
    for (Row r : rows) {
      posInList++;
      if (r instanceof Put) {
        Put put = (Put) r;
        if (put.isEmpty()) {
          throw new IllegalArgumentException("No columns to insert for #" + (posInList+1)+ " item");
        }
      }
      Action<Row> action = new Action<Row>(r, posInList);
      setNonce(ng, r, action);
      actions.add(action);
    }
    AsyncRequestFutureImpl<CResult> ars = createAsyncRequestFuture(
        tableName, actions, ng.getNonceGroup(), getPool(pool), callback, results, results != null);
    ars.groupAndSendMultiAction(actions, 1);
    return ars;
  }

  private void setNonce(NonceGenerator ng, Row r, Action<Row> action) {
    if (!(r instanceof Append) && !(r instanceof Increment)) return;
    action.setNonce(ng.newNonce()); // Action handles NO_NONCE, so it's ok if ng is disabled.
  }

  /**
   * The context, and return value, for a single submit/submitAll call.
   * Note on how this class (one AP submit) works. Initially, all requests are split into groups
   * by server; request is sent to each server in parallel; the RPC calls are not async so a
   * thread per server is used. Every time some actions fail, regions/locations might have
   * changed, so we re-group them by server and region again and send these groups in parallel
   * too. The result, in case of retries, is a "tree" of threads, with parent exiting after
   * scheduling children. This is why lots of code doesn't require any synchronization.
   */
  protected class AsyncRequestFutureImpl<CResult> implements AsyncRequestFuture {

    /**
     * Runnable (that can be submitted to thread pool) that waits for when it's time
     * to issue replica calls, finds region replicas, groups the requests by replica and
     * issues the calls (on separate threads, via sendMultiAction).
     * This is done on a separate thread because we don't want to wait on user thread for
     * our asynchronous call, and usually we have to wait before making replica calls.
     */
    private final class ReplicaCallIssuingRunnable implements Runnable {
      private final long startTime;
      private final List<Action<Row>> initialActions;

      public ReplicaCallIssuingRunnable(List<Action<Row>> initialActions, long startTime) {
        this.initialActions = initialActions;
        this.startTime = startTime;
      }

      @Override
      public void run() {
        boolean done = false;
        if (primaryCallTimeoutMicroseconds > 0) {
          try {
            done = waitUntilDone(startTime * 1000L + primaryCallTimeoutMicroseconds);
          } catch (InterruptedException ex) {
            LOG.error("Replica thread was interrupted - no replica calls: " + ex.getMessage());
            return;
          }
        }
        if (done) return; // Done within primary timeout
        Map<ServerName, MultiAction<Row>> actionsByServer =
            new HashMap<ServerName, MultiAction<Row>>();
        if (replicaGetIndices == null) {
          for (int i = 0; i < results.length; ++i) {
            addReplicaActions(i, actionsByServer);
          }
        } else {
          for (int i = 0; i < replicaGetIndices.length; ++i) {
            addReplicaActions(replicaGetIndices[i], actionsByServer);
          }
        }
        if (actionsByServer.isEmpty()) return; // Nothing to do - done or no replicas found.
        sendMultiAction(actionsByServer, 1, null);
      }

      /**
       * Add replica actions to action map by server.
       * @param index Index of the original action.
       * @param actionsByServer The map by server to add it to.
       */
      private void addReplicaActions(
          int index, Map<ServerName, MultiAction<Row>> actionsByServer) {
        if (results[index] != null) return; // opportunistic. Never goes from non-null to null.
        Action<Row> action = initialActions.get(index);
        RegionLocations loc = null;
        try {
          // For perf, we assume that this location coming from cache, since we just got location
          // from meta for the primary call. If it turns out to not be the case, we'd need local
          // cache since we want to keep as little time as possible before replica call.
          loc = findDestLocation(tableName, action.getAction(), false);
        } catch (IOException ex) {
          manageError(action.getOriginalIndex(), action.getAction(), false, ex, null);
          LOG.error("Cannot get location - no replica calls for some actions", ex);
          return;
        }
        HRegionLocation[] locs = loc.getRegionLocations();
        int replicaCount = 0;
        for (int i = 1; i < locs.length; ++i) {
          replicaCount += (locs[i] != null) ? 1 : 0;
        }
        if (replicaCount == 0) {
          LOG.warn("No replicas found for " + action.getAction());
          return;
        }
        synchronized (replicaResultLock) {
          // Don't run replica calls if the original has finished. We could do it e.g. if
          // original has already failed before first replica call (unlikely given retries),
          // but that would require additional synchronization w.r.t. returning to caller.
          if (results[index] != null) return;
          // We set the number of calls here. After that any path must call setResult/setError.
          results[index] = new ReplicaResultState(replicaCount + 1);
        }
        for (int i = 1; i < locs.length; ++i) {
          if (locs[i] == null) continue;
          addAction(locs[i].getServerName(), locs[i].getRegionInfo().getRegionName(),
              new Action<Row>(action, i), actionsByServer, nonceGroup);
        }
      }
    }

    /**
     * Runnable (that can be submitted to thread pool) that submits MultiAction to a
     * single server. The server call is synchronous, therefore we do it on a thread pool.
     */
    private final class SingleServerRequestRunnable implements Runnable {
      private final MultiAction<Row> multiAction;
      private final int numAttempt;
      private final ServerName server;

      private SingleServerRequestRunnable(
          MultiAction<Row> multiAction, int numAttempt, ServerName server) {
        this.multiAction = multiAction;
        this.numAttempt = numAttempt;
        this.server = server;
      }

      @Override
      public void run() {
        MultiResponse res;
        try {
          MultiServerCallable<Row> callable = createCallable(server, tableName, multiAction);
          try {
            res = createCaller(callable).callWithoutRetries(callable, timeout);
          } catch (IOException e) {
            // The service itself failed . It may be an error coming from the communication
            //   layer, but, as well, a functional error raised by the server.
            receiveGlobalFailure(multiAction, server, numAttempt, e);
            return;
          } catch (Throwable t) {
            // This should not happen. Let's log & retry anyway.
            LOG.error("#" + id + ", Caught throwable while calling. This is unexpected." +
                " Retrying. Server is " + server + ", tableName=" + tableName, t);
            receiveGlobalFailure(multiAction, server, numAttempt, t);
            return;
          }

          // Normal case: we received an answer from the server, and it's not an exception.
          receiveMultiAction(multiAction, server, res, numAttempt);
        } catch (Throwable t) {
              // Something really bad happened. We are on the send thread that will now die.
              LOG.error("Internal AsyncProcess #" + id + " error for "
                  + tableName + " processing for " + server, t);
              throw new RuntimeException(t);
        } finally {
          decTaskCounters(multiAction.getRegions(), server);
        }
      }
    }

    private final Batch.Callback<CResult> callback;
    private final BatchErrors errors;
    private final ConnectionManager.ServerErrorTracker errorsByServer;
    private final ExecutorService pool;


    private final TableName tableName;
    private final AtomicLong actionsInProgress = new AtomicLong(-1);
    /** The lock controls access to results. It is only held when populating results where
     * there might be several callers (eventual consistency gets). For other requests,
     * there's one unique call going on per result index. */
    private final Object replicaResultLock = new Object();
    /** Result array.  Null if results are not needed. Otherwise, each index corresponds to
     * the action index in initial actions submitted. For most request types, has null-s for
     * requests that are not done, and result/exception for those that are done.
     * For eventual-consistency gets, initially the same applies; at some point, replica calls
     * might be started, and ReplicaResultState is put at the corresponding indices. The
     * returning calls check the type to detect when this is the case. After all calls are done,
     * ReplicaResultState-s are replaced with results for the user. */
    private final Object[] results;
    /** Indices of replica gets in results. If null, all or no actions are replica-gets. */
    private final int[] replicaGetIndices;
    private final boolean hasAnyReplicaGets;
    private final long nonceGroup;

    public AsyncRequestFutureImpl(TableName tableName, List<Action<Row>> actions, long nonceGroup,
        ExecutorService pool, boolean needResults, Object[] results,
        Batch.Callback<CResult> callback) {
      this.pool = pool;
      this.callback = callback;
      this.nonceGroup = nonceGroup;
      this.tableName = tableName;
      this.actionsInProgress.set(actions.size());
      if (results != null) {
        assert needResults;
        if (results.length != actions.size()) throw new AssertionError("results.length");
        this.results = results;
        for (int i = 0; i != this.results.length; ++i) {
          results[i] = null;
        }
      } else {
        this.results = needResults ? new Object[actions.size()] : null;
      }
      List<Integer> replicaGetIndices = null;
      boolean hasAnyReplicaGets = false;
      if (needResults) {
        // Check to see if any requests might require replica calls.
        // We expect that many requests will consist of all or no multi-replica gets; in such
        // cases we would just use a boolean (hasAnyReplicaGets). If there's a mix, we will
        // store the list of action indexes for which replica gets are possible, and set
        // hasAnyReplicaGets to true.
        boolean hasAnyNonReplicaReqs = false;
        int posInList = 0;
        for (Action<Row> action : actions) {
          boolean isReplicaGet = isReplicaGet(action.getAction());
          if (isReplicaGet) {
            hasAnyReplicaGets = true;
            if (hasAnyNonReplicaReqs) { // Mixed case
              if (replicaGetIndices == null) {
                replicaGetIndices = new ArrayList<Integer>(actions.size() - 1);
              }
              replicaGetIndices.add(posInList);
            }
          } else if (!hasAnyNonReplicaReqs) {
            // The first non-multi-replica request in the action list.
            hasAnyNonReplicaReqs = true;
            if (posInList > 0) {
              // Add all the previous requests to the index lists. We know they are all
              // replica-gets because this is the first non-multi-replica request in the list.
              replicaGetIndices = new ArrayList<Integer>(actions.size() - 1);
              for (int i = 0; i < posInList; ++i) {
                replicaGetIndices.add(i);
              }
            }
          }
          ++posInList;
        }
      }
      this.hasAnyReplicaGets = hasAnyReplicaGets;
      if (replicaGetIndices != null) {
        this.replicaGetIndices = new int[replicaGetIndices.size()];
        int i = 0;
        for (Integer el : replicaGetIndices) {
          this.replicaGetIndices[i++] = el;
        }
      } else {
        this.replicaGetIndices = null;
      }
      this.errorsByServer = createServerErrorTracker();
      this.errors = (globalErrors != null) ? globalErrors : new BatchErrors();
    }

    /**
     * Group a list of actions per region servers, and send them.
     *
     * @param currentActions - the list of row to submit
     * @param numAttempt - the current numAttempt (first attempt is 1)
     */
    private void groupAndSendMultiAction(List<Action<Row>> currentActions, int numAttempt) {
      // group per location => regions server
      final Map<ServerName, MultiAction<Row>> actionsByServer =
          new HashMap<ServerName, MultiAction<Row>>();

      boolean isReplica = false;
      for (Action<Row> action : currentActions) {
        RegionLocations locs = null;
        try {
          locs = findDestLocation(tableName, action.getAction(), false);
        } catch (IOException ex) {
          // There are multiple retries in locateRegion already. No need to add new.
          // We can't continue with this row, hence it's the last retry.
          manageError(action.getOriginalIndex(), action.getAction(), false, ex, null);
          continue;
        }

        boolean isReplicaAction = !RegionReplicaUtil.isDefaultReplica(action.getReplicaId());
        if (isReplica && !isReplicaAction) {
          // This is the property of the current implementation, not a requirement.
          throw new AssertionError("Replica and non-replica actions in the same retry");
        }
        isReplica = isReplicaAction;
        HRegionLocation loc = locs.getRegionLocation(action.getReplicaId());
        if (loc == null || loc.getServerName() == null) {
          // On retry, we couldn't find location for some replica we saw before.
          String str = "Cannot find location for replica " + action.getReplicaId();
          LOG.error(str);
          manageError(action.getOriginalIndex(), action.getAction(),
              false, new IOException(str), null);
          continue;
        }
        byte[] regionName = loc.getRegionInfo().getRegionName();
        addAction(loc.getServerName(), regionName, action, actionsByServer, nonceGroup);
      }
      // If this is a first attempt to group and send, no replicas, we need replica thread.
      if (!actionsByServer.isEmpty()) {
        boolean doStartReplica = (numAttempt == 1 && !isReplica && hasAnyReplicaGets);
        sendMultiAction(actionsByServer, numAttempt, doStartReplica ? currentActions : null);
      }
    }

    /**
     * Send a multi action structure to the servers, after a delay depending on the attempt
     * number. Asynchronous.
     *
     * @param actionsByServer the actions structured by regions
     * @param numAttempt      the attempt number.
     * @param actionsForReplicaThread original actions for replica thread; null on non-first call.
     */
    private void sendMultiAction(Map<ServerName, MultiAction<Row>> actionsByServer,
        int numAttempt, List<Action<Row>> actionsForReplicaThread) {
      // Run the last item on the same thread if we are already on a send thread.
      // We hope most of the time it will be the only item, so we can cut down on threads.
      int actionsRemaining = actionsByServer.size();
      // This iteration is by server (the HRegionLocation comparator is by server portion only).
      for (Map.Entry<ServerName, MultiAction<Row>> e : actionsByServer.entrySet()) {
        final ServerName server = e.getKey();
        final MultiAction<Row> multiAction = e.getValue();
        incTaskCounters(multiAction.getRegions(), server);
        Runnable runnable = Trace.wrap("AsyncProcess.sendMultiAction",
            new SingleServerRequestRunnable(multiAction, numAttempt, server));
        --actionsRemaining;
        if ((numAttempt > 1) && actionsRemaining == 0) {
          runnable.run();
        } else {
          try {
            pool.submit(runnable);
          } catch (RejectedExecutionException ree) {
            // This should never happen. But as the pool is provided by the end user, let's secure
            //  this a little.
            decTaskCounters(multiAction.getRegions(), server);
            LOG.warn("#" + id + ", the task was rejected by the pool. This is unexpected." +
                " Server is " + server.getServerName(), ree);
            // We're likely to fail again, but this will increment the attempt counter, so it will
            //  finish.
            receiveGlobalFailure(multiAction, server, numAttempt, ree);
          }
        }
      }
      if (actionsForReplicaThread != null) {
        startWaitingForReplicaCalls(actionsForReplicaThread);
      }
    }

    /**
     * Starts waiting to issue replica calls on a different thread; or issues them immediately.
     */
    private void startWaitingForReplicaCalls(List<Action<Row>> actionsForReplicaThread) {
      long startTime = EnvironmentEdgeManager.currentTimeMillis();
      ReplicaCallIssuingRunnable replicaRunnable = new ReplicaCallIssuingRunnable(
          actionsForReplicaThread, startTime);
      if (primaryCallTimeoutMicroseconds == 0) {
        // Start replica calls immediately.
        replicaRunnable.run();
      } else {
        // Start the thread that may kick off replica gets.
        // TODO: we could do it on the same thread, but it's a user thread, might be a bad idea.
        try {
          pool.submit(replicaRunnable);
        } catch (RejectedExecutionException ree) {
          LOG.warn("#" + id + ", replica task was rejected by the pool - no replica calls", ree);
        }
      }
    }

    /**
     * Check that we can retry acts accordingly: logs, set the error status.
     *
     * @param originalIndex the position in the list sent
     * @param row           the row
     * @param canRetry      if false, we won't retry whatever the settings.
     * @param throwable     the throwable, if any (can be null)
     * @param server        the location, if any (can be null)
     * @return true if the action can be retried, false otherwise.
     */
    public boolean manageError(int originalIndex, Row row, boolean canRetry,
                                Throwable throwable, ServerName server) {
      if (canRetry && throwable != null && throwable instanceof DoNotRetryIOException) {
        canRetry = false;
      }

      if (!canRetry) {
        // Batch.Callback<Res> was not called on failure in 0.94. We keep this.
        setError(originalIndex, row, throwable, server);
      } else {
        // See if we are dealing with a replica action that was completed from other server.
        // Doesn't have to be synchronized, worst case we'd retry and be unable to set result.
        canRetry = !isActionComplete(originalIndex, row);
      }

      return canRetry;
    }

    /**
     * Resubmit all the actions from this multiaction after a failure.
     *
     * @param rsActions  the actions still to do from the initial list
     * @param server   the destination
     * @param numAttempt the number of attempts so far
     * @param t the throwable (if any) that caused the resubmit
     */
    private void receiveGlobalFailure(
        MultiAction<Row> rsActions, ServerName server, int numAttempt, Throwable t) {
      errorsByServer.reportServerError(server);
      boolean canRetry = errorsByServer.canRetryMore(numAttempt);

      List<Action<Row>> toReplay = new ArrayList<Action<Row>>();
      for (Map.Entry<byte[], List<Action<Row>>> e : rsActions.actions.entrySet()) {
        byte[] regionName = e.getKey();
        byte[] row = e.getValue().iterator().next().getAction().getRow();
        // Do not use the exception for updating cache because it might be coming from
        // any of the regions in the MultiAction.
        // TODO: depending on type of exception we might not want to update cache at all?
        hConnection.updateCachedLocations(tableName, regionName, row, null, server);
        for (Action<Row> action : e.getValue()) {
          if (manageError(action.getOriginalIndex(), action.getAction(), canRetry, t, server)) {
            toReplay.add(action);
          }
        }
      }

      logAndResubmit(server, toReplay, numAttempt, rsActions.size(), t);
    }

    /**
     * Log as much info as possible, and, if there is something to replay,
     * submit it again after a back off sleep.
     */
    private void logAndResubmit(ServerName oldServer, List<Action<Row>> toReplay,
        int numAttempt, int failureCount, Throwable throwable) {
      if (toReplay.isEmpty()) {
        // it's either a success or a last failure
        if (failureCount != 0) {
          // We have a failure but nothing to retry. We're done, it's a final failure..
          LOG.warn(createLog(numAttempt, failureCount, toReplay.size(),
              oldServer, throwable, -1, false, errorsByServer.getStartTrackingTime()));
        } else if (numAttempt > startLogErrorsCnt + 1) {
          // The operation was successful, but needed several attempts. Let's log this.
          LOG.info(createLog(numAttempt, failureCount, 0,
              oldServer, throwable, -1, false, errorsByServer.getStartTrackingTime()));
        }
        return;
      }

      // We have something to replay. We're going to sleep a little before.

      // We have two contradicting needs here:
      //  1) We want to get the new location after having slept, as it may change.
      //  2) We want to take into account the location when calculating the sleep time.
      // It should be possible to have some heuristics to take the right decision. Short term,
      //  we go for one.
      long backOffTime = errorsByServer.calculateBackoffTime(oldServer, pause);
      if (numAttempt > startLogErrorsCnt) {
        // We use this value to have some logs when we have multiple failures, but not too many
        //  logs, as errors are to be expected when a region moves, splits and so on
        LOG.info(createLog(numAttempt, failureCount, toReplay.size(),
            oldServer, throwable, backOffTime, true, errorsByServer.getStartTrackingTime()));
      }

      try {
        Thread.sleep(backOffTime);
      } catch (InterruptedException e) {
        LOG.warn("#" + id + ", not sent: " + toReplay.size() + " operations, " + oldServer, e);
        Thread.currentThread().interrupt();
        return;
      }

      groupAndSendMultiAction(toReplay, numAttempt + 1);
    }

    /**
     * Called when we receive the result of a server query.
     *
     * @param multiAction    - the multiAction we sent
     * @param server       - the location. It's used as a server name.
     * @param responses      - the response, if any
     * @param numAttempt     - the attempt
     */
    private void receiveMultiAction(MultiAction<Row> multiAction,
        ServerName server, MultiResponse responses, int numAttempt) {
       assert responses != null;

      // Success or partial success
      // Analyze detailed results. We can still have individual failures to be redo.
      // two specific throwables are managed:
      //  - DoNotRetryIOException: we continue to retry for other actions
      //  - RegionMovedException: we update the cache with the new region location

      List<Action<Row>> toReplay = new ArrayList<Action<Row>>();
      Throwable throwable = null;
      int failureCount = 0;
      boolean canRetry = true;

      // Go by original action.
      for (Map.Entry<byte[], List<Action<Row>>> regionEntry : multiAction.actions.entrySet()) {
        byte[] regionName = regionEntry.getKey();
        Map<Integer, Object> regionResults = responses.getResults().get(regionName);
        if (regionResults == null) {
          if (!responses.getExceptions().containsKey(regionName)) {
            LOG.error("Server sent us neither results nor exceptions for "
                + Bytes.toStringBinary(regionName));
            responses.getExceptions().put(regionName, new RuntimeException("Invalid response"));
          }
          continue;
        }
        boolean regionFailureRegistered = false;
        for (Action<Row> sentAction : regionEntry.getValue()) {
          Object result = regionResults.get(sentAction.getOriginalIndex());
          // Failure: retry if it's make sense else update the errors lists
          if (result == null || result instanceof Throwable) {
            Row row = sentAction.getAction();
            // Register corresponding failures once per server/once per region.
            if (!regionFailureRegistered) {
              regionFailureRegistered = true;
              hConnection.updateCachedLocations(
                  tableName, regionName, row.getRow(), result, server);
            }
            if (failureCount == 0) {
              errorsByServer.reportServerError(server);
              // We determine canRetry only once for all calls, after reporting server failure.
              canRetry = errorsByServer.canRetryMore(numAttempt);
            }
            ++failureCount;
            if (manageError(
                sentAction.getOriginalIndex(), row, canRetry, (Throwable)result, server)) {
              toReplay.add(sentAction);
            }
          } else {
            if (callback != null) {
              try {
                //noinspection unchecked
                // TODO: would callback expect a replica region name if it gets one?
                this.callback.update(regionName, sentAction.getAction().getRow(), (CResult)result);
              } catch (Throwable t) {
                LOG.error("User callback threw an exception for "
                    + Bytes.toStringBinary(regionName) + ", ignoring", t);
              }
            }
            setResult(sentAction, result);
          }
        }
      }

      // The failures global to a region. We will use for multiAction we sent previously to find the
      //   actions to replay.
      for (Map.Entry<byte[], Throwable> throwableEntry : responses.getExceptions().entrySet()) {
        throwable = throwableEntry.getValue();
        byte[] region = throwableEntry.getKey();
        List<Action<Row>> actions = multiAction.actions.get(region);
        if (actions == null || actions.isEmpty()) {
          throw new IllegalStateException("Wrong response for the region: " +
              HRegionInfo.encodeRegionName(region));
        }

        if (failureCount == 0) {
          errorsByServer.reportServerError(server);
          canRetry = errorsByServer.canRetryMore(numAttempt);
        }
        hConnection.updateCachedLocations(
            tableName, region, actions.get(0).getAction().getRow(), throwable, server);
        failureCount += actions.size();

        for (Action<Row> action : actions) {
          Row row = action.getAction();
          if (manageError(action.getOriginalIndex(), row, canRetry, throwable, server)) {
            toReplay.add(action);
          }
        }
      }

      logAndResubmit(server, toReplay, numAttempt, failureCount, throwable);
    }

    private String createLog(int numAttempt, int failureCount, int replaySize, ServerName sn,
                             Throwable error, long backOffTime, boolean willRetry, String startTime){
      StringBuilder sb = new StringBuilder();

      sb.append("#").append(id).append(", table=").append(tableName).
          append(", attempt=").append(numAttempt).append("/").append(numTries).append(" ");

      if (failureCount > 0 || error != null){
        sb.append("failed ").append(failureCount).append(" ops").append(", last exception: ").
            append(error == null ? "null" : error);
      } else {
        sb.append("SUCCEEDED");
      }

      sb.append(" on ").append(sn);

      sb.append(", tracking started ").append(startTime);

      if (willRetry) {
        sb.append(", retrying after ").append(backOffTime).append(" ms").
            append(", replay ").append(replaySize).append(" ops.");
      } else if (failureCount > 0) {
        sb.append(" - FAILED, NOT RETRYING ANYMORE");
      }

      return sb.toString();
    }

    /**
     * Sets the non-error result from a particular action.
     * @param action Action (request) that the server responded to.
     * @param result The result.
     */
    private void setResult(Action<Row> action, Object result) {
      ReplicaResultState state = null;
      boolean isStale = !RegionReplicaUtil.isDefaultReplica(action.getReplicaId());
      if (results == null || ((state = trySetResultSimple(
          action.getOriginalIndex(), action.getAction(), result, isStale)) == null)) {
        decActionCounter();
        return; // Simple case, no replica requests.
      }
      synchronized (state) {
        if (state.callCount == 0) return; // someone already set the result
        state.result = result;
        state.callCount = 0;
        state.replicaErrors = null; // no longer matters
      }
      decActionCounter();
    }

    /**
     * Sets the error from a particular action.
     * @param index Original action index.
     * @param row Original request.
     * @param throwable The resulting error.
     * @param server The source server.
     */
    private void setError(int index, Row row, Throwable throwable, ServerName server) {
      ReplicaResultState state = null;
      if (results == null
          || ((state = trySetResultSimple(index, row, throwable, false)) == null)) {
        errors.add(throwable, row, server);
        decActionCounter();
        return; // Simple case, no replica requests.
      }
      BatchErrors target = null; // Error will be added to final errors, or temp replica errors.
      boolean isActionDone = false;
      synchronized (state) {
        switch (state.callCount) {
          case 0: return; // someone already set the result
          case 1: { // All calls failed, we are the last error.
            state.result = throwable;
            target = errors;
            isActionDone = true;
            break;
          }
          default: {
            assert state.callCount > 1;
            if (state.replicaErrors == null) {
              state.replicaErrors = new BatchErrors();
            }
            target = state.replicaErrors;
            break;
          }
        }
        --state.callCount;
      }
      target.add(throwable, row, server);
      if (!isActionDone) return;
      if (state.replicaErrors != null) { // last call, no need to lock
        errors.merge(state.replicaErrors);
        state.replicaErrors = null;
      }
      decActionCounter();
    }

    /**
     * Checks if the action is complete; used on error to prevent needless retries.
     * Does not synchronize, assuming element index/field accesses are atomic.
     * This is an opportunistic optimization check, doesn't have to be strict.
     * @param index Original action index.
     * @param row Original request.
     */
    private boolean isActionComplete(int index, Row row) {
      if (!isReplicaGet(row)) return false;
      Object resObj = results[index];
      return (resObj != null) && (!(resObj instanceof ReplicaResultState)
          || ((ReplicaResultState)resObj).callCount == 0);
    }

    /**
     * Tries to set the result or error for a particular action as if there were no replica calls.
     * @return null if successful; replica state if there were in fact replica calls.
     */
    private ReplicaResultState trySetResultSimple(
        int index, Row row, Object result, boolean isFromReplica) {
      Object resObj = null;
      if (!isReplicaGet(row)) {
        if (isFromReplica) {
          throw new AssertionError("Unexpected stale result for " + row);
        }
        results[index] = result;
      } else {
        synchronized (replicaResultLock) {
          if ((resObj = results[index]) == null) {
            if (isFromReplica) {
              throw new AssertionError("Unexpected stale result for " + row);
            }
            results[index] = result;
          }
        }
      }
      return (resObj == null || !(resObj instanceof ReplicaResultState))
          ? null : (ReplicaResultState)resObj;
    }

    private void decActionCounter() {
      if (hasAnyReplicaGets && (actionsInProgress.get() == 1)) {
        // Convert replica sync structures to results.
        int staleCount = 0;
        if (replicaGetIndices == null) {
          for (int i = 0; i < results.length; ++i) {
            staleCount += convertReplicaResult(i) ? 1 : 0;
          }
        } else {
          for (int i = 0; i < replicaGetIndices.length; ++i) {
            staleCount += convertReplicaResult(replicaGetIndices[i]) ? 1 : 0;
          }
        }
        if (!actionsInProgress.compareAndSet(1, 0)) {
          throw new AssertionError("Cannot set actions in progress to 0");
        }
        if (staleCount > 0) {
          LOG.trace("Returning " + staleCount + " stale results");
        }
      } else {
        actionsInProgress.decrementAndGet();
      }
      synchronized (actionsInProgress) {
        actionsInProgress.notifyAll();
      }
    }

    private boolean convertReplicaResult(int index) {
      if (!(results[index] instanceof ReplicaResultState)) return false;
      ReplicaResultState state = (ReplicaResultState)results[index];
      // We know that noone will touch state with 0 callCount, no need to lock
      if (state.callCount != 0) {
        throw new AssertionError("Actions are done but callcount is " + state.callCount);
      }
      // TODO: we expect the Result coming from server to already have "isStale" specified.
      Object res = results[index] = state.result;
      return (res instanceof Result) && ((Result)res).isStale();
    }

    @Override
    public void waitUntilDone() throws InterruptedIOException {
      try {
        waitUntilDone(Long.MAX_VALUE);
      } catch (InterruptedException iex) {
        throw new InterruptedIOException(iex.getMessage());
      }
    }

    private boolean waitUntilDone(long cutoff) throws InterruptedException {
      boolean hasWait = cutoff != Long.MAX_VALUE;
      long lastLog = EnvironmentEdgeManager.currentTimeMillis();
      long currentInProgress;
      while (0 != (currentInProgress = actionsInProgress.get())) {
        long now = EnvironmentEdgeManager.currentTimeMillis();
        if (hasWait && (now * 1000L) > cutoff) {
          return false;
        }
        if (!hasWait) { // Only log if wait is infinite.
          if (now > lastLog + 10000) {
            lastLog = now;
            LOG.info("#" + id + ", waiting for " + currentInProgress + "  actions to finish");
          }
        }
        synchronized (actionsInProgress) {
          if (actionsInProgress.get() == 0) break;
          if (!hasWait) {
            actionsInProgress.wait(100);
          } else {
            long waitMicroSecond = Math.min(100000L, (cutoff - now * 1000L));
            TimeUnit.MICROSECONDS.timedWait(actionsInProgress, waitMicroSecond);
          }
        }
      }
      return true;
    }

    @Override
    public boolean hasError() {
      return errors.hasErrors();
    }

    @Override
    public List<? extends Row> getFailedOperations() {
      return errors.actions;
    }

    @Override
    public RetriesExhaustedWithDetailsException getErrors() {
      return errors.makeException();
    }

    @Override
    public Object[] getResults() throws InterruptedIOException {
      waitUntilDone();
      return results;
    }
  }


  @VisibleForTesting
  /** Create AsyncRequestFuture. Isolated to be easily overridden in the tests. */
  protected <CResult> AsyncRequestFutureImpl<CResult> createAsyncRequestFuture(
      TableName tableName, List<Action<Row>> actions, long nonceGroup, ExecutorService pool,
      Batch.Callback<CResult> callback, Object[] results, boolean needResults) {
    return new AsyncRequestFutureImpl<CResult>(
        tableName, actions, nonceGroup, getPool(pool), needResults, results, callback);
  }

  /**
   * Create a callable. Isolated to be easily overridden in the tests.
   */
  @VisibleForTesting
  protected MultiServerCallable<Row> createCallable(final ServerName server,
      TableName tableName, final MultiAction<Row> multi) {
    return new MultiServerCallable<Row>(hConnection, tableName, server, this.rpcFactory, multi);
  }

  /**
   * Create a caller. Isolated to be easily overridden in the tests.
   */
  @VisibleForTesting
  protected RpcRetryingCaller<MultiResponse> createCaller(MultiServerCallable<Row> callable) {
    return rpcCallerFactory.<MultiResponse> newCaller();
  }

  @VisibleForTesting
  /** Waits until all outstanding tasks are done. Used in tests. */
  void waitUntilDone() throws InterruptedIOException {
    waitForMaximumCurrentTasks(0);
  }

  /** Wait until the async does not have more than max tasks in progress. */
  private void waitForMaximumCurrentTasks(int max) throws InterruptedIOException {
    long lastLog = EnvironmentEdgeManager.currentTimeMillis();
    long currentInProgress, oldInProgress = Long.MAX_VALUE;
    while ((currentInProgress = this.tasksInProgress.get()) > max) {
      if (oldInProgress != currentInProgress) { // Wait for in progress to change.
        long now = EnvironmentEdgeManager.currentTimeMillis();
        if (now > lastLog + 10000) {
          lastLog = now;
          LOG.info("#" + id + ", waiting for some tasks to finish. Expected max="
              + max + ", tasksInProgress=" + currentInProgress);
        }
      }
      oldInProgress = currentInProgress;
      try {
        synchronized (this.tasksInProgress) {
          if (tasksInProgress.get() != oldInProgress) break;
          this.tasksInProgress.wait(100);
        }
      } catch (InterruptedException e) {
        throw new InterruptedIOException("#" + id + ", interrupted." +
            " currentNumberOfTask=" + currentInProgress);
      }
    }
  }

  /**
   * Only used w/useGlobalErrors ctor argument, for HTable backward compat.
   * @return Whether there were any errors in any request since the last time
   *          {@link #waitForAllPreviousOpsAndReset(List)} was called, or AP was created.
   */
  public boolean hasError() {
    return globalErrors.hasErrors();
  }

  /**
   * Only used w/useGlobalErrors ctor argument, for HTable backward compat.
   * Waits for all previous operations to finish, and returns errors and (optionally)
   * failed operations themselves.
   * @param failedRows an optional list into which the rows that failed since the last time
   *        {@link #waitForAllPreviousOpsAndReset(List)} was called, or AP was created, are saved.
   * @return all the errors since the last time {@link #waitForAllPreviousOpsAndReset(List)}
   *          was called, or AP was created.
   */
  public RetriesExhaustedWithDetailsException waitForAllPreviousOpsAndReset(
      List<Row> failedRows) throws InterruptedIOException {
    waitForMaximumCurrentTasks(0);
    if (!globalErrors.hasErrors()) {
      return null;
    }
    if (failedRows != null) {
      failedRows.addAll(globalErrors.actions);
    }
    RetriesExhaustedWithDetailsException result = globalErrors.makeException();
    globalErrors.clear();
    return result;
  }

  /**
   * increment the tasks counters for a given set of regions. MT safe.
   */
  protected void incTaskCounters(Collection<byte[]> regions, ServerName sn) {
    tasksInProgress.incrementAndGet();

    AtomicInteger serverCnt = taskCounterPerServer.get(sn);
    if (serverCnt == null) {
      taskCounterPerServer.putIfAbsent(sn, new AtomicInteger());
      serverCnt = taskCounterPerServer.get(sn);
    }
    serverCnt.incrementAndGet();

    for (byte[] regBytes : regions) {
      AtomicInteger regionCnt = taskCounterPerRegion.get(regBytes);
      if (regionCnt == null) {
        regionCnt = new AtomicInteger();
        AtomicInteger oldCnt = taskCounterPerRegion.putIfAbsent(regBytes, regionCnt);
        if (oldCnt != null) {
          regionCnt = oldCnt;
        }
      }
      regionCnt.incrementAndGet();
    }
  }

  /**
   * Decrements the counters for a given region and the region server. MT Safe.
   */
  protected void decTaskCounters(Collection<byte[]> regions, ServerName sn) {
    for (byte[] regBytes : regions) {
      AtomicInteger regionCnt = taskCounterPerRegion.get(regBytes);
      regionCnt.decrementAndGet();
    }

    taskCounterPerServer.get(sn).decrementAndGet();
    tasksInProgress.decrementAndGet();
    synchronized (tasksInProgress) {
      tasksInProgress.notifyAll();
    }
  }

  /**
   * Creates the server error tracker to use inside process.
   * Currently, to preserve the main assumption about current retries, and to work well with
   * the retry-limit-based calculation, the calculation is local per Process object.
   * We may benefit from connection-wide tracking of server errors.
   * @return ServerErrorTracker to use, null if there is no ServerErrorTracker on this connection
   */
  protected ConnectionManager.ServerErrorTracker createServerErrorTracker() {
    return new ConnectionManager.ServerErrorTracker(
        this.serverTrackerTimeout, this.numTries);
  }

  private static boolean isReplicaGet(Row row) {
    return (row instanceof Get) && (((Get)row).getConsistency() == Consistency.TIMELINE);
  }
}
