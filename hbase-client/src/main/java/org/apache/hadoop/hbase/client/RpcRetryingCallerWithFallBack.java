package org.apache.hadoop.hbase.client;


import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class RpcRetryingCallerWithFallBack {
  static final Log LOG = LogFactory.getLog(RpcRetryingCallerWithFallBack.class);

  protected final ExecutorService pool;

  protected final HConnection connection;
  protected final Configuration conf;

  protected final Get get;

  protected final TableName tableName;
  protected final long timeBeforeReplicas;
  /**
   * Timeout for the call including retries
   */
  private final int callTimeout;
  private final int retries;

  private final AtomicBoolean finished = new AtomicBoolean(false);

  public RpcRetryingCallerWithFallBack(TableName tableName,
                                       HConnection connection, final Get get, ExecutorService pool, int retries, int callTimeout, int timeBeforeReplicas) {
    this.tableName = tableName;
    this.connection = connection;
    this.conf = connection.getConfiguration();
    this.get = get;
    this.pool = pool;
    this.retries = retries;
    this.callTimeout = callTimeout;
    this.timeBeforeReplicas = timeBeforeReplicas;
  }

  /**
   * Takes into account the replicas, i.e.
   * - the call can be on any replica
   * - we need to stop retrying when the call is completed
   * - todo: we need to cancel the call in progress.
   */
  class ReplicaRegionServerCallable extends RegionServerCallable<Result> {
    final int id;

    public ReplicaRegionServerCallable(int id) {
      super(connection, tableName, get.getRow());
      this.id = id;
    }

    /**
     * Two responsibilities
     * - if the call is already completed (by another replica) stops the retries.
     * - set the location to the right region, depending on the replica.
     */
    @Override
    public void prepare(final boolean reload) throws IOException {
      if (finished.get()) {
        throw new DoNotRetryIOException("Operation already finished on another replica");
      }

      this.location = connection.getRegionLocation(tableName, row, true);
      if (this.location == null) {
        throw new IOException("Failed to find location, tableName=" + tableName +
            ", row=" + Bytes.toString(row) + ", reload=" + reload);
      }

      ServerName dest;
      if (id == HRegionInfo.REPLICA_ID_PRIMARY) {
        dest = getLocation().getServerName();
      } else {
        List<ServerName> snl = getLocation().getSecondaryServers();
        if (snl == null || snl.size() < id - 1) {
          throw new IOException("No replica of " + id);
        }
        dest = snl.get(id - 1);
      }

      setStub(getConnection().getClient(dest));
    }

    @Override
    public Result call() throws Exception {
      HRegionInfo mainHri = getHRegionInfo();
      byte[] reg =  mainHri.getRegionName();
      ServerName sn =  getLocation().getServerName();
      LOG.info("Calling replica with id=" + id +", sn=" + sn+ ", " +  mainHri.getRegionInfoForReplica(id).getEncodedName() + " " + Arrays.toString(reg));

      Result r = ProtobufUtil.get(getConnection().getClient(sn), reg, get);
      // todo we should build the object only once, but we can't right now as the protobufUtils wants
      //  the destination server.

      LOG.info("Got result from replica with id=" + id);

      if (id != HRegionInfo.REPLICA_ID_PRIMARY) {
        LOG.debug("Found a result on a secondary replicas, id=" + id);
        r.setStale(true);
      }
      return r;
    }
  }

  class RetryingRPC implements Callable<Result> {
    final RetryingCallable<Result> callable;

    RetryingRPC(RetryingCallable<Result> callable) {
      this.callable = callable;
    }

    @Override
    public Result call() throws Exception {
      try {
        return new RpcRetryingCallerFactory(conf).<Result>newCaller().callWithRetries(callable, callTimeout);
      } finally {
        synchronized (finished) {
          finished.notify();
        }
      }
    }
  }

  /**
   * Algo:
   * - we put the query into the execution pool.
   * - after x ms, we add the query for the secondary pools
   * - we take the first answer
   * - we cancel the others. Cancelling means:
   * - removing from the pool if the actual call was not started
   * - interrupting the call if it has started
   * - ideally, or may be, saying to the server that we're cancelling (todo).
   * Client side, we need to take into account
   * - a call is not executed immediately after being put into the pool
   * - a call is a thread. Let's not multiply the number of thread by the number of replicas.
   * Server side, if we can cancel when it's still in the handler pool, it's much better, as a call
   * can take some i/o.
   * <p/>
   * Globally, the number of retries, timeout and so on still applies, but it's per replica,
   * not global. We continue until all retries are done, or all timeouts are exceeded.
   */
  public synchronized Result call() throws DoNotRetryIOException, InterruptedIOException, RetriesExhaustedException {
    HRegionLocation location;
    try {
      connection.clearRegionCache(); // todo
      location = connection.getRegionLocation(tableName, get.getRow(), false);
    } catch (IOException e) {
      if (e instanceof DoNotRetryIOException) {
        throw (DoNotRetryIOException) e;
      } else if (e instanceof RetriesExhaustedException) {
        throw (RetriesExhaustedException) e;
      } else {
        throw new RetriesExhaustedException("Can't get the location", e);
      }
    }
    assert location != null;
    int secondaryReplicaCount = location.getSecondaryServers().size();


    List<Future<Result>> inProgress = new ArrayList<Future<Result>>();
    ReplicaRegionServerCallable mainCall = new ReplicaRegionServerCallable(HRegionInfo.REPLICA_ID_PRIMARY);
    RetryingRPC retryingRPC = new RetryingRPC(mainCall);

    List<RetriesExhaustedException.ThrowableWithExtraContext> exceptions =
        new ArrayList<RetriesExhaustedException.ThrowableWithExtraContext>();

    Future<Result> mainReturn = pool.submit(retryingRPC);

    boolean done = false;
    try {
      // Note that if the pool is full, we will send data to the replicas as well
      // It's a tradeoff: if we want to minimize latency; we MAY want to go to all the replica
      //  to be sure.
      return mainReturn.get(timeBeforeReplicas, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      LOG.info("Error on primary ", e);
      done = true;
      Throwable t = translateException(e);
      RetriesExhaustedException.ThrowableWithExtraContext qt =
          new RetriesExhaustedException.ThrowableWithExtraContext(t,
              EnvironmentEdgeManager.currentTimeMillis(), toString());
      exceptions.add(qt);
    } catch (TimeoutException ignored) {
    } catch (InterruptedException e) {
      mainReturn.cancel(false);
      throw new InterruptedIOException();
    }

    LOG.info("Primary is not fast enough. Using one of the  " + secondaryReplicaCount + " secondary replicas. done=" + done);

    if (!done) {
      inProgress.add(mainReturn);
      for (int i = 1; i <= secondaryReplicaCount; i++) {
        LOG.info("Primary is not fast enough. Using one of the  " + secondaryReplicaCount + " secondary replicas. done=" + done);
        ReplicaRegionServerCallable callOnReplica = new ReplicaRegionServerCallable(i);
        RetryingRPC retryingOnReplica = new RetryingRPC(callOnReplica);
        inProgress.add(pool.submit(retryingOnReplica));
      }
    }

    try {
      while (!inProgress.isEmpty()) {
        try {
          synchronized (finished) {
            finished.wait(100);
          }
          // If one of the task succeeded we return the result. If not, we continue.
          Iterator<Future<Result>> it = inProgress.iterator();
          while (it.hasNext()) {
            Future<Result> task = it.next();
            if (task.isDone()) {
              it.remove();
              if (!task.isCancelled()) {
                try {
                  Result r = task.get();
                  finished.set(true); // We've got a result. Any other call can now stop.
                  if (r.isStale()){
                    LOG.info("Using a stale data for the result");
                  }
                  return r;
                } catch (ExecutionException e) {
                  LOG.info("Caught " + e.getMessage());
                  Throwable t = translateException(e);
                  RetriesExhaustedException.ThrowableWithExtraContext qt =
                      new RetriesExhaustedException.ThrowableWithExtraContext(t,
                          EnvironmentEdgeManager.currentTimeMillis(), toString());
                  exceptions.add(qt);
                } catch (CancellationException ignored) {
                  // race condition: the task get cancelled before
                }
              }
              break;
            }
          }
        } catch (InterruptedException e) {
          throw new InterruptedIOException();
        }
      }
    } finally {
      for (Future<Result> task : inProgress) {
        task.cancel(false); // We should interrupt as well; but let's play it safe at the beginning.
      }
    }


    // if we're here, it means all attempts failed.
    throw new RetriesExhaustedException(retries, exceptions);
  }


  /**
   * Get the good or the remote exception if any, throws the DoNotRetryIOException.
   *
   * @param t the throwable to analyze
   * @return the translated exception, if it's not a DoNotRetryIOException
   * @throws org.apache.hadoop.hbase.DoNotRetryIOException
   *          - if we find it, we throw it instead of translating.
   */
  static Throwable translateException(Throwable t) throws DoNotRetryIOException, RetriesExhaustedException {
    if (t instanceof UndeclaredThrowableException) {
      if (t.getCause() != null) {
        t = t.getCause();
      }
    }
    if (t instanceof RemoteException) {
      t = ((RemoteException) t).unwrapRemoteException();
    }
    if (t instanceof ServiceException) {
      ServiceException se = (ServiceException) t;
      Throwable cause = se.getCause();
      if (cause != null && cause instanceof DoNotRetryIOException) {
        throw (DoNotRetryIOException) cause;
      }
      // Don't let ServiceException out; its rpc specific.
      t = cause;
      // t could be a RemoteException so go around again.
      translateException(t);
    } else if (t instanceof DoNotRetryIOException) {
      throw (DoNotRetryIOException) t;
    } else if (t instanceof RetriesExhaustedException) {
      throw (RetriesExhaustedException) t;
    }
    return t;
  }
}
