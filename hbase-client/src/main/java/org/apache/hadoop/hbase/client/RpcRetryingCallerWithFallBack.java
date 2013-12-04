package org.apache.hadoop.hbase.client;


import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class RpcRetryingCallerWithFallBack  {
  static final Log LOG = LogFactory.getLog(RpcRetryingCallerWithFallBack.class);

  protected ExecutorService pool;

  protected HConnection connection;
  protected Configuration conf;

  protected final Get get;

  protected TableName tableName;
  protected long timeBeforeReplicas;
  protected int replicaCount;
  /**
   * Timeout for the call including retries
   */
  private int callTimeout;
  private final int retries;

  private final AtomicBoolean finished = new AtomicBoolean(false);

  public RpcRetryingCallerWithFallBack(
      HConnection connection, final Get get, ExecutorService pool, int replicaCount, int retries, int callTimeout) {
    this.connection = connection;
    this.conf = connection.getConfiguration();
    this.get = get;
    this.pool = pool;
    this.retries = retries;
    this.callTimeout = callTimeout;
    this.replicaCount = replicaCount;
    this.timeBeforeReplicas = 50; // todo: should be configurable
  }

  /**
   * Takes into account the replicas, i.e.
   *  - the call can be on any replica
   *  - we need to stop retrying when the call is completed
   *  - todo: we need to cancel the call in progress.
   */
  class ReplicaRegionServerCallable extends RegionServerCallable<Result> {
    int id;

    public ReplicaRegionServerCallable(int id) {
      super(connection, tableName, get.getRow());
      this.id = id;
    }

    /**
     * Two responsibilities
     *  - if the call is already completed (by another replica) stops the retries.
     *  - set the location to the right region, depending on the replica.
     */
    @Override
    public void prepare(final boolean reload) throws IOException {
      if (finished.get()){
        throw new DoNotRetryIOException("Operation already finished on another replica");
      }

      // todo: how to get the location of the region?
      this.location = connection.getRegionLocation(tableName, row, reload);
      if (this.location == null) {
        throw new IOException("Failed to find location, tableName=" + tableName +
            ", row=" + Bytes.toString(row) + ", reload=" + reload);
      }
      setStub(getConnection().getClient(getLocation().getServerName()));
    }

    @Override
    public Result call() throws Exception {
      return ProtobufUtil.get(getConnection().getClient(
          getLocation().getServerName()), getHRegionInfo().getRegionName(), get);
          // todo we should build the object only once
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
    List<Future<Result>> inProgress = new ArrayList<Future<Result>>();
    ReplicaRegionServerCallable mainCall = new ReplicaRegionServerCallable(0);
    RetryingRPC retryingRPC = new RetryingRPC(mainCall);

    List<RetriesExhaustedException.ThrowableWithExtraContext> exceptions =
        new ArrayList<RetriesExhaustedException.ThrowableWithExtraContext>();

    Future<Result> mainReturn = pool.submit(retryingRPC);

    boolean done = false;
    try {
      mainReturn.get(timeBeforeReplicas, TimeUnit.MILLISECONDS);
      done = true;
    } catch (ExecutionException e) {
      done = true;
      Throwable t = translateException(e);
      RetriesExhaustedException.ThrowableWithExtraContext qt =
          new RetriesExhaustedException.ThrowableWithExtraContext(t,
              EnvironmentEdgeManager.currentTimeMillis(), toString());
      exceptions.add(qt);
    } catch (TimeoutException ignored) {
    } catch (InterruptedException e) {
      mainReturn.cancel(true);
      throw new InterruptedIOException();
    }

    if (!done) {
      inProgress.add(mainReturn);
      for (int i = 1; i <= replicaCount; i++) {
        ReplicaRegionServerCallable callOnReplica = new ReplicaRegionServerCallable(i);
        RetryingRPC retryingOnReplica = new RetryingRPC(callOnReplica);
        inProgress.add(pool.submit(retryingOnReplica));
      }
    }

    while (!inProgress.isEmpty()) {
      try {
        synchronized (finished) {
          finished.wait(100);
        }
        // If one of the task succeeded we return the result. If not, we continue.
        for (Future<Result> task : inProgress) {
          if (task.isDone()) {
            try {
              Result r = task.get();
              finished.set(true); // We've got a result. Any other call can now stop.
              return r;
            } catch (ExecutionException e) {
              Throwable t = translateException(e);
              RetriesExhaustedException.ThrowableWithExtraContext qt =
                  new RetriesExhaustedException.ThrowableWithExtraContext(t,
                      EnvironmentEdgeManager.currentTimeMillis(), toString());
              exceptions.add(qt);
              inProgress.remove(task);
              break;
            }
          }
        }
      } catch (InterruptedException e) {
        throw new InterruptedIOException();
      } finally {
        for (Future<Result> task : inProgress) {
          task.cancel(true);
        }
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
  static Throwable translateException(Throwable t) throws DoNotRetryIOException {
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
    }
    return t;
  }
}
