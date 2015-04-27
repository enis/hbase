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

package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.region.RegionServices;
import org.apache.hadoop.hbase.regionserver.LogRoller;
import org.apache.hadoop.hbase.regionserver.RegionServerRunningException;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.ipc.RemoteException;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

/**
 * A concrete implementation of WALServices. WALContainer hosts the WALFactory and LogRoller(s).
 */
@InterfaceAudience.Private
public class WALContainer implements WALServices {
  private static final Log LOG = LogFactory.getLog(WALContainer.class);

  private static final byte[] UNSPECIFIED_REGION = new byte[]{};

  private final Server server;
  private final RegionServices regionServices;
  private final UncaughtExceptionHandler uncaughtExceptionHandler;

  protected volatile WALFactory walFactory;

  // WAL roller. log is protected rather than private to avoid
  // eclipse warning when accessed by inner classes
  final LogRoller walRoller;
  // Lazily initialized if this container hosts a meta table.
  final AtomicReference<LogRoller> metawalRoller = new AtomicReference<LogRoller>();

  public WALContainer(final Server server, final RegionServices regionServices,
      UncaughtExceptionHandler uncaughtExceptionHandler) {
    this.walRoller = new LogRoller(server, regionServices);
    this.server = server;
    this.regionServices = regionServices;
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
  }

  @Override
  public WAL getWAL(HRegionInfo regionInfo) throws IOException {
    WAL wal;
    LogRoller roller = walRoller;
    //_ROOT_ and hbase:meta regions have separate WAL.
    if (regionInfo != null && regionInfo.isMetaTable() &&
        regionInfo.getReplicaId() == HRegionInfo.DEFAULT_REPLICA_ID) {
      roller = ensureMetaWALRoller();
      wal = walFactory.getMetaWAL(regionInfo.getEncodedNameAsBytes());
    } else if (regionInfo == null) {
      wal = walFactory.getWAL(UNSPECIFIED_REGION);
    } else {
      wal = walFactory.getWAL(regionInfo.getEncodedNameAsBytes());
    }
    roller.addWAL(wal);
    return wal;
  }

  public static class FSWALContext {
    public FileSystem fs;
    /** Directory for WALs for all servers*/
    public Path walDir;
    /** Archive directory for WALs */
    public Path walArchiveDir;
    /** Directory for WALs for this server*/
    public  Path serverWalDir;

    public FSWALContext(FileSystem fs, Path walDir, Path walArchiveDir, Path serverWalDir) {
      this.fs = fs;
      this.walDir = walDir;
      this.walArchiveDir = walArchiveDir;
      this.serverWalDir = serverWalDir;
    }
  }

  public FSWALContext checkServerWALDir(FileSystem fs, Path rootDir, ServerName serverName)
      throws IOException {
    final Path walArchiveDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    final Path walDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    final String serverWalDirName = DefaultWALProvider.getWALDirectoryName(serverName.toString());

    Path serverWalDir = new Path(rootDir, serverWalDirName);
    if (LOG.isDebugEnabled()) LOG.debug("logdir=" + serverWalDir);
    if (fs.exists(serverWalDir)) {
      throw new RegionServerRunningException("Region server has already " +
        "created directory at " + serverName.toString());
    }
    return new FSWALContext(fs, walDir, walArchiveDir, serverWalDir);
  }

  /**
   * Setup WAL
   * @throws IOException
   */
  public void setupWAL(Configuration conf, ServerName serverName,
      List<WALActionsListener> listeners) throws IOException {
    this.walFactory = new WALFactory(conf, listeners, serverName.toString());
  }

  @Override
  public WALFactory getWALFactory() {
    return this.walFactory;
  }

  @Override
  public void requestRollAll() {
    walRoller.requestRollAll();
  }

  public void startServiceThreads(String daemonName) {
    Threads.setDaemonThreadRunning(this.walRoller.getThread(), daemonName + ".logRoller",
      uncaughtExceptionHandler);
  }

  public void stopServiceThreads() {
    if (this.walRoller != null) {
      Threads.shutdown(this.walRoller.getThread());
    }
    final LogRoller metawalRoller = this.metawalRoller.get();
    if (metawalRoller != null) {
      Threads.shutdown(metawalRoller.getThread());
    }
  }

  public boolean isServiceThreadsAlive() {
    if (!walRoller.isAlive()) {
      return false;
    }
    final LogRoller metawalRoller = this.metawalRoller.get();
    if (metawalRoller != null && !metawalRoller.isAlive()) {
      return false;
    }
    return true;
  }

  /**
   * We initialize the roller for the wal that handles meta lazily
   * since we don't know if this regionserver will handle it. All calls to
   * this method return a reference to the that same roller. As newly referenced
   * meta regions are brought online, they will be offered to the roller for maintenance.
   * As a part of that registration process, the roller will add itself as a
   * listener on the wal.
   */
  protected LogRoller ensureMetaWALRoller() {
    // Using a tmp log roller to ensure metaLogRoller is alive once it is not
    // null
    LogRoller roller = metawalRoller.get();
    if (null == roller) {
      LogRoller tmpLogRoller = new LogRoller(server, regionServices);
      String n = Thread.currentThread().getName();
      Threads.setDaemonThreadRunning(tmpLogRoller.getThread(),
          n + "-MetaLogRoller", uncaughtExceptionHandler);
      if (metawalRoller.compareAndSet(null, tmpLogRoller)) {
        roller = tmpLogRoller;
      } else {
        // Another thread won starting the roller
        Threads.shutdown(tmpLogRoller.getThread());
        roller = metawalRoller.get();
      }
    }
    return roller;
  }

  public void shutdownWAL(final boolean close) {
    if (this.walFactory != null) {
      try {
        if (close) {
          walFactory.close();
        } else {
          walFactory.shutdown();
        }
      } catch (Throwable e) {
        e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
        LOG.error("Shutdown / close of WAL failed: " + e);
        LOG.debug("Shutdown / close exception details:", e);
      }
    }
  }
}
