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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.procedure2.store.RegionProcedureStore;
import org.apache.hadoop.hbase.region.EmbeddedDatabase;
import org.apache.hadoop.hbase.util.FSUtils;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractService;

/**
 * An bootstap table is:
 *  - Only 1 region
 *  - Not assigned through regular assignment, independent of meta of other tables
 *  - Hosted in only 1 server (typically master)
 *  - Not visible through regular means.
 *
 * Bootstrap tables are pre defined.
 */
@InterfaceAudience.Private
public class BootstrapTableService extends AbstractService {
  private static final Log LOG = LogFactory.getLog(BootstrapTableService.class);

  private static String BOOTSTRAP_TABLE_DIR = "bootstrap-tables";

  private EmbeddedDatabase db;
  private Connection connection;

  /** We cannot use the region servers onlineRegions since it will make it visible to clients */

  public BootstrapTableService(Configuration conf, ServerName serverName,
      Abortable abortable, Stoppable stoppable,
      RegionServerServices regionServerServices)
      throws IOException {

    // TODO: add MonitoredTask

    // pass the procedure directory as the root directory for the WAL
    Path rootDir = FSUtils.getRootDir(conf);
    rootDir = new Path(rootDir, BOOTSTRAP_TABLE_DIR);

    db = new EmbeddedDatabase(conf, serverName, abortable, stoppable,
      regionServerServices, rootDir);
  }

  /**
   * Adds pre-defined bootstrap tables to the service.
   */
  private List<HTableDescriptor> getBootstrappedTables() {
    HTableDescriptor procedureTable = RegionProcedureStore.getTableDescriptor();

    return Lists.newArrayList(procedureTable);
  }

  @Override
  protected void doStart() {
    State state = db.startAndWait();
    if (state != State.RUNNING) {
      notifyFailed(new IOException("Could not start EmbeddedDatabase"));
      return;
    }

    connection = db.createConnection();
    try (Admin admin = connection.getAdmin()) {
      for (HTableDescriptor htd : getBootstrappedTables()) {
        if (!admin.tableExists(htd.getTableName())) {
          admin.createTable(htd);
        }
      }
    } catch (IOException e) {
      notifyFailed(e);
      return;
    }

    notifyStarted();
  }

  @Override
  protected void doStop() {
    boolean failed = false;
    if (connection != null) {
      try {
        connection.close();
      } catch (IOException e) {
        this.notifyFailed(e);
        failed = true;
      }
    }
    try {
      db.stopAndWait();
    } catch (Exception e) {
      this.notifyFailed(e);
      failed = true;
    }
    if (!failed) {
      notifyStopped();
    }
  }


  public Connection getConnection() {
    return connection;
  }


}
