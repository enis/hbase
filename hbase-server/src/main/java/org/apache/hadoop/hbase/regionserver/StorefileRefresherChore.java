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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.Stoppable;

/**
 * A chore for refreshing the store files for seondary regions hosted in the region server
 */
public class StorefileRefresherChore extends Chore {

  private static final Log LOG = LogFactory.getLog(StorefileRefresherChore.class);

  private HRegionServer regionServer;

  public StorefileRefresherChore(int period, HRegionServer regionServer, Stoppable stoppable) {
    super("StorefileRefresherChore", period, stoppable);
    this.regionServer = regionServer;
  }

  @Override
  protected void chore() {
    for (HRegion r : regionServer.onlineRegions.values()) {
      if (r.getRegionInfo().isPrimaryReplica()) {
        continue;
      }

      for (Store store : r.getStores().values()) {
        // TODO: some stores might see new data, while others
        // do not in case of flushes which MIGHT break atomic edits across column families

        try {
          store.refreshStoreFiles();
        } catch (IOException ex) {
          LOG.warn("Exception while trying to refresh store files for region:" + r.getRegionInfo());
          LOG.warn(ex);
          // TODO: Store files have a TTL in the archive directory. If we fail to refresh for that long,
          // we should abort the RS or close the secondary region
        }
      }
    }
  }
}
