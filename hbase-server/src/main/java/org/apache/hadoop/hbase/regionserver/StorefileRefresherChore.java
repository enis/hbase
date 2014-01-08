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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.util.StringUtils;

/**
 * A chore for refreshing the store files for seondary regions hosted in the region server
 */
public class StorefileRefresherChore extends Chore {

  private static final Log LOG = LogFactory.getLog(StorefileRefresherChore.class);

  private HRegionServer regionServer;
  private long hfileTtl;
  private int period;

  public StorefileRefresherChore(int period, HRegionServer regionServer, Stoppable stoppable) {
    super("StorefileRefresherChore", period, stoppable);
    this.period = period;
    this.regionServer = regionServer;
    this.hfileTtl = this.regionServer.getConfiguration().getLong(
      TimeToLiveHFileCleaner.TTL_CONF_KEY, TimeToLiveHFileCleaner.DEFAULT_TTL);
    if (period > hfileTtl / 2) {
      throw new RuntimeException(HConstants.REGIONSERVER_STOREFILE_REFRESH_PERIOD +
        " should be set smaller than half of " + TimeToLiveHFileCleaner.TTL_CONF_KEY);
    }
  }

  @Override
  protected void chore() {
    for (HRegion r : regionServer.getOnlineRegionsLocalContext()) {
      if (r.getRegionInfo().isPrimaryReplica()) {
        continue;
      }
      long time = System.currentTimeMillis();
      try {
        for (Store store : r.getStores().values()) {
          // TODO: some stores might see new data, while others
          // do not in case of flushes which MIGHT break atomic edits across column families
            store.refreshStoreFiles();
        }
      } catch (IOException ex) {
        LOG.warn("Exception while trying to refresh store files for region:" + r.getRegionInfo()
          + ", exception:" + StringUtils.stringifyException(ex));
        // TODO: Store files have a TTL in the archive directory. If we fail to refresh for that long, we stop serving reads
        if (isRegionStale(r)) {
          r.setStale(true); // stop serving reads
        }
        continue;
      }
      r.setLastFileRefreshTime(time);
      r.setStale(false);
    }
  }

  protected boolean isRegionStale(HRegion r) {
    return System.currentTimeMillis() - r.getLastFileRefreshTime() > hfileTtl - 2 * period;
  }
}
