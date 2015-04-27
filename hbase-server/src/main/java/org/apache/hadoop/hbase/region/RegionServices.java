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

import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.regionserver.CompactionRequestor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager;
import org.apache.hadoop.hbase.regionserver.OnlineRegions;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.ServerNonceManager;

/**
 * Services related to hosting regions.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface RegionServices extends OnlineRegions, Stoppable, Abortable {
  // TODO: Guava Service?

  /**
   * @return True if this region service is stopping.
   */
  boolean isStopping();

  /**
   * @return Return the FileSystem object used by the region service
   */
  FileSystem getFileSystem();

  /**
   * @return Implementation of {@link CompactionRequestor} or null.
   */
  CompactionRequestor getCompactionRequester();

  /**
   * @return Implementation of {@link FlushRequester} or null.
   */
  FlushRequester getFlushRequester();

  // TODO: RegionServiceAccounting?
  /**
   * @return the RegionServerAccounting for this Region Server
   */
  RegionServerAccounting getRegionServerAccounting();

  /**
   * @return heap memory manager instance
   */
  HeapMemoryManager getHeapMemoryManager();


  // TODO: HRegion should not depend on the below
  /**
   * Only required for "old" log replay; if it's removed, remove this.
   * @return The RegionServer's NonceManager
   */
  public ServerNonceManager getNonceManager();

  /**
   * @return set of recovering regions on the hosting region server
   */
  Map<String, Region> getRecoveringRegions();

}
