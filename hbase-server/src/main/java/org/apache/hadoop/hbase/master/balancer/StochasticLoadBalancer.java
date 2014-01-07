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
package org.apache.hadoop.hbase.master.balancer;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.Action;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.Action.Type;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.MoveRegionAction;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.SwapRegionsAction;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * <p>This is a best effort load balancer. Given a Cost function F(C) => x It will
 * randomly try and mutate the cluster to Cprime. If F(Cprime) < F(C) then the
 * new cluster state becomes the plan. It includes costs functions to compute the cost of:</p>
 * <ul>
 * <li>Region Load</li>
 * <li>Table Load</li>
 * <li>Data Locality</li>
 * <li>Memstore Sizes</li>
 * <li>Storefile Sizes</li>
 * </ul>
 *
 *
 * <p>Every cost function returns a number between 0 and 1 inclusive; where 0 is the lowest cost
 * best solution, and 1 is the highest possible cost and the worst solution.  The computed costs are
 * scaled by their respective multipliers:</p>
 *
 * <ul>
 *   <li>hbase.master.balancer.stochastic.regionLoadCost</li>
 *   <li>hbase.master.balancer.stochastic.moveCost</li>
 *   <li>hbase.master.balancer.stochastic.tableLoadCost</li>
 *   <li>hbase.master.balancer.stochastic.localityCost</li>
 *   <li>hbase.master.balancer.stochastic.memstoreSizeCost</li>
 *   <li>hbase.master.balancer.stochastic.storefileSizeCost</li>
 * </ul>
 *
 * <p>In addition to the above configurations, the balancer can be tuned by the following
 * configuration values:</p>
 * <ul>
 *   <li>hbase.master.balancer.stochastic.maxMoveRegions which
 *   controls what the max number of regions that can be moved in a single invocation of this
 *   balancer.</li>
 *   <li>hbase.master.balancer.stochastic.stepsPerRegion is the coefficient by which the number of
 *   regions is multiplied to try and get the number of times the balancer will
 *   mutate all servers.</li>
 *   <li>hbase.master.balancer.stochastic.maxSteps which controls the maximum number of times that
 *   the balancer will try and mutate all the servers. The balancer will use the minimum of this
 *   value and the above computation.</li>
 * </ul>
 *
 * <p>This balancer is best used with hbase.master.loadbalance.bytable set to false
 * so that the balancer gets the full picture of all loads on the cluster.</p>
 */
@InterfaceAudience.Private
public class StochasticLoadBalancer extends BaseLoadBalancer {

  protected static final String STEPS_PER_REGION_KEY =
      "hbase.master.balancer.stochastic.stepsPerRegion";
  protected static final String MAX_STEPS_KEY =
      "hbase.master.balancer.stochastic.maxSteps";
  protected static final String MAX_RUNNING_TIME_KEY =
      "hbase.master.balancer.stochastic.maxRunningTime";
  protected static final String KEEP_REGION_LOADS =
      "hbase.master.balancer.stochastic.numRegionLoadsToRemember";

  private static final Random RANDOM = new Random(System.currentTimeMillis());
  private static final Log LOG = LogFactory.getLog(StochasticLoadBalancer.class);

  private final RegionLocationFinder regionFinder = new RegionLocationFinder();
  private ClusterStatus clusterStatus = null;
  Map<String, Deque<RegionLoad>> loads = new HashMap<String, Deque<RegionLoad>>();

  // values are defaults
  private int maxSteps = 1000000;
  private int stepsPerRegion = 800;
  private long maxRunningTime = 30 * 1000 * 1; // 30 seconds.
  private int numRegionLoadsToRemember = 15;

  private CandidateGenerator[] candidateGenerators;
  private CostFromRegionLoadFunction[] regionLoadFunctions;
  private CostFunction[] costFunctions;
  // Keep locality based picker and cost function to alert them
  // when new services are offered
  private LocalityBasedCandidateGenerator localityCandidateGenerator;
  private LocalityCostFunction localityCost;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);

    regionFinder.setConf(conf);

    maxSteps = conf.getInt(MAX_STEPS_KEY, maxSteps);

    stepsPerRegion = conf.getInt(STEPS_PER_REGION_KEY, stepsPerRegion);
    maxRunningTime = conf.getLong(MAX_RUNNING_TIME_KEY, maxRunningTime);

    numRegionLoadsToRemember = conf.getInt(KEEP_REGION_LOADS, numRegionLoadsToRemember);

    localityCandidateGenerator = new LocalityBasedCandidateGenerator(services);
    localityCost = new LocalityCostFunction(conf, services);

    candidateGenerators = new CandidateGenerator[] {
      new RandomCandidateGenerator(),
      new LoadCandidateGenerator(),
      localityCandidateGenerator,
      new RegionReplicaHostCandidateGenerator(),
    };

    regionLoadFunctions = new CostFromRegionLoadFunction[] {
      new ReadRequestCostFunction(conf),
      new WriteRequestCostFunction(conf),
      new MemstoreSizeCostFunction(conf),
      new StoreFileCostFunction(conf)
    };

    costFunctions = new CostFunction[]{
      new RegionCountSkewCostFunction(conf),
      new MoveCostFunction(conf),
      localityCost,
      new TableSkewCostFunction(conf),
      new RegionReplicaHostCostFunction(conf),
      regionLoadFunctions[0],
      regionLoadFunctions[1],
      regionLoadFunctions[2],
      regionLoadFunctions[3],
    };
  }

  @Override
  protected void setSlop(Configuration conf) {
    this.slop = conf.getFloat("hbase.regions.slop", 0.001F);
  }

  @Override
  public void setClusterStatus(ClusterStatus st) {
    super.setClusterStatus(st);
    regionFinder.setClusterStatus(st);
    this.clusterStatus = st;
    updateRegionLoad();
    for(CostFromRegionLoadFunction cost : regionLoadFunctions) {
      cost.setClusterStatus(st);
    }
  }

  @Override
  public void setMasterServices(MasterServices masterServices) {
    super.setMasterServices(masterServices);
    this.regionFinder.setServices(masterServices);
    this.localityCost.setServices(masterServices);
    this.localityCandidateGenerator.setServices(masterServices);

  }

  /**
   * Given the cluster state this will try and approach an optimal balance. This
   * should always approach the optimal state given enough steps.
   */
  @Override
  public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState) {
    //The clusterState that is given to this method contains the state
    //of all the regions in the table(s) (that's true today)
    if (!needsBalance(new ClusterLoadState(clusterState))) {
      return null;
    }

    long startTime = EnvironmentEdgeManager.currentTimeMillis();

    // Keep track of servers to iterate through them.
    Cluster cluster = new Cluster(clusterState, loads, regionFinder, rackManager);
    initCosts(cluster);

    double currentCost = computeCost(cluster, Double.MAX_VALUE);

    double initCost = currentCost;
    double newCost = currentCost;

    long computedMaxSteps = Math.min(this.maxSteps,
        ((long)cluster.numRegions * (long)this.stepsPerRegion * (long)cluster.numServers));
    // Perform a stochastic walk to see if we can get a good fit.
    long step;

    for (step = 0; step < computedMaxSteps; step++) {
      int generatorIdx = RANDOM.nextInt(candidateGenerators.length);
      CandidateGenerator p = candidateGenerators[generatorIdx];
      Cluster.Action action = p.generate(cluster);

      if (action.type == Type.NULL) {
        continue;
      }

      cluster.doAction(action);
      updateCostsWithAction(cluster, action);

      newCost = computeCost(cluster, currentCost);

      // Should this be kept?
      if (newCost < currentCost) {
        currentCost = newCost;
      } else {
        // Put things back the way they were before.
        // TODO: undo by remembering old values
        Action undoAction = action.undoAction();
        cluster.doAction(undoAction);
        updateCostsWithAction(cluster, undoAction);
      }

      if (EnvironmentEdgeManager.currentTimeMillis() - startTime >
          maxRunningTime) {
        break;
      }
    }

    long endTime = EnvironmentEdgeManager.currentTimeMillis();

    metricsBalancer.balanceCluster(endTime - startTime);

    if (initCost > currentCost) {
      List<RegionPlan> plans = createRegionPlans(cluster);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Finished computing new load balance plan.  Computation took "
            + (endTime - startTime) + "ms to try " + step
            + " different iterations.  Found a solution that moves "
            + plans.size() + " regions; Going from a computed cost of "
            + initCost + " to a new cost of " + currentCost);
      }
      return plans;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Could not find a better load balance plan.  Tried "
          + step + " different configurations in " + (endTime - startTime)
          + "ms, and did not find anything with a computed cost less than " + initCost);
    }
    return null;
  }

  /**
   * Create all of the RegionPlan's needed to move from the initial cluster state to the desired
   * state.
   *
   * @param cluster The state of the cluster
   * @return List of RegionPlan's that represent the moves needed to get to desired final state.
   */
  private List<RegionPlan> createRegionPlans(Cluster cluster) {
    List<RegionPlan> plans = new LinkedList<RegionPlan>();
    for (int regionIndex = 0;
         regionIndex < cluster.regionIndexToServerIndex.length; regionIndex++) {
      int initialServerIndex = cluster.initialRegionIndexToServerIndex[regionIndex];
      int newServerIndex = cluster.regionIndexToServerIndex[regionIndex];

      if (initialServerIndex != newServerIndex) {
        HRegionInfo region = cluster.regions[regionIndex];
        ServerName initialServer = cluster.servers[initialServerIndex];
        ServerName newServer = cluster.servers[newServerIndex];

        if (LOG.isTraceEnabled()) {
          LOG.trace("Moving Region " + region.getEncodedName() + " from server "
              + initialServer.getHostname() + " to " + newServer.getHostname());
        }
        RegionPlan rp = new RegionPlan(region, initialServer, newServer);
        plans.add(rp);
      }
    }
    return plans;
  }

  /**
   * Store the current region loads.
   */
  private synchronized void updateRegionLoad() {
    // We create a new hashmap so that regions that are no longer there are removed.
    // However we temporarily need the old loads so we can use them to keep the rolling average.
    Map<String, Deque<RegionLoad>> oldLoads = loads;
    loads = new HashMap<String, Deque<RegionLoad>>();

    for (ServerName sn : clusterStatus.getServers()) {
      ServerLoad sl = clusterStatus.getLoad(sn);
      if (sl == null) {
        continue;
      }
      for (Entry<byte[], RegionLoad> entry : sl.getRegionsLoad().entrySet()) {
        Deque<RegionLoad> rLoads = oldLoads.get(Bytes.toString(entry.getKey()));
        if (rLoads == null) {
          // There was nothing there
          rLoads = new ArrayDeque<RegionLoad>();
        } else if (rLoads.size() >= 15) {
          rLoads.remove();
        }
        rLoads.add(entry.getValue());
        loads.put(Bytes.toString(entry.getKey()), rLoads);

      }
    }

    for(CostFromRegionLoadFunction cost : regionLoadFunctions) {
      cost.setLoads(loads);
    }
  }

  protected void initCosts(Cluster cluster) {
    for (CostFunction c:costFunctions) {
      c.init(cluster);
    }
  }

  protected void updateCostsWithAction(Cluster cluster, Action action) {
    for (CostFunction c:costFunctions) {
      c.postAction(cluster, action);
    }
  }

  /**
   * This is the main cost function.  It will compute a cost associated with a proposed cluster
   * state.  All different costs will be combined with their multipliers to produce a double cost.
   *
   * @param cluster The state of the cluster
   * @param previousCost the previous cost. This is used as an early out.
   * @return a double of a cost associated with the proposed cluster state.  This cost is an
   *         aggregate of all individual cost functions.
   */
  protected double computeCost(Cluster cluster, double previousCost) {
    double total = 0;

    for (CostFunction c:costFunctions) {
      if (c.getMultiplier() <= 0) {
        continue;
      }

      total += c.getMultiplier() * c.cost(cluster);

      if (total > previousCost) {
        return total;
      }
    }
    return total;
  }

  /** Generates a candidate action to be applied to the cluster for cost function search */
  abstract static class CandidateGenerator {
    abstract Cluster.Action generate(Cluster cluster);

    /**
     * From a list of regions pick a random one. Null can be returned which
     * {@link StochasticLoadBalancer#balanceCluster(Map)} recognize as signal to try a region move
     * rather than swap.
     *
     * @param cluster        The state of the cluster
     * @param server         index of the server
     * @param chanceOfNoSwap Chance that this will decide to try a move rather
     *                       than a swap.
     * @return a random {@link HRegionInfo} or null if an asymmetrical move is
     *         suggested.
     */
    protected int pickRandomRegion(Cluster cluster, int server, double chanceOfNoSwap) {
      // Check to see if this is just a move.
      if (cluster.regionsPerServer[server].length == 0 || RANDOM.nextFloat() < chanceOfNoSwap) {
        // signal a move only.
        return -1;
      }
      int rand = RANDOM.nextInt(cluster.regionsPerServer[server].length);
      return cluster.regionsPerServer[server][rand];

    }
    protected int pickRandomServer(Cluster cluster) {
      if (cluster.numServers < 1) {
        return -1;
      }

      return RANDOM.nextInt(cluster.numServers);
    }

    protected int pickRandomHost(Cluster cluster) {
      if (cluster.numHosts < 1) {
        return -1;
      }

      return RANDOM.nextInt(cluster.numHosts);
    }

    protected int pickOtherRandomServer(Cluster cluster, int serverIndex) {
      if (cluster.numServers < 2) {
        return -1;
      }
      while (true) {
        int otherServerIndex = pickRandomServer(cluster);
        if (otherServerIndex != serverIndex) {
          return otherServerIndex;
        }
      }
    }

    protected int pickOtherRandomHost(Cluster cluster, int hostIndex) {
      if (cluster.numHosts < 2) {
        return -1;
      }
      while (true) {
        int otherHostIndex = pickRandomHost(cluster);
        if (otherHostIndex != hostIndex) {
          return otherHostIndex;
        }
      }
    }

    protected Cluster.Action pickRandomRegions(Cluster cluster,
                                                       int thisServer,
                                                       int otherServer) {
      if (thisServer < 0 || otherServer < 0) {
        return Cluster.NullAction;
      }

      // Decide who is most likely to need another region
      int thisRegionCount = cluster.getNumRegions(thisServer);
      int otherRegionCount = cluster.getNumRegions(otherServer);

      // Assign the chance based upon the above
      double thisChance = (thisRegionCount > otherRegionCount) ? 0 : 0.5;
      double otherChance = (thisRegionCount <= otherRegionCount) ? 0 : 0.5;

      int thisRegion = pickRandomRegion(cluster, thisServer, thisChance);
      int otherRegion = pickRandomRegion(cluster, otherServer, otherChance);

      return getAction(thisServer, thisRegion, otherServer, otherRegion);
    }

    protected Cluster.Action getAction (int fromServer, int fromRegion,
        int toServer, int toRegion) {
      if (fromServer < 0 || toServer < 0) {
        return Cluster.NullAction;
      }
      if (fromRegion > 0 && toRegion > 0) {
        return new Cluster.SwapRegionsAction(fromServer, fromRegion,
          toServer, toRegion);
      } else if (fromRegion > 0) {
        return new Cluster.MoveRegionAction(fromRegion, fromServer, toServer);
      } else if (toRegion > 0) {
        return new Cluster.MoveRegionAction(toRegion, toServer, fromServer);
      } else {
        return Cluster.NullAction;
      }
    }
  }

  static class RandomCandidateGenerator extends CandidateGenerator {

    @Override
    Cluster.Action generate(Cluster cluster) {

      int thisServer = pickRandomServer(cluster);

      // Pick the other server
      int otherServer = pickOtherRandomServer(cluster, thisServer);

      return pickRandomRegions(cluster, thisServer, otherServer);
    }
  }

  public static class LoadCandidateGenerator extends CandidateGenerator {

    @Override
    Cluster.Action generate(Cluster cluster) {
      cluster.sortServersByRegionCount();
      int thisServer = pickMostLoadedServer(cluster, -1);
      int otherServer = pickLeastLoadedServer(cluster, thisServer);

      return pickRandomRegions(cluster, thisServer, otherServer);
    }

    private int pickLeastLoadedServer(final Cluster cluster, int thisServer) {
      Integer[] servers = cluster.serverIndicesSortedByRegionCount;

      int index = 0;
      while (servers[index] == null || servers[index] == thisServer) {
        index++;
        if (index == servers.length) {
          return -1;
        }
      }
      return servers[index];
    }

    private int pickMostLoadedServer(final Cluster cluster, int thisServer) {
      Integer[] servers = cluster.serverIndicesSortedByRegionCount;

      int index = servers.length - 1;
      while (servers[index] == null || servers[index] == thisServer) {
        index--;
        if (index < 0) {
          return -1;
        }
      }
      return servers[index];
    }
  }

  static class LocalityBasedCandidateGenerator extends CandidateGenerator {

    private MasterServices masterServices;

    LocalityBasedCandidateGenerator(MasterServices masterServices) {
      this.masterServices = masterServices;
    }

    @Override
    Cluster.Action generate(Cluster cluster) {
      if (this.masterServices == null) {
        return Cluster.NullAction;
      }
      // Pick a random region server
      int thisServer = pickRandomServer(cluster);

      // Pick a random region on this server
      int thisRegion = pickRandomRegion(cluster, thisServer, 0.0f);

      if (thisRegion == -1) {
        return Cluster.NullAction;
      }

      // Pick the server with the highest locality
      int otherServer = pickHighestLocalityServer(cluster, thisServer, thisRegion);

      // pick an region on the other server to potentially swap
      int otherRegion = this.pickRandomRegion(cluster, otherServer, 0.5f);

      return getAction(thisServer, thisRegion, otherServer, otherRegion);
    }

    private int pickHighestLocalityServer(Cluster cluster, int thisServer, int thisRegion) {
      int[] regionLocations = cluster.regionLocations[thisRegion];

      if (regionLocations == null || regionLocations.length <= 1) {
        return pickOtherRandomServer(cluster, thisServer);
      }

      int idx = 0;

      while (idx < regionLocations.length && regionLocations[idx] == thisServer) {
        idx++;
      }

      return idx;
    }

    void setServices(MasterServices services) {
      this.masterServices = services;
    }
  }

  /**
   * Generates candidates which moves the replicas out of the region server for
   * co-hosted region replicas
   */
  public static class RegionReplicaHostCandidateGenerator extends CandidateGenerator {

    RandomCandidateGenerator randomGenerator = new RandomCandidateGenerator();

    @Override
    Cluster.Action generate(Cluster cluster) {

      int hostIndex = pickRandomHost(cluster);

      if (cluster.numHosts <= 1 || hostIndex == -1) {
        return Cluster.NullAction;
      }

      // find the regions in the host with its replicas in the same host
      int currentPrimary = -1;
      int currentPrimaryIndex = -1;
      int numRegionsWithReplica = 0;
      for (int j = 0; j <= cluster.regionsByPrimaryPerHost[hostIndex].length; j++) {
        int primary = j < cluster.regionsByPrimaryPerHost[hostIndex].length ? cluster.regionsByPrimaryPerHost[hostIndex][j] : -1;
        if (primary != currentPrimary) {
          int numReplicas = j - currentPrimaryIndex;
          if (numReplicas > 1) {
            numRegionsWithReplica++;
          }

          currentPrimary = primary;
          currentPrimaryIndex = j;
        }
      }

      // randomly select one primaryIndex out of all region replicas in the same host
      currentPrimary = -1;
      currentPrimaryIndex = -1;
      int primaryIndex = -1;
      if (numRegionsWithReplica > 0) {
        int randomIndex = RANDOM.nextInt(numRegionsWithReplica);
        int index = 0;
        for (int j = 0; j <= cluster.regionsByPrimaryPerHost[hostIndex].length; j++) {
          int primary = j < cluster.regionsByPrimaryPerHost[hostIndex].length ? cluster.regionsByPrimaryPerHost[hostIndex][j] : -1;
          if (primary != currentPrimary) {
            int numReplicas = j - currentPrimaryIndex;
            if (numReplicas > 1) {
              if (index++ == randomIndex) {
                primaryIndex = currentPrimary;
                break;
              }
            }

            currentPrimary = primary;
            currentPrimaryIndex = j;
          }
        }
      }

      if (primaryIndex == -1) {
        // default to randompicker
        return randomGenerator.generate(cluster);
      }

      int regionIndex = -1;
      int serverIndex = -1;
      // move the region replica out of the host
      for (int j = 0; j < cluster.serversPerHost[hostIndex].length; j++) {
        for (int k = 0; k < cluster.regionsPerServer[cluster.serversPerHost[hostIndex][j]].length; k++) {
          int region = cluster.regionsPerServer[cluster.serversPerHost[hostIndex][j]][k];
          if (primaryIndex == cluster.regionIndexToPrimaryIndex[region]) {
            // always move the secondary, not the primary
            if (!cluster.regions[region].isPrimaryReplica()) {
              regionIndex = region;
              serverIndex = cluster.serversPerHost[hostIndex][j];
              break;
            }
          }
        }
      }

      int toHostIndex = pickOtherRandomHost(cluster, hostIndex);
      int toServerIndex = cluster.serversPerHost[toHostIndex]
          [RANDOM.nextInt(cluster.serversPerHost[toHostIndex].length)];

      int toRegionIndex = pickRandomRegion(cluster, toServerIndex, 0.9f);

      return getAction (serverIndex, regionIndex, toServerIndex, toRegionIndex);
    }
  }

  /**
   * Base class of StochasticLoadBalancer's Cost Functions.
   */
  public abstract static class CostFunction {

    private float multiplier = 0;
    private Configuration conf;

    CostFunction(Configuration c) {
      this.conf = c;
    }

    float getMultiplier() {
      return multiplier;
    }

    void setMultiplier(float m) {
      this.multiplier = m;
    }

    /** Called once per LB invocation to give the cost function
     * to initialize it's state, and perform any costly calculation.
     */
    void init(Cluster cluster) { }

    /** Called once per cluster Action to give the cost function
     * an opportunity to update it's state. postAction() is always
     * called at least once before cost() is called with the cluster
     * that this action is performed on. */
    void postAction(Cluster cluster, Action action) {
      switch (action.type) {
      case NULL: break;
      case MOVE_REGION:
        MoveRegionAction mra = (MoveRegionAction) action;
        regionMoved(cluster, mra.region, mra.fromServer, mra.toServer);
        break;
      case SWAP_REGIONS:
        SwapRegionsAction a = (SwapRegionsAction) action;
        regionMoved(cluster, a.fromRegion, a.fromServer, a.toServer);
        regionMoved(cluster, a.toRegion, a.toServer, a.fromServer);
        break;
      default:
        throw new RuntimeException("Uknown action:" + action.type);
      }
    }
    protected void regionMoved(Cluster cluster, int region, int oldServer, int newServer) {
    }

    abstract double cost(Cluster cluster);

    /**
     * Function to compute a scaled cost using {@link DescriptiveStatistics}. It
     * assumes that this is a zero sum set of costs.  It assumes that the worst case
     * possible is all of the elements in one region server and the rest having 0.
     *
     * @param stats the costs
     * @return a scaled set of costs.
     */
    protected double costFromArray(double[] stats) {
      double totalCost = 0;
      double total = getSum(stats);
      double mean = total/((double)stats.length);
      double count = stats.length;

      // Compute max as if all region servers had 0 and one had the sum of all costs.  This must be
      // a zero sum cost for this to make sense.
      // TODO: Should we make this sum of square errors?
      double max = ((count - 1) * mean) + (total - mean);
      for (double n : stats) {
        double diff = Math.abs(mean - n);
        totalCost += diff;
      }

      double scaled =  scale(0, max, totalCost);
      return scaled;
    }

    private double getSum(double[] stats) {
      double total = 0;
      for(double s:stats) {
        total += s;
      }
      return total;
    }

    /**
     * Scale the value between 0 and 1.
     *
     * @param min   Min value
     * @param max   The Max value
     * @param value The value to be scaled.
     * @return The scaled value.
     */
    protected double scale(double min, double max, double value) {
      if (max == 0 || value == 0) {
        return 0;
      }

      return Math.max(0d, Math.min(1d, (value - min) / max));
    }
  }

  /**
   * Given the starting state of the regions and a potential ending state
   * compute cost based upon the number of regions that have moved.
   */
  public static class MoveCostFunction extends CostFunction {
    private static final String MOVE_COST_KEY = "hbase.master.balancer.stochastic.moveCost";
    private static final String MAX_MOVES_PERCENT_KEY =
        "hbase.master.balancer.stochastic.maxMovePercent";
    private static final float DEFAULT_MOVE_COST = 100;
    private static final int DEFAULT_MAX_MOVES = 600;
    private static final float DEFAULT_MAX_MOVE_PERCENT = 0.25f;
    private static final int META_MOVE_COST_MULT = 10;

    private final float maxMovesPercent;

    MoveCostFunction(Configuration conf) {
      super(conf);

      // Move cost multiplier should be the same cost or higher than the rest of the costs to ensure
      // that large benefits are need to overcome the cost of a move.
      this.setMultiplier(conf.getFloat(MOVE_COST_KEY, DEFAULT_MOVE_COST));
      // What percent of the number of regions a single run of the balancer can move.
      maxMovesPercent = conf.getFloat(MAX_MOVES_PERCENT_KEY, DEFAULT_MAX_MOVE_PERCENT);
    }

    @Override
    double cost(Cluster cluster) {
      // Try and size the max number of Moves, but always be prepared to move some.
      int maxMoves = Math.max((int) (cluster.numRegions * maxMovesPercent),
          DEFAULT_MAX_MOVES);

      double moveCost = cluster.numMovedRegions;

      // Don't let this single balance move more than the max moves.
      // This allows better scaling to accurately represent the actual cost of a move.
      if (moveCost > maxMoves) {
        return 1000000;   // return a number much greater than any of the other cost
      }

      // hbase:meta region is special
      if (cluster.numMovedMetaRegions > 0) {
        // assume each hbase:meta region move costs 10 times
        moveCost += META_MOVE_COST_MULT * cluster.numMovedMetaRegions;
      }

      return scale(0, cluster.numRegions + META_MOVE_COST_MULT, moveCost);
    }
  }

  /**
   * Compute the cost of a potential cluster state from skew in number of
   * regions on a cluster.
   */
  public static class RegionCountSkewCostFunction extends CostFunction {
    private static final String REGION_COUNT_SKEW_COST_KEY =
        "hbase.master.balancer.stochastic.regionCountCost";
    private static final float DEFAULT_REGION_COUNT_SKEW_COST = 500;

    private double[] stats = null;

    RegionCountSkewCostFunction(Configuration conf) {
      super(conf);
      // Load multiplier should be the greatest as it is the most general way to balance data.
      this.setMultiplier(conf.getFloat(REGION_COUNT_SKEW_COST_KEY, DEFAULT_REGION_COUNT_SKEW_COST));
    }

    @Override
    double cost(Cluster cluster) {
      if (stats == null || stats.length != cluster.numServers) {
        stats = new double[cluster.numServers];
      }

      for (int i =0; i < cluster.numServers; i++) {
        stats[i] = cluster.regionsPerServer[i].length;
      }
      return costFromArray(stats);
    }
  }

  /**
   * Compute the cost of a potential cluster configuration based upon how evenly
   * distributed tables are.
   */
  public static class TableSkewCostFunction extends CostFunction {

    private static final String TABLE_SKEW_COST_KEY =
        "hbase.master.balancer.stochastic.tableSkewCost";
    private static final float DEFAULT_TABLE_SKEW_COST = 35;

    TableSkewCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(TABLE_SKEW_COST_KEY, DEFAULT_TABLE_SKEW_COST));
    }

    @Override
    double cost(Cluster cluster) {
      double max = cluster.numRegions;
      double min = cluster.numRegions / cluster.numServers;
      double value = 0;

      for (int i = 0; i < cluster.numMaxRegionsPerTable.length; i++) {
        value += cluster.numMaxRegionsPerTable[i];
      }

      return scale(min, max, value);
    }
  }


  /**
   * Compute a cost of a potential cluster configuration based upon where
   * {@link org.apache.hadoop.hbase.regionserver.StoreFile}s are located.
   */
  public static class LocalityCostFunction extends CostFunction {

    private static final String LOCALITY_COST_KEY = "hbase.master.balancer.stochastic.localityCost";
    private static final float DEFAULT_LOCALITY_COST = 25;

    private MasterServices services;

    LocalityCostFunction(Configuration conf, MasterServices srv) {
      super(conf);
      this.setMultiplier(conf.getFloat(LOCALITY_COST_KEY, DEFAULT_LOCALITY_COST));
      this.services = srv;
    }

    void setServices(MasterServices srvc) {
      this.services = srvc;
    }

    @Override
    double cost(Cluster cluster) {
      double max = 0;
      double cost = 0;

      // If there's no master so there's no way anything else works.
      if (this.services == null) {
        return cost;
      }

      for (int i = 0; i < cluster.regionLocations.length; i++) {
        max += 1;
        int serverIndex = cluster.regionIndexToServerIndex[i];
        int[] regionLocations = cluster.regionLocations[i];

        // If we can't find where the data is getTopBlock returns null.
        // so count that as being the best possible.
        if (regionLocations == null) {
          continue;
        }

        int index = -1;
        for (int j = 0; j < regionLocations.length; j++) {
          if (regionLocations[j] >= 0 && regionLocations[j] == serverIndex) {
            index = j;
            break;
          }
        }

        if (index < 0) {
          cost += 1;
        } else {
          cost += (double) index / (double) regionLocations.length;
        }
      }
      return scale(0, max, cost);
    }
  }

  /**
   * Base class the allows writing costs functions from rolling average of some
   * number from RegionLoad.
   */
  public abstract static class CostFromRegionLoadFunction extends CostFunction {

    private ClusterStatus clusterStatus = null;
    private Map<String, Deque<RegionLoad>> loads = null;
    private double[] stats = null;
    CostFromRegionLoadFunction(Configuration conf) {
      super(conf);
    }

    void setClusterStatus(ClusterStatus status) {
      this.clusterStatus = status;
    }

    void setLoads(Map<String, Deque<RegionLoad>> l) {
      this.loads = l;
    }


    @Override
    double cost(Cluster cluster) {
      if (clusterStatus == null || loads == null) {
        return 0;
      }

      if (stats == null || stats.length != cluster.numServers) {
        stats = new double[cluster.numServers];
      }

      for (int i =0; i < stats.length; i++) {
        //Cost this server has from RegionLoad
        long cost = 0;

        // for every region on this server get the rl
        for(int regionIndex:cluster.regionsPerServer[i]) {
          Collection<RegionLoad> regionLoadList =  cluster.regionLoads[regionIndex];

          // Now if we found a region load get the type of cost that was requested.
          if (regionLoadList != null) {
            cost += getRegionLoadCost(regionLoadList);
          }
        }

        // Add the total cost to the stats.
        stats[i] = cost;
      }

      // Now return the scaled cost from data held in the stats object.
      return costFromArray(stats);
    }

    protected double getRegionLoadCost(Collection<RegionLoad> regionLoadList) {
      double cost = 0;

      for (RegionLoad rl : regionLoadList) {
        double toAdd = getCostFromRl(rl);

        if (cost == 0) {
          cost = toAdd;
        } else {
          cost = (.5 * cost) + (.5 * toAdd);
        }
      }

      return cost;
    }

    protected abstract double getCostFromRl(RegionLoad rl);
  }

  /**
   * Compute the cost of total number of read requests  The more unbalanced the higher the
   * computed cost will be.  This uses a rolling average of regionload.
   */

  public static class ReadRequestCostFunction extends CostFromRegionLoadFunction {

    private static final String READ_REQUEST_COST_KEY =
        "hbase.master.balancer.stochastic.readRequestCost";
    private static final float DEFAULT_READ_REQUEST_COST = 5;

    ReadRequestCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(READ_REQUEST_COST_KEY, DEFAULT_READ_REQUEST_COST));
    }


    @Override
    protected double getCostFromRl(RegionLoad rl) {
      return rl.getReadRequestsCount();
    }
  }

  /**
   * Compute the cost of total number of write requests.  The more unbalanced the higher the
   * computed cost will be.  This uses a rolling average of regionload.
   */
  public static class WriteRequestCostFunction extends CostFromRegionLoadFunction {

    private static final String WRITE_REQUEST_COST_KEY =
        "hbase.master.balancer.stochastic.writeRequestCost";
    private static final float DEFAULT_WRITE_REQUEST_COST = 5;

    WriteRequestCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(WRITE_REQUEST_COST_KEY, DEFAULT_WRITE_REQUEST_COST));
    }

    @Override
    protected double getCostFromRl(RegionLoad rl) {
      return rl.getWriteRequestsCount();
    }
  }

  /**
   * Compute the cost of total memstore size.  The more unbalanced the higher the
   * computed cost will be.  This uses a rolling average of regionload.
   */
  public static class MemstoreSizeCostFunction extends CostFromRegionLoadFunction {

    private static final String MEMSTORE_SIZE_COST_KEY =
        "hbase.master.balancer.stochastic.memstoreSizeCost";
    private static final float DEFAULT_MEMSTORE_SIZE_COST = 5;

    MemstoreSizeCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(MEMSTORE_SIZE_COST_KEY, DEFAULT_MEMSTORE_SIZE_COST));
    }

    @Override
    protected double getCostFromRl(RegionLoad rl) {
      return rl.getMemStoreSizeMB();
    }
  }
  /**
   * Compute the cost of total open storefiles size.  The more unbalanced the higher the
   * computed cost will be.  This uses a rolling average of regionload.
   */
  public static class StoreFileCostFunction extends CostFromRegionLoadFunction {

    private static final String STOREFILE_SIZE_COST_KEY =
        "hbase.master.balancer.stochastic.storefileSizeCost";
    private static final float DEFAULT_STOREFILE_SIZE_COST = 5;

    StoreFileCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(STOREFILE_SIZE_COST_KEY, DEFAULT_STOREFILE_SIZE_COST));
    }

    @Override
    protected double getCostFromRl(RegionLoad rl) {
      return rl.getStorefileSizeMB();
    }
  }

  /**
   * A cost function for region replicas. We give a very high cost to hosting
   * replicas of the same region in the same server. We do not prevent the case
   * though, since if numReplicas > numRegionServers, we still want to keep the
   * replica open.
   */
  public static class RegionReplicaHostCostFunction extends CostFunction {
    private static final String REGION_REPLICA_HOST_COST_KEY =
        "hbase.master.balancer.stochastic.regionReplicaHostCostKey";
    private static final float DEFAULT_REGION_REPLICA_HOST_COST_KEY = 100000000;

    long maxCost = 0;
    long[] costsPerHost;

    public RegionReplicaHostCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(REGION_REPLICA_HOST_COST_KEY, DEFAULT_REGION_REPLICA_HOST_COST_KEY));
    }

    @Override
    void init(Cluster cluster) {
      super.init(cluster);
      // max cost is the case where every region replica is hosted together regardless of host
      int[] regionsByPrimary = new int[cluster.numRegions];
      for (int i = 0; i < cluster.regions.length; i++) {
        int primaryIndex = cluster.regionIndexToPrimaryIndex[i];
        regionsByPrimary[i] = primaryIndex;
      }

      Arrays.sort(regionsByPrimary);

      // compute numReplicas from the sorted array
      maxCost = costPerHost(regionsByPrimary);
      costsPerHost = new long[cluster.numHosts];
      for (int i = 0 ; i < cluster.regionsByPrimaryPerHost.length; i++) {
        costsPerHost[i] = costPerHost(cluster.regionsByPrimaryPerHost[i]);
      }
    }

    @Override
    double cost(Cluster cluster) {
      if (maxCost <= 0) {
        return 0;
      }

      long totalCost = 0;
      for (int i = 0 ; i < costsPerHost.length; i++) {
        totalCost += costsPerHost[i];
      }
      return scale(0, maxCost, totalCost);
    }

    protected long costPerHost(int[] regionsByPrimary) {
      long cost = 0;
      int currentPrimary = -1;
      int currentPrimaryIndex = -1;
      for (int j = 0 ; j <= regionsByPrimary.length; j++) {
        int primary = j < regionsByPrimary.length ? regionsByPrimary[j] : -1;
        if (primary != currentPrimary) {
          int numReplicas = j - currentPrimaryIndex;
          // square the cost
          cost += (numReplicas - 1) * (numReplicas - 1);
          currentPrimary = primary;
          currentPrimaryIndex = j;
        }
      }

      return cost;
    }

    @Override
    protected void regionMoved(Cluster cluster, int region, int oldServer, int newServer) {
      int oldHost = cluster.serverIndexToHostIndex[oldServer];
      int newHost = cluster.serverIndexToHostIndex[newServer];
      costsPerHost[oldHost] = costPerHost(cluster.regionsByPrimaryPerHost[oldHost]);
      costsPerHost[newHost] = costPerHost(cluster.regionsByPrimaryPerHost[newHost]);
    }

  }

  //TODO RegionReplicaRackCostFunction
}
