package edu.umd.lib.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Custom HDFS Block Placement Policy
 * 
 * This is an experimental policy for controlling the block placement of
 * files stored in HDFS for the fedora commons repository. 
 * 
 * First block request for each file (srcPath) is allocated using the default
 * block placement policy.
 * 
 * In subsequent requests for additional blocks for the same file (srcPath),
 * the same set of datanodes that were allocated for the first block is chosen
 * again.  
 * 
 * 
 * @author mohideen
 *
 */

public class BlockPlacementPolicyFedora extends BlockPlacementPolicyDefault {
  
  int cacheSize = 10000;
  private NetworkTopology clusterMap;
  String enforcementPath = "/fedora/dsfedora";
  
  /**
   * A temporary cache for node locations of a file. The cache entry
   * only needs to be available until the client finishes writing the
   * file. 
   * 
   * Note: The cache currently stores the datanode descriptor objects 
   * in memory. This should be optimized so that only the datanode
   * references are stored in memory, and the descriptor object should be
   * retrieved while serving the requests.
   * 
   */
  private Cache<String, DatanodeDescriptor[]> pathNodeMap = CacheBuilder.newBuilder()
      .maximumSize(cacheSize).build();
  
  @Override
  public void initialize(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap) {
    this.clusterMap = clusterMap;
    super.initialize(conf, stats, clusterMap);
  }

  @Override
  public DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
      long blocksize) {
    return chooseTargetInternal(srcPath, numOfReplicas, writer, chosenNodes, 
        false, null, blocksize);
  }

  @Override
  public DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
      boolean returnChosenNodes, HashMap<Node, Node> excludedNodes,
      long blocksize) {
    return chooseTargetInternal(srcPath, numOfReplicas, writer, chosenNodes, 
        returnChosenNodes, excludedNodes, blocksize);
  }
  
  /**
   * Custom Policy Implementation
   */
  DatanodeDescriptor[] chooseTargetInternal(String srcPath, int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
      boolean returnChosenNodes, HashMap<Node, Node> excludedNodes,
      long blocksize) {
    
    if(!srcPath.startsWith(enforcementPath)) {
      return super.chooseTarget(srcPath, numOfReplicas, writer, 
          chosenNodes, returnChosenNodes, excludedNodes, blocksize);
    }    
    List<DatanodeDescriptor> results = 
        new ArrayList<DatanodeDescriptor>(chosenNodes);
    DatanodeDescriptor[] resultsArray;
    DatanodeDescriptor[] datanodeLocations;
    
    datanodeLocations = pathNodeMap.getIfPresent(srcPath);
    if(datanodeLocations != null) {
      //Existing File (already has some block allocated)
      for(int index=0; index < datanodeLocations.length; index++) {
        results.add(datanodeLocations[index]);
      }     
      resultsArray = results.toArray(new DatanodeDescriptor[results.size()]);
    } else {
      //New file - block request
      resultsArray = super.chooseTarget(srcPath, numOfReplicas, writer, 
          chosenNodes, returnChosenNodes, excludedNodes, blocksize);
        if(resultsArray != null) {
        datanodeLocations = new DatanodeDescriptor[resultsArray.length];
        for(int index=0; index < resultsArray.length; index++) {
          datanodeLocations[index] = resultsArray[index];
        }
        pathNodeMap.put(srcPath, datanodeLocations);
      }
    }
    
    return getPipeline(writer, resultsArray);
  }
  
  /* Same as BlockPlacementPolicyDefault
   * 
   * Return a pipeline of nodes.
   * The pipeline is formed finding a shortest path that 
   * starts from the writer and traverses all <i>nodes</i>
   * This is basically a traveling salesman problem.
   */
  private DatanodeDescriptor[] getPipeline(
                                           DatanodeDescriptor writer,
                                           DatanodeDescriptor[] nodes) {
    if (nodes.length==0) return nodes;
      
    synchronized(clusterMap) {
      int index=0;
      if (writer == null || !clusterMap.contains(writer)) {
        writer = nodes[0];
      }
      for(;index<nodes.length; index++) {
        DatanodeDescriptor shortestNode = nodes[index];
        int shortestDistance = clusterMap.getDistance(writer, shortestNode);
        int shortestIndex = index;
        for(int i=index+1; i<nodes.length; i++) {
          DatanodeDescriptor currentNode = nodes[i];
          int currentDistance = clusterMap.getDistance(writer, currentNode);
          if (shortestDistance>currentDistance) {
            shortestDistance = currentDistance;
            shortestNode = currentNode;
            shortestIndex = i;
          }
        }
        //switch position index & shortestIndex
        if (index != shortestIndex) {
          nodes[shortestIndex] = nodes[index];
          nodes[index] = shortestNode;
        }
        writer = shortestNode;
      }
    }
    return nodes;
  }

}
