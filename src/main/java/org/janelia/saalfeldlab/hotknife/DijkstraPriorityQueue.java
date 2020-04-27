package org.janelia.saalfeldlab.hotknife;

import java.io.IOException;
import java.io.Serializable;
//Java implementation of Dijkstra's Algorithm  
//using Priority Queue: https://www.geeksforgeeks.org/dijkstras-shortest-path-algorithm-in-java-using-priorityqueue/
import java.util.*;
import java.util.Map.Entry;
 
public class DijkstraPriorityQueue implements Serializable{ 
 /**
	 * Class to perform Dijkstra algorithm using priority queue
	 */
	private static final long serialVersionUID = 1L;
private float dist[];
 private int prev[];
 private Set<Integer> settled; 
 private PriorityQueue<Node> pq; 
 private int V; // Number of vertices 
 List<List<Node> > adj;
 List<Integer> longestShortestPath;
 List<Integer> branches;
 List<Integer> endpoints;
 float longestShortestPathLength;
 public  int longestShortestPathI = -1;
 public int longestShortestPathJ = -1;
 int longestShortestPathNumVertices;
 
 
 public DijkstraPriorityQueue(int V) 
 { 
     this.V = V; 
     dist = new float[V];
     
     //DGA: added previous array to keep track of actual longest shortest path
     prev = new int[V];
     Arrays.fill(prev,-1);
     
     //DGA: Store longest shortest path
     longestShortestPath = new ArrayList<Integer>();
     longestShortestPathLength = -1;
     longestShortestPathNumVertices = 0;
     
     settled = new HashSet<Integer>();
     pq = new PriorityQueue<Node>(V, new Node()); 
     
     adj = new ArrayList();
 } 
 
 
 public DijkstraPriorityQueue(int V, Map<List<Integer>,Float> adj) 
 { 
	 this(V);
	 for (int i = 0; i < V; i++) { 
         List<Node> item = new ArrayList<Node>(); 
         this.adj.add(item); 
     } 
	for(Entry<List<Integer>, Float> e : adj.entrySet()) {
		Integer v1 = e.getKey().get(0);
		Integer v2 = e.getKey().get(1);
		float edgeWeight = e.getValue();
		this.adj.get(v1).add(new Node(v2,edgeWeight));
		this.adj.get(v2).add(new Node(v1,edgeWeight));
	}
    
 }
 
 public DijkstraPriorityQueue(int V, List<List<Node> > adj) 
 { 
	 this(V);
     this.adj = adj;
 }
 
 public void getBranchesAndEndpoints() {
	 branches =  new ArrayList();
	 endpoints = new ArrayList();
	 //find branches and endpoints
	for(int currentNode=0; currentNode<adj.size(); currentNode++) {
		List<Node> nodeList = adj.get(currentNode);
		if(nodeList.size()==1) { //then is an endpoint
			endpoints.add(currentNode);
		}
		else if(nodeList.size()>2) { //then is a branch
			branches.add(currentNode);
		}
	}
 }
 
/* public void simplifyBranches() {
	 //simplify branches
	 List<Integer> nodesToRemove = new ArrayList();
	 HashMap<List<Integer>,Float> connectionToAdd = new HashMap();
	 
	 for(int source: endpoints) {
		int nearestBranch = -1;
		float nearestBranchLength = Float.MAX_VALUE;
		 dijkstra(adj, source);
         for (int target : branches) { //for each endpoint, find nearest branch 
        	 if(dist[target]<nearestBranchLength) {
        		 nearestBranch = target;
        		 nearestBranchLength = dist[target];
        	 } 
         }
         connectionToAdd.put(Arrays.asList(source, nearestBranch), nearestBranchLength);
         
         int previousNode = nearestBranch;
    	 while( previousNode != -1 ) {
    		 if (previousNode != nearestBranch) nodesToRemove.add(previousNode);
    		 previousNode = prev[previousNode];
    	 }  
       }
	 
     List<List<Node> > simplifiedAdjacency = new ArrayList<List<Node> >(); 
     V = 0;
	 for( List<Node> currentNode : adj) {
		 for(Node nodeConnectedTo : currentNode) {
			 if 
		 }
		 simplifiedAdjacency
		 V++;
	 }
	 
	
 }
 */
 
 // Function for Dijkstra's Algorithm 
 public void dijkstra(List<List<Node> > adj, int src) 
 {        
	 Arrays.fill(dist, Integer.MAX_VALUE);
	 Arrays.fill(prev,-1);
	 
	 //DGA: Store longest shortest path
	 settled.clear();
	 pq.clear();
 
     this.adj = adj; 

     for (int i = 0; i < V; i++) 
         dist[i] = Integer.MAX_VALUE; 
     
     Arrays.fill(prev,-1);
     
     // Add source node to the priority queue 
     pq.add(new Node(src, 0)); 

     // Distance to the source is 0 
     dist[src] = 0; 
     long tic = System.currentTimeMillis();
     int numChecked=0;
     while (settled.size() != V) { 

         // remove the minimum distance node  
         // from the priority queue 
         int u = pq.remove().node; 

         // adding the node whose distance is 
         // finalized 
         settled.add(u); 

         e_Neighbours(u); 
         numChecked++;
     } 
     //System.out.println(src+" "+numChecked+" "+(System.currentTimeMillis()-tic));
 } 

 // Function to process all the neighbours  
 // of the passed node 
 private void e_Neighbours(int u) 
 { 
     float edgeDistance = -1; 
     float newDistance = -1; 

     // All the neighbors of v 
     for (int i = 0; i < adj.get(u).size(); i++) { 
         Node v = adj.get(u).get(i); 

         // If current node hasn't already been processed 
         if (!settled.contains(v.node)) { 
             edgeDistance = v.cost; 
             newDistance = dist[u] + edgeDistance; 

             // If new distance is cheaper in cost 
             if (newDistance < dist[v.node]) {
                 dist[v.node] = newDistance; 
                 prev[v.node] = u;
             }
             
             // Add the current node to the queue 
             pq.add(new Node(v.node, dist[v.node])); 
         } 
     } 
 } 
 
 public void calculateLongestShortestPathInformation() {
     long tic = System.currentTimeMillis();
     longestShortestPathLength = -1;
     for(int source=0; source<V; source++) {
    	dijkstra(adj, source);
         for (int target = source; target < V; target++) { 
        	 if(dist[target]>longestShortestPathLength) {
        		 longestShortestPathLength = dist[target];
        		 longestShortestPathI = source;
        		 longestShortestPathJ = target;
        		
        	 } 
         }
       }
     
     dijkstra(adj,longestShortestPathI);
     int previousNode = longestShortestPathJ;
	 while( previousNode != -1 ) {
		 longestShortestPathNumVertices++;
		 longestShortestPath.add(previousNode);
		 previousNode = prev[previousNode];
	 }
     long toc  = System.currentTimeMillis();
     System.out.println(toc-tic);
     System.out.println("Total vertices: "+V+". Longest shortest path number of vertices: "+longestShortestPathNumVertices+". The shortest path length is: " + longestShortestPathLength);// +". Longest shortest path is: "+longestShortestPath+". Time: "+(toc-tic)/1000.0); 

    /* System.out.println(longestShortestPathNumVertices);
     System.out.println(longestShortestPath);
     System.out.println(longestShortestPathLength);*/
 }	
 
 public void calculateLongestShortestPathWithRemovalOfIntermediates() {
     long tic = System.currentTimeMillis();
     List <Integer> remainingVertices = new ArrayList();
     List <Integer> intermediateVerticesToDelete = new ArrayList();
     for(int i=0; i<V; i++)
    	 remainingVertices.add(i);
     
     longestShortestPathLength = -1;
     while(remainingVertices.size()>0) {
    	 int source = remainingVertices.get(0);
    	 remainingVertices.remove(new Integer(source));
	     dijkstra(adj, source);
	     for (int target : remainingVertices) { 
	    	 int previousNode = target;
    		 while( previousNode != -1 ) {
    			 if(previousNode != target && previousNode!=source) remainingVertices.remove(new Integer (previousNode));
    			 previousNode = prev[previousNode];
    		 }
        	 if(dist[target]>longestShortestPathLength) {
        		 longestShortestPathLength = dist[target];
        		 longestShortestPathI = source;
        		 longestShortestPathJ = target;
        		
        	 } 
	     }
	     
	   }
     
     dijkstra(adj,longestShortestPathI);
     int previousNode = longestShortestPathJ;
	 while( previousNode != -1 ) {
		 longestShortestPathNumVertices++;
		 longestShortestPath.add(previousNode);
		 previousNode = prev[previousNode];
	 }
     long toc  = System.currentTimeMillis();
     System.out.println(toc-tic);
     System.out.println("Total vertices: "+V+". Longest shortest path number of vertices: "+longestShortestPathNumVertices+". The shortest path length is: " + longestShortestPathLength);// +". Longest shortest path is: "+longestShortestPath+". Time: "+(toc-tic)/1000.0); 

    /* System.out.println(longestShortestPathNumVertices);
     System.out.println(longestShortestPath);
     System.out.println(longestShortestPathLength);*/
 }	
 
 // Driver code 
 public static void main(String arg[]) throws IOException 
 { 
	 
     int V = 100; 
     Map<List<Integer>,Float>adjacency = new HashMap();
     for(int i=0; i<V-1;i++) {
    	 adjacency.put(Arrays.asList(i,i+1),1.0f);
     }
     adjacency.put(Arrays.asList(50,99),3.0f);
     // Adjacency list representation of the  
     // connected edges 
    /* List<List<Node> > adjacency = new ArrayList<List<Node> >(); 

     // Initialize list for every node 
     for (int i = 0; i < V; i+=1) { 
         List<Node> item = new ArrayList<Node>(); 
         adjacency.add(item); 
     } 
     
     int connections = 1;
     for(int i=0; i<V-connections; i++) {
    	 for(int c = 1; c<=connections; c++) {
	    	 adjacency.get(i).add(new Node(i+c,1));
	    	 adjacency.get(i+c).add(new Node(i,1));
    	 }
     }
     for(int i=1000;i<4000;i+=2) {
    	 adjacency.get(i).add(new Node(V-i,1));
    	 adjacency.get(V-i).add(new Node(i,1));
     }*/
     System.out.println("hi");
     DijkstraPriorityQueue dpq = new DijkstraPriorityQueue(V, adjacency);
     dpq.calculateLongestShortestPathInformation();
 } 
} 


//Class to represent a node in the graph 
class Node implements Comparator<Node>,Serializable { 
 /**
	 * Node class for path finding
	 */
	private static final long serialVersionUID = 1L;
public int node; 
 public float cost; 

 public Node() 
 { 
 } 

 public Node(int node, float cost) 
 { 
     this.node = node; 
     this.cost = cost; 
 } 
 
 public Node(int node, double cost) 
 { 
     this.node = node; 
     this.cost = (float) cost; 
 } 
 

 @Override
 public int compare(Node node1, Node node2) 
 { 
     if (node1.cost < node2.cost) 
         return -1; 
     if (node1.cost > node2.cost) 
         return 1; 
     return 0; 
 }
}