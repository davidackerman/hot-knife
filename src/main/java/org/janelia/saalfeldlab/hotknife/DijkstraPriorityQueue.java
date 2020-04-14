package org.janelia.saalfeldlab.hotknife;

import java.io.IOException;
import java.io.Serializable;
//Java implementation of Dijkstra's Algorithm  
//using Priority Queue: https://www.geeksforgeeks.org/dijkstras-shortest-path-algorithm-in-java-using-priorityqueue/
import java.util.*;
 
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
 float longestShortestPathLength;
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
 } 
 
 public DijkstraPriorityQueue(int V, List<List<Node> > adj) 
 { 
	 this(V);
     this.adj = adj;
 } 

 // Function for Dijkstra's Algorithm 
 public void dijkstra(List<List<Node> > adj, int src) 
 { 
     this.adj = adj; 

     for (int i = 0; i < V; i++) 
         dist[i] = Integer.MAX_VALUE; 
     
     Arrays.fill(prev,-1);
     
     // Add source node to the priority queue 
     pq.add(new Node(src, 0)); 

     // Distance to the source is 0 
     dist[src] = 0; 
     while (settled.size() != V) { 

         // remove the minimum distance node  
         // from the priority queue 
         int u = pq.remove().node; 

         // adding the node whose distance is 
         // finalized 
         settled.add(u); 

         e_Neighbours(u); 
     } 
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
 
 public void calculateLongestShortestPath() {
     long tic = System.currentTimeMillis();
	 DijkstraPriorityQueue dpq = null;
     longestShortestPathLength = -1;
     for(int source=0; source<V; source++) {
    	 dpq = new DijkstraPriorityQueue(V, adj);
    	 dpq.dijkstra(adj, source);
         for (int target = source; target < V; target++) { 
        	 if(dpq.dist[target]>longestShortestPathLength) {
        	     longestShortestPathNumVertices = 0;
        		 longestShortestPath.clear();
        		 longestShortestPathLength = dpq.dist[target];
        		 int previousNode = target;
        		 while( previousNode != -1 ) {
        			 longestShortestPathNumVertices++;
    				 longestShortestPath.add(previousNode);
        			 previousNode = dpq.prev[previousNode];
        		 }
        	 } 
         }
       }
     long toc  = System.currentTimeMillis();

     System.out.println("Total vertices: "+V+". Longest shortest path number of vertices: "+longestShortestPathNumVertices+". The shortest path length is: " + longestShortestPathLength +". Longest shortest path is: "+longestShortestPath+". Time: "+(toc-tic)/1000.0); 

    /* System.out.println(longestShortestPathNumVertices);
     System.out.println(longestShortestPath);
     System.out.println(longestShortestPathLength);*/
 }	
 
 // Driver code 
 public static void main(String arg[]) throws IOException 
 { 
	 
     int V = 100; 

     // Adjacency list representation of the  
     // connected edges 
     List<List<Node> > adj = new ArrayList<List<Node> >(); 

     // Initialize list for every node 
     for (int i = 0; i < V; i+=1) { 
         List<Node> item = new ArrayList<Node>(); 
         adj.add(item); 
     } 

     for(int i=0; i<V-2; i++) {
    	 adj.get(i).add(new Node(i+1,1));
    	 adj.get(i).add(new Node(i+2,1));	 
     }

     DijkstraPriorityQueue dpq = new DijkstraPriorityQueue(V, adj);
     dpq.calculateLongestShortestPath();
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