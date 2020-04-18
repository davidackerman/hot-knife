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
 } 
 
 public DijkstraPriorityQueue(int V, List<List<Node> > adj) 
 { 
	 this(V);
     this.adj = adj;
 }
 
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
 
 // Driver code 
 public static void main(String arg[]) throws IOException 
 { 
	 
     int V = 50000; 

     // Adjacency list representation of the  
     // connected edges 
     List<List<Node> > adjacency = new ArrayList<List<Node> >(); 

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
     
     adjacency.get(25000).add(new Node(49000,1));
     adjacency.get(49000).add(new Node(25000,1));

   /*
     adjacency.get(13).add(new Node(17,1.0));
     adjacency.get(17).add(new Node(13,1.0));
     adjacency.get(5).add(new Node(10,1.0));
     adjacency.get(10).add(new Node(5,1.0));
     adjacency.get(11).add(new Node(25,1.7320508));
     adjacency.get(25).add(new Node(11,1.7320508));
     adjacency.get(36).add(new Node(35,1.0));
     adjacency.get(35).add(new Node(36,1.0));
     adjacency.get(28).add(new Node(13,1.4142135));
     adjacency.get(13).add(new Node(28,1.4142135));
     adjacency.get(16).add(new Node(21,1.7320508));
     adjacency.get(21).add(new Node(16,1.7320508));
     adjacency.get(9).add(new Node(18,1.0));
     adjacency.get(18).add(new Node(9,1.0));
     adjacency.get(0).add(new Node(11,1.4142135));
     adjacency.get(11).add(new Node(0,1.4142135));
     adjacency.get(18).add(new Node(23,1.7320508));
     adjacency.get(23).add(new Node(18,1.7320508));
     adjacency.get(5).add(new Node(24,1.4142135));
     adjacency.get(24).add(new Node(5,1.4142135));
     adjacency.get(20).add(new Node(19,1.0));
     adjacency.get(19).add(new Node(20,1.0));
     adjacency.get(33).add(new Node(30,1.7320508));
     adjacency.get(30).add(new Node(33,1.7320508));
     adjacency.get(16).add(new Node(21,1.7320508));
     adjacency.get(21).add(new Node(16,1.7320508));
     adjacency.get(14).add(new Node(12,1.4142135));
     adjacency.get(12).add(new Node(14,1.4142135));
     adjacency.get(11).add(new Node(25,1.7320508));
     adjacency.get(25).add(new Node(11,1.7320508));
     adjacency.get(18).add(new Node(23,1.7320508));
     adjacency.get(23).add(new Node(18,1.7320508));
     adjacency.get(20).add(new Node(31,1.4142135));
     adjacency.get(31).add(new Node(20,1.4142135));
     adjacency.get(32).add(new Node(1,1.4142135));
     adjacency.get(1).add(new Node(32,1.4142135));
     adjacency.get(22).add(new Node(21,1.0));
     adjacency.get(21).add(new Node(22,1.0));
     adjacency.get(2).add(new Node(30,1.4142135));
     adjacency.get(30).add(new Node(2,1.4142135));
     adjacency.get(27).add(new Node(6,1.4142135));
     adjacency.get(6).add(new Node(27,1.4142135));
     adjacency.get(10).add(new Node(26,1.4142135));
     adjacency.get(26).add(new Node(10,1.4142135));
     adjacency.get(5).add(new Node(24,1.4142135));
     adjacency.get(24).add(new Node(5,1.4142135));
     adjacency.get(29).add(new Node(27,1.4142135));
     adjacency.get(27).add(new Node(29,1.4142135));
     adjacency.get(20).add(new Node(31,1.4142135));
     adjacency.get(31).add(new Node(20,1.4142135));
     adjacency.get(22).add(new Node(21,1.0));
     adjacency.get(21).add(new Node(22,1.0));
     adjacency.get(34).add(new Node(29,1.4142135));
     adjacency.get(29).add(new Node(34,1.4142135));
     adjacency.get(23).add(new Node(15,1.0));
     adjacency.get(15).add(new Node(23,1.0));
     adjacency.get(2).add(new Node(30,1.4142135));
     adjacency.get(30).add(new Node(2,1.4142135));
     adjacency.get(1).add(new Node(34,1.0));
     adjacency.get(34).add(new Node(1,1.0));
     adjacency.get(27).add(new Node(6,1.4142135));
     adjacency.get(6).add(new Node(27,1.4142135));
     adjacency.get(25).add(new Node(35,1.4142135));
     adjacency.get(35).add(new Node(25,1.4142135));
     adjacency.get(25).add(new Node(35,1.4142135));
     adjacency.get(35).add(new Node(25,1.4142135));
     adjacency.get(0).add(new Node(14,1.4142135));
     adjacency.get(14).add(new Node(0,1.4142135));
     adjacency.get(9).add(new Node(18,1.0));
     adjacency.get(18).add(new Node(9,1.0));
     adjacency.get(15).add(new Node(7,1.0));
     adjacency.get(7).add(new Node(15,1.0));
     adjacency.get(20).add(new Node(19,1.0));
     adjacency.get(19).add(new Node(20,1.0));
     adjacency.get(36).add(new Node(35,1.0));
     adjacency.get(35).add(new Node(36,1.0));
     adjacency.get(32).add(new Node(1,1.4142135));
     adjacency.get(1).add(new Node(32,1.4142135));
     adjacency.get(16).add(new Node(8,1.4142135));
     adjacency.get(8).add(new Node(16,1.4142135));
     adjacency.get(31).add(new Node(33,1.4142135));
     adjacency.get(33).add(new Node(31,1.4142135));
     adjacency.get(19).add(new Node(22,1.0));
     adjacency.get(22).add(new Node(19,1.0));
     adjacency.get(3).add(new Node(2,1.0));
     adjacency.get(2).add(new Node(3,1.0));
     adjacency.get(19).add(new Node(22,1.0));
     adjacency.get(22).add(new Node(19,1.0));
     adjacency.get(8).add(new Node(24,1.0));
     adjacency.get(24).add(new Node(8,1.0));
     adjacency.get(4).add(new Node(16,1.4142135));
     adjacency.get(16).add(new Node(4,1.4142135));
     adjacency.get(3).add(new Node(2,1.0));
     adjacency.get(2).add(new Node(3,1.0));
     adjacency.get(28).add(new Node(13,1.4142135));
     adjacency.get(13).add(new Node(28,1.4142135));
     adjacency.get(34).add(new Node(29,1.4142135));
     adjacency.get(29).add(new Node(34,1.4142135));
     adjacency.get(17).add(new Node(0,1.0));
     adjacency.get(0).add(new Node(17,1.0));
     adjacency.get(33).add(new Node(30,1.7320508));
     adjacency.get(30).add(new Node(33,1.7320508));
     adjacency.get(24).add(new Node(32,1.4142135));
     adjacency.get(32).add(new Node(24,1.4142135));
     adjacency.get(3).add(new Node(28,1.4142135));
     adjacency.get(28).add(new Node(3,1.4142135));
     adjacency.get(10).add(new Node(26,1.4142135));
     adjacency.get(26).add(new Node(10,1.4142135));
     adjacency.get(24).add(new Node(32,1.4142135));
     adjacency.get(32).add(new Node(24,1.4142135));
     adjacency.get(8).add(new Node(5,1.0));
     adjacency.get(5).add(new Node(8,1.0));
     adjacency.get(29).add(new Node(27,1.4142135));
     adjacency.get(27).add(new Node(29,1.4142135));
     adjacency.get(14).add(new Node(12,1.4142135));
     adjacency.get(12).add(new Node(14,1.4142135));
     adjacency.get(0).add(new Node(14,1.4142135));
     adjacency.get(14).add(new Node(0,1.4142135));
     adjacency.get(15).add(new Node(7,1.0));
     adjacency.get(7).add(new Node(15,1.0));
     adjacency.get(13).add(new Node(17,1.0));
     adjacency.get(17).add(new Node(13,1.0));
     adjacency.get(17).add(new Node(0,1.0));
     adjacency.get(0).add(new Node(17,1.0));
     adjacency.get(16).add(new Node(8,1.4142135));
     adjacency.get(8).add(new Node(16,1.4142135));
     adjacency.get(7).add(new Node(4,1.4142135));
     adjacency.get(4).add(new Node(7,1.4142135));
     adjacency.get(4).add(new Node(16,1.4142135));
     adjacency.get(16).add(new Node(4,1.4142135));
     adjacency.get(23).add(new Node(15,1.0));
     adjacency.get(15).add(new Node(23,1.0));
     adjacency.get(7).add(new Node(4,1.4142135));
     adjacency.get(4).add(new Node(7,1.4142135));
     adjacency.get(3).add(new Node(28,1.4142135));
     adjacency.get(28).add(new Node(3,1.4142135));
     adjacency.get(0).add(new Node(11,1.4142135));
     adjacency.get(11).add(new Node(0,1.4142135));
     adjacency.get(1).add(new Node(34,1.0));
     adjacency.get(34).add(new Node(1,1.0));
     adjacency.get(8).add(new Node(24,1.0));
     adjacency.get(24).add(new Node(8,1.0));
     adjacency.get(8).add(new Node(5,1.0));
     adjacency.get(5).add(new Node(8,1.0));
     adjacency.get(5).add(new Node(10,1.0));
     adjacency.get(10).add(new Node(5,1.0));
     adjacency.get(31).add(new Node(33,1.4142135));
     adjacency.get(33).add(new Node(31,1.4142135));
     */
     System.out.println("hi");
     DijkstraPriorityQueue dpq = new DijkstraPriorityQueue(V, adjacency);
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