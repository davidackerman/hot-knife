package org.janelia.saalfeldlab.hotknife;

import java.io.IOException;
import java.io.Serializable;
//Java implementation of Dijkstra's Algorithm  
//using Priority Queue: https://www.geeksforgeeks.org/dijkstras-shortest-path-algorithm-in-java-using-priorityqueue/
import java.util.*;
 
public class DijkstraPriorityQueue implements Serializable{ 
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
     longestShortestPath = new ArrayList();
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
       	 //adj.get(i+1).add(new Node(i,1));
    	 //adj.get(i+2).add(new Node(i,1));
    	 /*adj.get(i).add(new Node(i+3,1));
    	 adj.get(i).add(new Node(i+4,1));
    	 adj.get(i).add(new Node(i+5,1));
    	 adj.get(i).add(new Node(i+6,1));
    	 adj.get(i).add(new Node(i+7,1));
    	 adj.get(i).add(new Node(i+8,1));*/
    	 
    	// adj.get(i+1).add(new Node(i,1));
    	// adj.get(i+2).add(new Node(i,1));
    	 /*adj.get(i+3).add(new Node(i,1));
    	 adj.get(i+4).add(new Node(i,1));
    	 adj.get(i+5).add(new Node(i,1));
    	 adj.get(i+6).add(new Node(i,1));
    	 adj.get(i+7).add(new Node(i,1));
    	 adj.get(i+8).add(new Node(i,1));*/
    	 
     }
   // adj.get(5).add(new Node(7,1));
    // adj.get(7).add(new Node(5,1));

     long tic = System.currentTimeMillis();
     DijkstraPriorityQueue dpq = new DijkstraPriorityQueue(V, adj);
     dpq.calculateLongestShortestPath();
     long toc  = System.currentTimeMillis();
    // System.out.println("Longest shortest path number of vertices: "+dpq.longestShortestPathNumVertices+". The shortest path length is: " + dpq.longestShortestPathLength +". Longest shortest path is: "+dpq.longestShortestPath+ ". Calcualted in: "  + ((toc-tic)/1000.0)); 
     
	 
	 
	 /*
	  List<List<Node> > adj = new ArrayList<List<Node> >(); 
	 
	 	final N5FSReader n5 = new N5FSReader("/groups/cosem/cosem/ackermand/HeLa_Cell3_4x4x4nm_it450000_crop_analysis.n5/");
		final RandomAccessibleInterval<UnsignedByteType> img = N5Utils.open(n5, "mito_cc_skeletonIndependent");
		RandomAccess<UnsignedByteType> imgRandomAccess= img.randomAccess();
		
		Map<Long,Integer> mapPixelToInt = new HashMap<>();
		System.out.println(img.dimension(0)+" "+img.dimension(1)+" "+img.dimension(2));
		
		//vertex count: 19517
		//edge count: 30283
		
		int edgeCount = 0, vertexCount = 0;
		
		for(int x=0; x<img.dimension(0); x++) {
			for(int y=0; y<img.dimension(1); y++) {
				for(int z=0; z<img.dimension(2); z++) {
					int [] pos = new int[] {x,y,z};
					imgRandomAccess.setPosition(pos);
					if(imgRandomAccess.get().get()>0) { //is part of skeleton
						long V1 = img.dimension(0) * img.dimension(1) * pos[2] + img.dimension(0) * pos[1] + pos[0] + 1;
						if(mapPixelToInt.putIfAbsent(V1 , vertexCount) == null) {//null is returned if it is newly added, so need to increase vertex count
							vertexCount++;
							List<Node> item = new ArrayList<Node>(); 
					        adj.add(item); 
						}
						int node1 = mapPixelToInt.get(V1);
						for(int dx = 0; dx<=1; dx ++) {
							for(int dy = 0; dy<=1; dy++) {
								for(int dz = 0; dz<=1; dz++) {
									int [] newPos = new int[] {x+dx, y+dy, z+dz};
									imgRandomAccess.setPosition(newPos);
									//if(x==500 || y==500 || z == 500) {
									//	System.out.println(Arrays.toString(newPos));
									//	System.out.println(imgRandomAccess.get().get());
									//}
										if (!(dx==0 && dy==0 && dz==0) && (newPos[0]<img.dimension(0) && newPos[1]<img.dimension(1) && newPos[2] < img.dimension(2)) && imgRandomAccess.get().get()>0) {//also part of skeleton
										long V2 = img.dimension(0) * img.dimension(1) * newPos[2] + img.dimension(0) * newPos[1] + newPos[0] + 1;
										if(mapPixelToInt.putIfAbsent(V2 , vertexCount) == null) {//null is returned if it is newly added, so need to increase vertex count
											vertexCount++;
											List<Node> item = new ArrayList<Node>(); 
									        adj.add(item); 
										}
										int node2 = mapPixelToInt.get(V2);
										
										float distance = (float) Math.sqrt(dx^2+dy^2+dz^2);
										adj.get(node1).add(new Node(node2, distance));
										adj.get(node2).add(new Node(node1, distance));
										
										edgeCount++;
									}
								}
							}
						}
						//System.out.println(count);						
					}
				}
			}
		}
		
	 
		System.out.println("Vertices: "+vertexCount+", edges: "+edgeCount);
		 long tic = System.currentTimeMillis();
	     DijkstraPriorityQueue dpq = new DijkstraPriorityQueue(vertexCount, adj);
	     float shortestPath = dpq.calculateLongestShortestPath();
	     long toc  = System.currentTimeMillis();
	     System.out.println("The shortest path length is : " + shortestPath +". Calcualted in: "  + ((toc-tic)/1000.0)); 
			*/
     
	 /*
	 int V = 5; 
     int source = 0; 

     // Adjacency list representation of the  
     // connected edges 
     List<List<Node> > adj = new ArrayList<List<Node> >(); 

     // Initialize list for every node 
     for (int i = 0; i < V; i++) { 
         List<Node> item = new ArrayList<Node>(); 
         adj.add(item); 
     } 

     // Inputs for the DPQ graph 
     adj.get(0).add(new Node(1, 9)); 
     adj.get(0).add(new Node(2, 6)); 
     adj.get(0).add(new Node(3, 5)); 
     adj.get(0).add(new Node(4, 3)); 

     adj.get(2).add(new Node(1, 2)); 
     adj.get(2).add(new Node(3, 4)); 

     // Calculate the single source shortest path 
     DijkstraPriorityQueue dpq = new DijkstraPriorityQueue(V); 
     dpq.dijkstra(adj, source); 

     // Print the shortest path to all the nodes 
     // from the source node 
     System.out.println("The shorted path from node :"); 
     for (int i = 0; i < dpq.dist.length; i++) 
         System.out.println(source + " to " + i + " is "
                            + dpq.dist[i]); 
                            */
 } 
} 


//Class to represent a node in the graph 
class Node implements Comparator<Node>,Serializable { 
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