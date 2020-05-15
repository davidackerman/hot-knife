// Java to shortest path from a given source vertex 's' to  
// a given destination vertex 't'. Expected time complexity  
// is O(V+E).  

//https://www.geeksforgeeks.org/shortest-path-weighted-graph-weight-edge-1-2/
package org.janelia.saalfeldlab.hotknife;

import java.util.*; 
  
class BreadthFirstSearch  
{ 
    // This class represents a directed graph using adjacency 
    // list representation 
    static class Graph  
    { 
        int V; // No. of vertices 
        public Vector<Integer>[] adj; // No. of vertices 
        public boolean[] visited; 
        public int[] parent; 
        
        static int level; 
        
        public Queue<Integer> queue;
  
        // Constructor 
        @SuppressWarnings("unchecked") 
        Graph(int V) 
        { 
            this.V = V; 
            this.adj = new Vector[3 * V]; 
  
            for (int i = 0; i < 3 * V; i++) 
                this.adj[i] = new Vector<>(); 
            
            visited = new boolean[3 * this.V]; 
            parent = new int[3 * this.V]; 
            
            // Create a queue for BFS 
            queue = new LinkedList<>(); 
  
        } 
        
        public void addUndirectedEdge(int v, int w, int weight) {
        	 // split all edges of weight 2 into two 
            // edges of weight 1 each. The intermediate 
            // vertex number is maximum vertex number + 1, 
            // that is V.
        	if (weight == 3) {
        		addUndirectedUnitLengthEdge(v, v+this.V);
        		addUndirectedUnitLengthEdge(v+this.V, v+2*this.V);
        		addUndirectedUnitLengthEdge(v+2*this.V, w);
        	}
        	else if (weight == 2)  
            { 	
            	addUndirectedUnitLengthEdge(v, v+this.V);
        		addUndirectedUnitLengthEdge(v+this.V, w);
            } else { // Weight is 1
            	addUndirectedUnitLengthEdge(v, w);
            }
        }
        
        public void addUndirectedUnitLengthEdge(int v, int w) {
        	adj[v].add(w); 
        	adj[w].add(v);
        }
        
        // print shortest path from a source vertex 's' to 
        // destination vertex 'd'. 
        public int printShortestPath(int[] parent, int s, int d) 
        { 
            level = 0; 
  
            // If we reached root of shortest path tree 
            if (parent[s] == -1) 
            { 
               /*System.out.printf("Shortest Path between"+  
                                "%d and %d is %s ", s, d, s); */
                return level; 
            } 

            printShortestPath(parent, parent[s], d); 
  
            level++; 
           /* if (s < this.V) { 
                System.out.printf("%d ", s); 
            }*/
            return level; 
        } 
        
        public int getShortestPath(int[] parent, int s, int d) 
        { 
            level = 0; 
            while(parent[s] !=-1 ) {
            	s = parent[s];
            	level++;
            }
            return level;
           /* // If we reached root of shortest path tree 
            if (parent[s] == -1) 
            { 
                return level; 
            } 

            getShortestPath(parent, parent[s], d); 
  
            level++; 
            return level; */
        } 
        
  
        // finds shortest path from source vertex 's' to 
        // destination vertex 'd'. 
  
        // This function mainly does BFS and prints the 
        // shortest path from src to dest. It is assumed 
        // that weight of every edge is 1 
        public int findShortestPath(int src, int dest) 
        { 
           // boolean[] visited = new boolean[3 * this.V]; 
            //int[] parent = new int[3 * this.V]; 
  
            // Initialize parent[] and visited[] 
            for (int i = 0; i < 3 * this.V; i++)  
            { 
                visited[i] = false; 
                parent[i] = -1; 
            } 
        	queue.clear();

           
            // Mark the current node as visited and enqueue it 
            visited[src] = true; 
            queue.add(src); 
  
            while (!queue.isEmpty())  
            { 
  
                // Dequeue a vertex from queue and print it 
                int s = queue.peek(); 
  
                if (s == dest) 
                	return getShortestPath(parent, s, dest);
                    //return printShortestPath(parent, s, dest); 
                queue.poll(); 
  
                // Get all adjacent vertices of the dequeued vertex s 
                // If a adjacent has not been visited, then mark it 
                // visited and enqueue it 
                for (int i : this.adj[s])  
                { 
                    if (!visited[i])  
                    { 
                        visited[i] = true; 
                        queue.add(i); 
                        parent[i] = s; 
                    } 
                } 
            } 
            return 0; 
        } 
    } 
  
    // Driver Code 
    public static void main(String[] args) 
    { 
  
        // Create a graph given in the above diagram 
        int V = 120000; 
        Graph g = new Graph(V);
        for(int i=0; i<V-1; i++) {
        	g.addUndirectedEdge(i, i+1, 1);
        }
        /*for(int i=1000;i<4000;i+=2) {
        	g.addUndirectedEdge(i, V-i,1);
        }*/
        //g.addUndirectedEdge(50000,25000,1);
       /* g.addUndirectedEdge(0,1,1);
        g.addUndirectedEdge(1,2,1);
        g.addUndirectedEdge(2,3,2);
        g.addUndirectedEdge(3,4,3);*/
        int src = 0, dest = V;
        long longestShortestPathLength = 0;
        long tic = System.currentTimeMillis();
        List<Integer> remainingVerticesToCheck = new ArrayList();
 
        
        int count=0;
        for(src = 0; src<V; src++) {
            for(int i=src+1;i<V;i++) remainingVerticesToCheck.add(i);
	        while(! remainingVerticesToCheck.isEmpty()) {
	        		dest = remainingVerticesToCheck.remove(remainingVerticesToCheck.size()-1);
	                long shortestPathLength = g.findShortestPath(src, dest);
	                tic = System.currentTimeMillis();
	                for(int inPath : g.parent)
	                	remainingVerticesToCheck.remove(Integer.valueOf(inPath));
	                //remainingVerticesToCheck.removeAll(Arrays.asList(g.parent));
	                System.out.println(System.currentTimeMillis()-tic);
	                if(shortestPathLength>longestShortestPathLength) longestShortestPathLength = shortestPathLength;
	                count++;
	            	//if (dest%100==0) {
	            	//}
	        	//}
	        }
	       /* System.out.printf("\n %d Src: %d. Current longest shortest %d\n", System.currentTimeMillis()-tic,src,  
                    longestShortestPathLength); */
        }
       // https://www.quora.com/What-algorithm-can-be-used-to-find-the-diameter-of-an-unweighted-simple-connected-graph
        //https://www.sciencedirect.com/science/article/pii/S0304397512008687
        System.out.printf("%f %d\n.",(System.currentTimeMillis()-tic)*1.0/count,longestShortestPathLength);
       /* int count=0;
        for(Vector<Integer> row : g.adj) {
        	System.out.println(count+" "+row);
        	count++;
        }*/
        

    } 
} 
  
// This code is contributed by 
// sanjeev2552 