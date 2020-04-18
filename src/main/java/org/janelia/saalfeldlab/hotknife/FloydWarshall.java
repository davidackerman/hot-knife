package org.janelia.saalfeldlab.hotknife;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class FloydWarshall {
	/**
	 * Class for doing Floyd Warshall algorithm for symmetric adjacency matrix based on wiki: https://stackoverflow.com/questions/2037735/optimise-floyd-warshall-for-symmetric-adjacency-matrix
	*/
	public float dist[][];
	public int next[][];
	final Set<Long> V = new HashSet<>(); 
	public float longestShortestPathLength;
	public  int longestShortestPathI = -1;
	public int longestShortestPathJ = -1;
	public List<Integer> longestShortestPath = new ArrayList<Integer>();
	public 	int longestShortestPathNumVertices = 0;
 
	
	public FloydWarshall(Map<List<Integer>,Float> adjacency, int numVertices) {
		
		
		this.dist = new float[numVertices][numVertices];
		for (float[] row: dist)
		    Arrays.fill(row, Float.MAX_VALUE);
		
		this.next = new int[numVertices][numVertices];
		for (int[] row: next)
		    Arrays.fill(row, -1);

		for(Entry<List<Integer>,Float> entry : adjacency.entrySet()) {
			
			List<Integer> key = entry.getKey();
			Float value = entry.getValue();
			
			int v1 = key.get(0);
			int v2 = key.get(1);
			dist[v1][v1] = 0;
			dist[v2][v2] = 0;
			dist[v1][v2] = value;
			dist[v2][v1] = value;
			
			next[v1][v1] = v1;
			next[v2][v2] = v2;
			next[v1][v2] = v2;
			next[v2][v1] = v1;
		}

	}
	
	public FloydWarshall(Map<List<Integer>,Float> adjacency) {
		this(adjacency, getNumberOfVertices(adjacency));
	}
	
	private static int getNumberOfVertices(Map<List<Integer>,Float> adjacency) {
		HashSet<Integer> uniqueVertices = new HashSet<Integer>();
		for(List<Integer> key : adjacency.keySet()) {
			uniqueVertices.add(key.get(0));
			uniqueVertices.add(key.get(1));
		}
		return uniqueVertices.size();
	}


	public void calculateFloydWarshallPaths() {
		float newDist;
		//i>j, k>i
		for (int k = 0; k < dist.length; ++k) {
		    for (int i = 0; i < k; ++i) //i<k
		        for (int j = 0; j <= i; ++j) { //j<i<k (biggest first)
		        	newDist = dist[k][i] + dist[k][j];
		        	if(newDist < dist[i][j]) {
		        		dist[i][j] = newDist;
		        		next[i][j] = next[k][i];
		        	}
		        }
		    for (int i = k; i < dist.length; ++i) { //k<=i
		        for (int j = 0; j < k; ++j) { //j<=k<=i
		        	newDist = dist[i][k] + dist[k][j];
		        	if(newDist < dist[i][j]) {
		        		dist[i][j] = newDist;
		        		next[i][j] = next[i][k];
		        	}
		        }
		        for (int j = k; j <= i; ++j) {//k<=j<=i
		        	newDist = dist[i][k] + dist[j][k];
		        	if(newDist < dist[i][j]) {
		        		dist[i][j] = newDist;
		        		next[i][j] = next[i][k];
		        	}
		        }
		    }
		}
	}
	
	public void calculateFloydWarshallPathsOriginal() {
		float newDist;
		for (int k = 0; k < dist.length; ++k) {
		    for (int i = 0; i < dist.length; ++i)
		        for (int j = 0; j <dist.length; ++j) {
		        	newDist = dist[i][k] + dist[k][j];
		        	if (newDist < dist[i][j]) {
		        		dist[i][j] = newDist;
		        		next[i][j] = next[i][k];
		        	}
		        }
		}

	}
	
	public void calculateLongestShortestPath() {	
		if (next[longestShortestPathI][longestShortestPathJ] != -1) {
				int i  = longestShortestPathI;
				int j = longestShortestPathJ;
				longestShortestPath.add(longestShortestPathI);
				while(i != j) {
					i = next[i][j];
					longestShortestPath.add(i);
				}
		}
		longestShortestPathNumVertices = longestShortestPath.size();
	}
	
    		
	public void calculateLongestShortestPathInformation() {
		long tic = System.currentTimeMillis();
		calculateFloydWarshallPathsOriginal();
		float maxDist = -1;
		for(int i=0; i<dist.length; i++) {
			for(int j=0; j<i; j++) {
				if(dist[i][j]>maxDist) {
					longestShortestPathI = i;
					longestShortestPathJ = j;
					longestShortestPathLength = dist[i][j];	
					maxDist = longestShortestPathLength;
				}
			}
		}
		
		calculateLongestShortestPath();
		System.out.println("Number of vertices: "+dist.length+ ". Longest shortest path: (Start,End,Vertices, Length): "+"("+longestShortestPathI + ", " + longestShortestPathJ+", "+ longestShortestPathNumVertices+ ", "+ longestShortestPathLength + "). Time: "+(System.currentTimeMillis()-tic)/1000.0);
	}
	 

	public static void main(final String[] args) throws IOException {

		//System.out.println(vertexCount);
		Map<List<Integer>,Float> distanceMap = new HashMap<>();   
		for(int i = 0; i<2000;i++) {
			distanceMap.put(Arrays.asList(i,i+1),1.0f);
		}
		/*distanceMap.put(Arrays.asList(1,11),1.0f);
		distanceMap.put(Arrays.asList(0,11),1.0f);
		distanceMap.put(Arrays.asList(11,12),1.0f);
		distanceMap.put(Arrays.asList(12,13),1.0f);*/
						
		FloydWarshall as = new FloydWarshall(distanceMap);
		long tic = System.currentTimeMillis();
		as.calculateFloydWarshallPathsOriginal();
		as.calculateLongestShortestPathInformation();
		System.out.println("time: "+(System.currentTimeMillis()-tic));	
		
	}
	

	
}
