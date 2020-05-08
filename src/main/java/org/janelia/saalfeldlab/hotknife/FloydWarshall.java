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
	public  int longestShortestPathStart = -1;
	public int longestShortestPathEnd = -1;
	public List<Integer> longestShortestPath = new ArrayList<Integer>();
	public 	int longestShortestPathNumVertices = 0;
	public Set<Integer> endpoints;
	public Set<Integer> branchpoints;
	public Set<Integer> prunedVertices;
 
	
	public FloydWarshall(int numVertices, Map<List<Integer>,Float> adjacency) {
		
		this.prunedVertices = new HashSet<Integer>();
		
		this.dist = new float[numVertices][numVertices];
		for (float[] row: dist)
		    Arrays.fill(row, Float.MAX_VALUE);

		for(int i=0; i<numVertices;i++) {
			dist[i][i]=0;
		}
		
		this.next = new int[numVertices][numVertices];
		for (int[] row: next)
		    Arrays.fill(row, -1);

		for(Entry<List<Integer>,Float> entry : adjacency.entrySet()) {
			
			List<Integer> key = entry.getKey();
			Float value = entry.getValue();
			
			int v1 = key.get(0);
			int v2 = key.get(1);
			dist[v1][v2] = value;
			dist[v2][v1] = value;
			
			next[v1][v1] = v1;
			next[v2][v2] = v2;
			next[v1][v2] = v2;
			next[v2][v1] = v1;
		}
		
		endpoints = new HashSet<Integer>();
		branchpoints = new HashSet<Integer>();
	}
	
	public FloydWarshall(Map<List<Integer>,Float> adjacency) {
		this(getNumberOfVertices(adjacency), adjacency);
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
	void getEndpointsAndBranchpoints(){
		endpoints.clear();
		branchpoints.clear();
		double sqrt3 = Math.sqrt(3);
		for(int i=0; i<dist.length; i++) {
			int numConnections=0;
			for(int j=0; j<dist.length; j++) {
				if (dist[i][j]<=sqrt3 && dist[i][j]>0) {//For voxel image, where can be diagonally connected, then two voxels are directly connected if the distance is less than sqrt(3)
					numConnections++;
				}
				if(numConnections==3) {//considered branchpoint if it has at least 3 connections
					break;
				}
			}
			if(numConnections==1) {
				endpoints.add(i);
			}
			else if(numConnections==3) {
				branchpoints.add(i);
			}
		}
	}
	
	public List<Integer> getShortestPath(int start, int end) {
		List<Integer> shortestPath = new ArrayList<Integer>();
		shortestPath.add(start);
		if (next[start][end] != -1) {
				int i  = start;
				int j = end;
				while(i != j) {
					i = next[i][j];
					shortestPath.add(i);
				}
		}
		return shortestPath;
	}
	
   public void calculateLongestShortestPathStartEndAndLength() {
	    longestShortestPathLength = -1;
		for(int start=0; start<dist.length; start++) {
			for(int end=0; end<=start; end++) {
				if(dist[start][end]>longestShortestPathLength && dist[start][end]<Float.MAX_VALUE) {
					longestShortestPathStart = start;
					longestShortestPathEnd = end;
					longestShortestPathLength = dist[start][end];	
				}
			}
		}		
   }
	
   	public void pruneAndCalculateLongestShortestPathInformation(float minLength) {
		calculateFloydWarshallPathsOriginal();
		calculateLongestShortestPathStartEndAndLength();
		
		int numPrunedPrev = -1;
		if(longestShortestPathLength>3*minLength) {//then can prune
			while(numPrunedPrev!=prunedVertices.size() ) { //Then some were removed so need to prune again
				numPrunedPrev = prunedVertices.size();
				pruneSkeletons(minLength);
			}
		}
		
		if(prunedVertices.size()>0) {//for visualization we are using the pruned version of the skeleton, but otherwise we use the original longest shortest path for skeletonization
			calculateLongestShortestPathStartEndAndLength(); //recalculate longest shortest path after pruning
		}
		
		longestShortestPath = getShortestPath(longestShortestPathStart, longestShortestPathEnd);//longest shortest path;
		longestShortestPathNumVertices = longestShortestPath.size();
		System.out.println("Number of vertices: "+dist.length+ ". Longest shortest path: (Start,End,Vertices,Length): "+"("+longestShortestPathStart + ", " + longestShortestPathEnd+", "+ longestShortestPathNumVertices+ ", "+ longestShortestPathLength + ")");
	}
   
	public void calculateLongestShortestPathInformation() {
		calculateFloydWarshallPathsOriginal();
		calculateLongestShortestPathStartEndAndLength();
		longestShortestPath = getShortestPath(longestShortestPathStart, longestShortestPathEnd);//longest shortest path;
		longestShortestPathNumVertices = longestShortestPath.size();
		System.out.println("Number of vertices: "+dist.length+ ". Longest shortest path: (Start,End,Vertices,Length): "+"("+longestShortestPathStart + ", " + longestShortestPathEnd+", "+ longestShortestPathNumVertices+ ", "+ longestShortestPathLength + ")");

	}
	 
	public void pruneSkeletons(float minLength) {
		getEndpointsAndBranchpoints();
		
		Integer shortestPathBranchpoint = -1;
		for(Integer shortestPathEndpoint : endpoints) { //for each endpoint, find nearest branchpoint. if the distance is below the cutoff, then prune the branch
			float shortestPathLength = Float.MAX_VALUE;
			for(Integer branchpoint : branchpoints) {
				if(dist[shortestPathEndpoint][branchpoint]<shortestPathLength) {
					shortestPathLength = dist[shortestPathEndpoint][branchpoint];
					shortestPathBranchpoint  = branchpoint;
				}
			}
			
			if(shortestPathLength!=-1 && shortestPathLength<=minLength) { //then there is a branch that should be removed
				removeBranch(shortestPathEndpoint, shortestPathBranchpoint);
			}
			
		}
		
	}
	
	public void removeBranch(Integer shortestPathEndpoint, Integer shortestPathBranchpoint) {
		List<Integer> shortestPath = getShortestPath(shortestPathEndpoint, shortestPathBranchpoint);
		shortestPath.remove(shortestPathBranchpoint); // don't want to remove branchpoint
		for(Integer vertexOnBranch : shortestPath) {
			for(int i=0; i<dist.length; i++) {
				prunedVertices.add(vertexOnBranch);
				dist[vertexOnBranch][i] = Float.MAX_VALUE;
				dist[i][vertexOnBranch] = Float.MAX_VALUE;
				
				next[vertexOnBranch][i] = -1;
				next[i][vertexOnBranch] = -1;
			}
		}
	}
	
	public static void main(final String[] args) throws IOException {

		//System.out.println(vertexCount);
		Map<List<Integer>,Float> distanceMap = new HashMap<>(); 
		int numVertices = 106;
		for(int i = 0; i<100;i++) {
			distanceMap.put(Arrays.asList(i,i+1),1.0f);
		}
		
		distanceMap.put(Arrays.asList(20,101),1.0f);
		distanceMap.put(Arrays.asList(101,102),1.0f);
		distanceMap.put(Arrays.asList(102,103),1.0f);
		distanceMap.put(Arrays.asList(103,104),1.0f);
		distanceMap.put(Arrays.asList(102,105),1.0f);

		/*distanceMap.put(Arrays.asList(1,11),1.0f);
		distanceMap.put(Arrays.asList(0,11),1.0f);
		distanceMap.put(Arrays.asList(11,12),1.0f);
		distanceMap.put(Arrays.asList(12,13),1.0f);*/
						
		FloydWarshall fw = new FloydWarshall(numVertices,distanceMap);
		long tic = System.currentTimeMillis();
		//as.calculateFloydWarshallPathsOriginal();
		fw.calculateLongestShortestPathInformation();
		fw = new FloydWarshall(numVertices,distanceMap);
		fw.pruneAndCalculateLongestShortestPathInformation(10);
		System.out.println(fw.prunedVertices);
		System.out.println("time: "+(System.currentTimeMillis()-tic));	
		
	}
	

	
}
