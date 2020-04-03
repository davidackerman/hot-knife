package org.janelia.saalfeldlab.hotknife;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.math3.analysis.solvers.NewtonSolver;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.EM;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import ij.IJ;
import ij.ImagePlus;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.util.unionfind.IntArrayRankedUnionFind;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.view.Views;

public class AnalyzeSkeletonDGA {
	//Floyd Warshall algorithm for symmetric adjacency matrix based on wiki: https://stackoverflow.com/questions/2037735/optimise-floyd-warshall-for-symmetric-adjacency-matrix
	
	public float dist[][];
	public int next[][];
	final Set<Long> V = new HashSet<>(); 
	public float longestShortestPathLength;
	public  int longestShortestPathI = -1;
	public int longestShortestPathJ = -1;
	public List<Integer> longestShortestPath = new ArrayList();
 
	
	public AnalyzeSkeletonDGA(Map<List<Long>,Float> distanceMap) {
		
		final Map<Long,Integer> renumberedVertices = new HashMap<>();
		int count = 0;
		for(List<Long> key : distanceMap.keySet()) {
			for(Long V : key) {
				if( renumberedVertices.putIfAbsent(V, count) == null){//then was added
					count++;	
				}
			}
		}
		
		System.out.println(count);
		
		this.dist = new float[renumberedVertices.size()][renumberedVertices.size()];
		for (float[] row: dist)
		    Arrays.fill(row, Float.MAX_VALUE);
		
		this.next = new int[renumberedVertices.size()][renumberedVertices.size()];
		for (int[] row: next)
		    Arrays.fill(row, -1);

		for(Entry<List<Long>,Float> entry : distanceMap.entrySet()) {
			
			List<Long> key = entry.getKey();
			Float value = entry.getValue();
			
			//int renumberedV1 = renumberedVertices.get( key.get(0) );
			//int renumberedV2 = renumberedVertices.get( key.get(1) );
			long V1 = key.get(0);
			long V2 = key.get(1);
			int renumberedV1 = (int) V1;
			int renumberedV2 = (int) V2;
			
			dist[renumberedV1][renumberedV1] = 0;
			dist[renumberedV2][renumberedV2] = 0;
			dist[renumberedV1][renumberedV2] = value;
			dist[renumberedV2][renumberedV1] = value;
			
			next[renumberedV1][renumberedV1] = renumberedV1;
			next[renumberedV2][renumberedV2] = renumberedV2;
			next[renumberedV1][renumberedV2] = renumberedV2;
			next[renumberedV2][renumberedV1] = renumberedV1;
		}

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
		           // dist[j][i] = dist[i][j] = Math.min(dist[i][j], dist[i][k] + dist[k][j]);
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
	}
	
    		
	public void calculateLongestShortestPathLength() {
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
		System.out.println(longestShortestPathI + " " + longestShortestPathJ+" "+longestShortestPathLength);
		System.out.println(longestShortestPath);

	}
	 

	public static void main(final String[] args) throws IOException {
		/*final int[] dimensions = new int[] { 400, 320 };
        final Img< UnsignedByteType > img1 = new ArrayImgFactory< UnsignedByteType >()
            .create( dimensions, new UnsignedByteType() );
 
        final RandomAccess< UnsignedByteType > r = img1.randomAccess();
        final Random random = new Random();
        for ( int i = 0; i < 1000; ++i )
        {
            final int x = ( int ) ( random.nextFloat() * img1.max( 0 ) );
            final int y = ( int ) ( random.nextFloat() * img1.max( 1 ) );
            r.setPosition( x, 0 );
            r.setPosition( y, 1 );
            final UnsignedByteType t = r.get();
            t.set( 255 );
        }
		
		
		final N5FSReader n5 = new N5FSReader("/groups/cosem/cosem/ackermand/HeLa_Cell3_4x4x4nm_it450000_crop_analysis.n5/");
		final RandomAccessibleInterval<UnsignedByteType> img = N5Utils.open(n5, "mito_cc_skeletonIndependent");
		RandomAccess<UnsignedByteType> imgRandomAccess= img.randomAccess();
		
		System.out.println(img.dimension(0)+" "+img.dimension(1)+" "+img.dimension(2));
		*/
		/*int edgeCount = 0, vertexCount = 0;
		{
			int [] pos = new int[] {501,501,501};
			imgRandomAccess.setPosition(pos);
			System.out.println(imgRandomAccess.get().get());
		}
		{
			int x=501, y=501, z=501;
			imgRandomAccess.setPosition(new int[] {x,y,z});
			System.out.println(imgRandomAccess.get().get());
		}
		{
			int x=501, y=501, z=501;
			int [] pos = new int[] {x,y,z};
			imgRandomAccess.setPosition(pos);
			System.out.println(imgRandomAccess.get().get());
		}
		
		/*for( int x=0; x<img.dimension(0); x++) {
			for(int y=0; y<img.dimension(1); y++) {
				for(int z=0; z<img.dimension(2); z++) {
					//System.out.println(z);
					int [] pos = new int[] {x,y,z};
					imgRandomAccess.setPosition(pos);
					if(imgRandomAccess.get().get()>0) { 
						vertexCount++;
					}
				}
				
			}
		}*/
		int vertexCount = 0;

		//System.out.println(vertexCount);
		Map<List<Long>,Float> distanceMap = new HashMap<>();

		
		
		
        
		vertexCount = 0;
		int edgeCount = 0;
		/*for(int x=0; x<img.dimension(0); x++) {
			for(int y=0; y<img.dimension(1); y++) {
				for(int z=0; z<img.dimension(2); z++) {
					
					int [] pos = new int[] {x,y,z};
					imgRandomAccess.setPosition(pos);
					long V1 = img.dimension(0) * img.dimension(1) * pos[2] + img.dimension(0) * pos[1] + pos[0] + 1;
					
					if(imgRandomAccess.get().get()>0) { //is part of skeleton
						vertexCount++;
						for(int dx = -1; dx<=1; dx ++) {
							for(int dy = -1; dy<=1; dy++) {
								for(int dz = -1; dz<=1; dz++) {
									int [] newPos = new int[] {x+dx, y+dy, z+dz};
									imgRandomAccess.setPosition(newPos);
									if (!(dx==0 && dy==0 && dz==0) && (newPos[0]<img.dimension(0) && newPos[1]<img.dimension(1) && newPos[2] < img.dimension(2) &&
											(newPos[0]>0 && newPos[1]>0 && newPos[2] >0)) && imgRandomAccess.get().get()>0) {//also part of skeleton
										long V2 = img.dimension(0) * img.dimension(1) * newPos[2] + img.dimension(0) * newPos[1] + newPos[0] + 1;
										float distance = (float) Math.sqrt(dx^2+dy^2+dz^2);
										if(V2<V1)
											distanceMap.put(Arrays.asList(V2,V1), distance);
										else 
											distanceMap.put(Arrays.asList(V1,V2), distance) ;
										
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
				
		System.out.println("vertex count: "+vertexCount);
		System.out.println("edge count: "+edgeCount);
		*/
		for(long i = 0; i<2000;i++) {
			distanceMap.put(Arrays.asList(i,i+1),1.0f);
			/*distanceMap.put(Arrays.asList(i,i+2),1.0f);
			distanceMap.put(Arrays.asList(i,i+3),1.0f);
			distanceMap.put(Arrays.asList(i,i+4),1.0f);
			distanceMap.put(Arrays.asList(i,i+5),1.0f);
			distanceMap.put(Arrays.asList(i,i+6),1.0f);
			distanceMap.put(Arrays.asList(i,i+7),1.0f);
			distanceMap.put(Arrays.asList(i,i+8),1.0f);*/
		}
						
		AnalyzeSkeletonDGA as = new AnalyzeSkeletonDGA(distanceMap);
		long tic = System.currentTimeMillis();
		as.calculateFloydWarshallPathsOriginal();
		as.calculateLongestShortestPathLength();
		System.out.println("time: "+(System.currentTimeMillis()-tic));

		
		/*AnalyzeSkeletonDGA asOriginal = new AnalyzeSkeletonDGA(distanceMap);
		tic = System.currentTimeMillis();
		asOriginal.calculateFloydWarshallPathsOriginal();
		System.out.println(System.currentTimeMillis()-tic);*/

		
		
	}
	

	
}
