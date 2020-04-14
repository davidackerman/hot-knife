/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.janelia.saalfeldlab.hotknife;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.hotknife.util.Grid;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ij.ImageJ;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.converter.Converters;
import net.imglib2.img.NativeImg;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

import org.janelia.saalfeldlab.hotknife.DijkstraPriorityQueue;

/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkLengthAndThickness {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputDirectory", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputDirectory = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;

		public Options(final String[] args) {
			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);
				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				parser.printUsage(System.err);
			}
		}
		
		public String getInputN5Path() {
			return inputN5Path;
		}

		public String getInputN5DatasetName() {
			return inputN5DatasetName;
		}
		
		public String getOutputDirectory() {
			if(outputDirectory == null) {
				outputDirectory = inputN5Path.split(".n5")[0]+"_results";
			}
			return outputDirectory;
		}
		

	}

	public static final ObjectwiseSkeletonInformation getObjectwiseSkeletonInformation(
			final JavaSparkContext sc,
			final String n5Path,
			final String datasetName,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);
		double [] pixelResolution = IOHelper.getResolution(n5Reader, datasetName);
		
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
		
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final int n = dimensions.length;
		
		
		/*
		 * grid block size for parallelization to minimize double loading of
		 * blocks
		 */
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<ObjectwiseSkeletonInformation> javaRDDBlockwiseSkeletonInformation = rdd.map(blockInformation -> {
			final long [][] gridBlock = blockInformation.gridBlock;
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			boolean show=false;
			if(show) new ImageJ();
			RandomAccessibleInterval<UnsignedLongType> connectedComponents = (RandomAccessibleInterval)N5Utils.open(n5BlockReader, datasetName);
			final RandomAccessibleInterval<NativeBoolType> source = Converters.convert(
					connectedComponents,
					(a, b) -> {
						b.set(a.getIntegerLong()<1);
					},
					new NativeBoolType());
			
			long[] sourceDimensions = {0,0,0};
			source.dimensions(sourceDimensions);
			NativeImg<FloatType, ?> distanceTransform = null;
			final long[] initialPadding = {16,16,16};
			long[] padding = initialPadding.clone();
			final long[] paddedBlockMin = new long[n];
			final long[] paddedBlockSize = new long[n];
			final long[] minInside = new long[n];
			final long[] dimensionsInside = new long[n];
			
			int shellPadding = 1;

			//Distance Transform
A:			for (boolean paddingIsTooSmall = true; paddingIsTooSmall; Arrays.setAll(padding, i -> padding[i] + initialPadding[i])) {

				paddingIsTooSmall = false;
	
				final long maxPadding =  Arrays.stream(padding).max().getAsLong();
				final long squareMaxPadding = maxPadding * maxPadding;
	
				Arrays.setAll(paddedBlockMin, i -> gridBlock[0][i] - padding[i]);
				Arrays.setAll(paddedBlockSize, i -> gridBlock[1][i] + 2*padding[i]);
				//System.out.println(Arrays.toString(gridBlock[0]) + ", padding = " + Arrays.toString(padding) + ", padded blocksize = " + Arrays.toString(paddedBlockSize));
				
				final IntervalView<NativeBoolType> sourceBlock =
						Views.offsetInterval(
								Views.extendValue(
										source,
										new NativeBoolType(true)),
								paddedBlockMin,
								paddedBlockSize);
				
				/* make distance transform */				
				if(show) ImageJFunctions.show(sourceBlock, "sourceBlock");
				distanceTransform = ArrayImgs.floats(paddedBlockSize);
				
				DistanceTransform.binaryTransform(sourceBlock, distanceTransform, DISTANCE_TYPE.EUCLIDIAN);
				if(show) ImageJFunctions.show(distanceTransform,"dt");
	
				Arrays.setAll(minInside, i -> padding[i] );
				Arrays.setAll(dimensionsInside, i -> gridBlock[1][i] );
	
				final IntervalView<FloatType> insideBlock = Views.offsetInterval(Views.extendZero(distanceTransform), minInside, dimensionsInside);
				if(show) ImageJFunctions.show(insideBlock,"inside");
	
				/* test whether distances at inside boundary are smaller than padding */
				for (int d = 0; d < n; ++d) {
	
					final IntervalView<FloatType> topSlice = Views.hyperSlice(insideBlock, d, 1);
					for (final FloatType t : topSlice)
						if (t.get() >= squareMaxPadding-shellPadding) { //Subtract one from squareMaxPadding because we want to ensure that if we have a shell in later calculations for finding surface points, we can access valid points
							paddingIsTooSmall = true;
						//	System.out.println("padding too small");
							continue A;
						}
	
					final IntervalView<FloatType> botSlice = Views.hyperSlice(insideBlock, d, insideBlock.max(d));
					for (final FloatType t : botSlice)
						if (t.get() >= squareMaxPadding-shellPadding) {
							paddingIsTooSmall = true;
						//	System.out.println("padding too small");
							continue A;
						}
				}
			}
			
			long [] paddedOffset = new long[] {gridBlock[0][0]-1, gridBlock[0][1]-1, gridBlock[0][2]-1};
			long [] paddedDimension = new long[] {gridBlock[1][0]+2, gridBlock[1][1]+2, gridBlock[1][2]+2};//Extend by 1 so can get overlap region
			
			final IntervalView<UnsignedLongType> connectedComponentsCropped = Views.offsetInterval(Views.extendZero(connectedComponents), paddedOffset, paddedDimension);
			final IntervalView<UnsignedByteType> skeleton = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(n5BlockReader, datasetName+"_skeleton")
					),paddedOffset, paddedDimension);
			IntervalView<FloatType> distanceTransformCropped = Views.offsetInterval(Views.extendZero(distanceTransform), new long[] {minInside[0]-1, minInside[1]-1,minInside[2]-1}, new long[] {dimensionsInside[0]+2, dimensionsInside[1]+2, dimensionsInside[2]+2});		
			
			final RandomAccess<UnsignedLongType> connectedComponentsRandomAccess = connectedComponentsCropped.randomAccess();
			final RandomAccess<UnsignedByteType> skeletonRandomAccess = skeleton.randomAccess();
			final RandomAccess<FloatType> distanceTransformRandomAccess = distanceTransformCropped.randomAccess();

			ObjectwiseSkeletonInformation currentBlockObjectwiseSkeletonInformation = new ObjectwiseSkeletonInformation();
			for (int x=1; x<paddedDimension[0]-1; x++) {
				for(int y=1; y<paddedDimension[1]-1; y++) {
					for(int z=1; z<paddedDimension[2]-1; z++) {
						int [] pos = {x,y,z};
						
						skeletonRandomAccess.setPosition(pos);
						if(skeletonRandomAccess.get().get()>0) { //then it is skeleton
							connectedComponentsRandomAccess.setPosition(pos);
							distanceTransformRandomAccess.setPosition(pos);
							long objectID = connectedComponentsRandomAccess.get().get();
							long[] globalVoxelPosition = new long[] { pos[0]+gridBlock[0][0]-1, pos[1]+gridBlock[0][1]-1, pos[2]+gridBlock[0][2]-1};//subtract 1 because offset by -1
							
							long v1 = sourceDimensions[0] * sourceDimensions[1] * globalVoxelPosition[2] + sourceDimensions[0] * globalVoxelPosition[1] + globalVoxelPosition[0] + 1;
							
							float radius = (float) Math.sqrt(distanceTransformRandomAccess.get().get());
							currentBlockObjectwiseSkeletonInformation.addRadius(objectID, v1, radius);
							for(int dx=-1; dx<=1; dx++) { //Check for edges
								for(int dy=-1; dy<=1; dy++) {
									for(int dz=-1; dz<=1; dz++) { //DGA:  redundant checking but necessary because we need to check corners TODO could optimize
										if(!(dx==0 && dy==0 && dz==0)) {
											//has a pair
											int [] newPos = {pos[0]+dx, pos[1]+dy, pos[2]+dz};
											skeletonRandomAccess.setPosition(newPos);
											if (skeletonRandomAccess.get().get()>0) {
												connectedComponentsRandomAccess.setPosition(newPos);
												if(connectedComponentsRandomAccess.get().get() == objectID) {//then same object
													globalVoxelPosition = new long[] { newPos[0]+gridBlock[0][0]-1, newPos[1]+gridBlock[0][1]-1, newPos[2]+gridBlock[0][2]-1};//subtract 1 because offset by -1
													long v2 = sourceDimensions[0] * sourceDimensions[1] * globalVoxelPosition[2] + sourceDimensions[0] * globalVoxelPosition[1] + globalVoxelPosition[0] + 1;
													float edgeWeight = (float)Math.sqrt(dx*dx+dy*dy+dz*dz);
													
													currentBlockObjectwiseSkeletonInformation.addSkeletonEdge(objectID, Math.min(v1,v2), Math.max(v1,v2), edgeWeight);
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			
						
			return currentBlockObjectwiseSkeletonInformation;
		
		});
		
		ObjectwiseSkeletonInformation objectwiseSkeletonInformation = javaRDDBlockwiseSkeletonInformation.reduce((a,b) -> {
			a.merge(b);
			return a;
		});
		
		for(Map.Entry<Long,SkeletonInformation> entry : objectwiseSkeletonInformation.skeletonInformationByObjectID.entrySet()) {
			SkeletonInformation value = entry.getValue();
			System.out.println("info " + entry.getKey()+" "+value.vertexRadii.size()+" "+value.listOfSkeletonEdges.size());
			
		}
		return objectwiseSkeletonInformation;
	}

	public static void calculateObjectwiseLengthAndThickness(final JavaSparkContext sc,
		ObjectwiseSkeletonInformation objectwiseSkeletonInformation, 
		String outputDirectory) {
		ArrayList<SkeletonInformation> listOfObjectwiseSkeletonInformation= objectwiseSkeletonInformation.asList();
		final JavaRDD<SkeletonInformation> rdd = sc.parallelize(listOfObjectwiseSkeletonInformation);
		JavaRDD<SkeletonInformation> javaRDDObjectInformation = rdd.map(skeletonInformation -> {
			System.out.println(skeletonInformation.objectID);
			skeletonInformation.calculateLongestShortestPath();		
			return skeletonInformation;
		});
		
		List<SkeletonInformation> allObjectSkeletonInformation = javaRDDObjectInformation.collect();
		
		boolean demo = true;
		ArrayImg<UnsignedByteType, ByteArray> skeleton =null;
		RandomAccess<UnsignedByteType> skeletonRandomAccess = null;
		if(demo) {
			new ImageJ();
			skeleton = ArrayImgs.unsignedBytes(new long[] {501,501,501});
			skeletonRandomAccess = skeleton.randomAccess();
		}
		for(SkeletonInformation skeletonInformation : allObjectSkeletonInformation) {
			Long objectID = skeletonInformation.objectID;			
			String outputString = objectID+" "+skeletonInformation.longestShortestPathLength+" "+skeletonInformation.radiusMean+" "+skeletonInformation.radiusStd;
			System.out.println(outputString);
			if(demo) {
				/*for(long vertexID : skeletonInformation.vertexRadii.keySet()) {
					skeletonRandomAccess.setPosition(convertIDtoXYZ(vertexID));
					skeletonRandomAccess.get().set(128);
					}*/
				for(long vertexID : skeletonInformation.longestShortestPath) {
					skeletonRandomAccess.setPosition(convertIDtoXYZ(vertexID));
					skeletonRandomAccess.get().set((int)Math.round(skeletonInformation.vertexRadii.get(vertexID)));
				}
			}
		}
		
		if(demo) {
			ImageJFunctions.show(skeleton);
		}
		
	}
	
	public static int[] convertIDtoXYZ(long vertexID) {
		int z = (int)Math.floor((vertexID-1)/(501*501));
		int y = (int)Math.floor((vertexID-1 - z*(501*501))/501);
		int x = (int)Math.floor((vertexID-1 - z*(501*501) - y*501));
		//System.out.println(x+" "+y+" "+z);
		return new int[] {x,y,z};
	}
	
	public static List<BlockInformation> buildBlockInformationList(final String inputN5Path,
			final String inputN5DatasetName) throws IOException {
		//Get block attributes
		N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();
		
		//Build list
		List<long[][]> gridBlockList = Grid.create(outputDimensions, blockSize);
		List<BlockInformation> blockInformationList = new ArrayList<BlockInformation>();
		for (int i = 0; i < gridBlockList.size(); i++) {
			long[][] currentGridBlock = gridBlockList.get(i);
			blockInformationList.add(new BlockInformation(currentGridBlock, null, null));
		}
		return blockInformationList;
	}

	

	public static void logMemory(final String context) {
		final long freeMem = Runtime.getRuntime().freeMemory() / 1000000L;
		final long totalMem = Runtime.getRuntime().totalMemory() / 1000000L;
		logMsg(context + ", Total: " + totalMem + " MB, Free: " + freeMem + " MB, Delta: " + (totalMem - freeMem)
				+ " MB");
	}

	public static void logMsg(final String msg) {
		final String ts = new SimpleDateFormat("HH:mm:ss").format(new Date()) + " ";
		System.out.println(ts + " " + msg);
	}
	
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkCurvatureOnSurface");

		// Get all organelles
		String[] organelles = { "" };
		if (options.getInputN5DatasetName() != null) {
			organelles = options.getInputN5DatasetName().split(",");
		} else {
			File file = new File(options.getInputN5Path());
			organelles = file.list(new FilenameFilter() {
				@Override
				public boolean accept(File current, String name) {
					return new File(current, name).isDirectory();
				}
			});
		}

		System.out.println(Arrays.toString(organelles));

		List<String> directoriesToDelete = new ArrayList<String>();
		for (String currentOrganelle : organelles) {
			logMemory(currentOrganelle);	
			
			//Create block information list
			List<BlockInformation> blockInformationList = buildBlockInformationList(options.getInputN5Path(),
				currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			ObjectwiseSkeletonInformation objectwiseSkeletonInformation = getObjectwiseSkeletonInformation(
					sc,
					options.getInputN5Path(),
					options.getInputN5DatasetName(),
					blockInformationList);
			
			calculateObjectwiseLengthAndThickness(sc, objectwiseSkeletonInformation, options.getOutputDirectory() );
			
			sc.close();
		}
		//Remove temporary files
		SparkDirectoryDelete.deleteDirectories(conf, directoriesToDelete);

	}
	
}

class SkeletonEdge implements Serializable{
	  final long v1;
	  final long v2;
	  final float edgeWeight;
	  SkeletonEdge(long v1, long v2, float edgeWeight) {this.v1=v1; this.v2=v2; this.edgeWeight = edgeWeight; }
	  public long getV1() {return v1;}
	  public long getV2() {return v2;}
	  public float getEdgeWeight() {return edgeWeight;}

}

class SkeletonInformation implements Serializable{
	long objectID;
	
	List<SkeletonEdge> listOfSkeletonEdges;
	Map<Long,Float> vertexRadii;
	HashSet<Long> longestShortestPath;
	
	float longestShortestPathLength;
	float radiusMean;
	float radiusStd;
	
	public SkeletonInformation(long objectID){
		this.objectID = objectID;
		this.listOfSkeletonEdges = new ArrayList<SkeletonEdge>();
		this.vertexRadii = new HashMap<Long, Float>();
		this.longestShortestPath = new HashSet();
	}
	
	public void merge(SkeletonInformation newSkeletonInformation) {
		listOfSkeletonEdges.addAll(newSkeletonInformation.listOfSkeletonEdges);
		vertexRadii.putAll(newSkeletonInformation.vertexRadii);
	}
	
	public void calculateLongestShortestPath(){
		Map<Integer, Long> objectVertexIDtoGlobalVertexID = new HashMap<Integer, Long>();
		Map<Long, Integer> globalVertexIDtoObjectVertexID = new HashMap<Long, Integer>();

		int vObject=0;
		for (long v : vertexRadii.keySet()) {	
			objectVertexIDtoGlobalVertexID.put(vObject, v);
			globalVertexIDtoObjectVertexID.put(v, vObject);			
			vObject++;
		}
		
		Map<List<Integer>,Float> adjacency = new HashMap<>();
		for (SkeletonEdge skeletonEdge : listOfSkeletonEdges) {	
			int v1Object = globalVertexIDtoObjectVertexID.get( skeletonEdge.getV1() );
			int v2Object = globalVertexIDtoObjectVertexID.get( skeletonEdge.getV2() );
			float edgeWeight = skeletonEdge.getEdgeWeight();
			
			adjacency.put(Arrays.asList(v1Object,v2Object),edgeWeight);
		}
		
		FloydWarshall floydWarshall = new FloydWarshall(adjacency, vObject);
		floydWarshall.calculateLongestShortestPathInformation();
		longestShortestPathLength = floydWarshall.longestShortestPathLength;
		
		float[] radius = new float[floydWarshall.longestShortestPath.size()];
		radiusMean = 0;
		
		int count = 0;
		for(Integer currentVObjectID : floydWarshall.longestShortestPath) {
			long currentVGlobalID = objectVertexIDtoGlobalVertexID.get(currentVObjectID);
			longestShortestPath.add(currentVGlobalID);
			
			
			float currentRadius = vertexRadii.get(currentVGlobalID);

			radiusMean += currentRadius;
			radius[count] = currentRadius;
			count++;
		}
		radiusMean /= floydWarshall.longestShortestPathNumVertices;
		
		radiusStd=0;
		for(float currentRadius : radius) {
			float diff = (radiusMean-currentRadius);
			radiusStd+=diff*diff;
		}
		radiusStd = (float) Math.sqrt(radiusStd/(floydWarshall.longestShortestPathNumVertices-1));
	}
	
}

//Class to represent a node in the graph 
class ObjectwiseSkeletonInformation implements Serializable{
	//use map to associate object ID with radii, edges etc
	public Map<Long, SkeletonInformation> skeletonInformationByObjectID;
	
	public ObjectwiseSkeletonInformation() 
	{ 
		this.skeletonInformationByObjectID = new HashMap<Long, SkeletonInformation>();
	}
	
	public void addSkeletonEdge(long objectID, long v1, long v2, float edgeWeight) {
		//System.out.println(objectID+" "+" "+v1+" "+v2+" "+edgeWeight);
		SkeletonInformation currentSkeletonInformation = skeletonInformationByObjectID.getOrDefault(objectID, new SkeletonInformation(objectID));
		currentSkeletonInformation.listOfSkeletonEdges.add(new SkeletonEdge(v1,v2,edgeWeight));
		skeletonInformationByObjectID.put(objectID, currentSkeletonInformation);
	}
	
	public void addRadius(long objectID, long v, float radius) {
		//System.out.println(objectID+" "+" "+v+" "+radius);
		SkeletonInformation currentSkeletonInformation = skeletonInformationByObjectID.getOrDefault(objectID, new SkeletonInformation(objectID));
		currentSkeletonInformation.vertexRadii.put(v, radius);
		skeletonInformationByObjectID.put(objectID, currentSkeletonInformation);
	}
	
	public void merge(ObjectwiseSkeletonInformation newBlockwiseSkeletonInformation) {
		for (Map.Entry<Long, SkeletonInformation> entry : newBlockwiseSkeletonInformation.skeletonInformationByObjectID.entrySet()) {
			Long objectID = entry.getKey();
			SkeletonInformation newSkeletonInformation = entry.getValue();
			
			//get it if it exists
			SkeletonInformation currentSkeletonInformation = skeletonInformationByObjectID.getOrDefault(objectID, new SkeletonInformation(objectID));
			currentSkeletonInformation.merge(newSkeletonInformation);
			
			skeletonInformationByObjectID.put(objectID, currentSkeletonInformation);
		}
	}
	
	public ArrayList<SkeletonInformation> asList(){
		return new ArrayList<SkeletonInformation>(skeletonInformationByObjectID.values());
	} 
}
