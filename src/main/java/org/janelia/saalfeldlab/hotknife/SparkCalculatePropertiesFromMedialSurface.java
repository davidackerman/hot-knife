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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
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

import javax.swing.text.html.StyleSheet.ListPainter;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.hotknife.util.Grid;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
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
import net.imglib2.img.Img;
import net.imglib2.img.NativeImg;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.type.NativeType;
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
public class SparkCalculatePropertiesFromMedialSurface {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputDirectory", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputDirectory = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;
		
		@Option(name = "--minimumBranchLength", required = false, usage = "Minimum branch length (nm)")
		private float minimumBranchLength = 80;

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
		
		public float getMinimumBranchLength() {
			return minimumBranchLength;
		}

	}

	/*
	public static final ObjectwiseSkeletonInformation getObjectwiseSkeletonInformation(
			final JavaSparkContext sc,
			final String n5Path,
			final String datasetName,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);		
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);

		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final int n = dimensions.length;
		
		
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<ObjectwiseSkeletonInformation> javaRDDBlockwiseSkeletonInformation = rdd.map(blockInformation -> {
			final long [][] gridBlock = blockInformation.gridBlock;
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			long [] offset = gridBlock[0];
			long [] dimension = gridBlock[1];
			
			boolean show=false;
			if(show) new ImageJ();
			RandomAccessibleInterval<UnsignedLongType> connectedComponents = (RandomAccessibleInterval)N5Utils.open(n5BlockReader, datasetName);
			
			NativeImg<FloatType, ?> distanceTransform = null;
			
			long [] padding = getCorrectlyPaddedDistanceTransform(connectedComponents, distanceTransform, offset, dimension);
			//final IntervalView<FloatType> insideBlock = Views.offsetInterval(Views.extendZero(distanceTransform), minInside, dimensionsInside);

			//now distance transform is sufficient, but we still may have a situation where the closest medial surface requires crossing out of the object, so need to check.
			
			updateDistanceTransformToPreventCrossingObjectBoundary(connectedComponents, distanceTransform, offset, dimension, padding);
			
			final IntervalView<UnsignedLongType> connectedComponentsCropped = Views.offsetInterval(Views.extendZero(connectedComponents), paddedOffset, paddedDimension);
			final IntervalView<UnsignedLongType> skeleton = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5BlockReader, datasetName+"_blahblah")
					),paddedOffset, paddedDimension);
			IntervalView<FloatType> distanceTransformCropped = Views.offsetInterval(Views.extendZero(distanceTransform), new long[] {minInside[0]-1, minInside[1]-1,minInside[2]-1}, new long[] {dimensionsInside[0]+2, dimensionsInside[1]+2, dimensionsInside[2]+2});		
			if(show) {
				ImageJFunctions.show(connectedComponentsCropped);
				ImageJFunctions.show(skeleton);
				ImageJFunctions.show(distanceTransformCropped);
			}
			
			final RandomAccess<UnsignedLongType> skeletonRandomAccess = skeleton.randomAccess();
			final RandomAccess<FloatType> distanceTransformRandomAccess = distanceTransformCropped.randomAccess();

			ObjectwiseSkeletonInformation currentBlockObjectwiseSkeletonInformation = new ObjectwiseSkeletonInformation();
			
			show = false;
			if(show) {
				new ImageJ();
				//ImageJFunctions.show(connectedComponentsCropped);
				ImageJFunctions.show(skeleton);
				//ImageJFunctions.show(distanceTransformCropped);
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
	*/
	public static long [] getCorrectlyPaddedDistanceTransform(RandomAccessibleInterval<UnsignedLongType> source, NativeImg<FloatType, ?> distanceTransform, long[] offset, long[] dimension){
		long[] sourceDimensions = {0,0,0};
		source.dimensions(sourceDimensions);
		final RandomAccessibleInterval<NativeBoolType> sourceBinarized = Converters.convert(
				source,
				(a, b) -> {
					b.set(a.getIntegerLong()<1);
				},
				new NativeBoolType());
		
		final long[] initialPadding = {16,16,16};
		long[] padding = initialPadding.clone();
		final long[] paddedBlockMin = new long[3];
		final long[] paddedBlockSize = new long[3];
		final long[] minInside = new long[3];
		final long[] dimensionsInside = new long[3];

		int shellPadding = 1;

		//Distance Transform
A:			for (boolean paddingIsTooSmall = true; paddingIsTooSmall; Arrays.setAll(padding, i -> padding[i] + initialPadding[i])) {

			paddingIsTooSmall = false;

			final long maxPadding =  Arrays.stream(padding).max().getAsLong();
			final long squareMaxPadding = maxPadding * maxPadding;

			Arrays.setAll(paddedBlockMin, i -> offset[i] - padding[i]);
			Arrays.setAll(paddedBlockSize, i -> dimension[i] + 2*padding[i]);
			//System.out.println(Arrays.toString(gridBlock[0]) + ", padding = " + Arrays.toString(padding) + ", padded blocksize = " + Arrays.toString(paddedBlockSize));
			
			final IntervalView<NativeBoolType> sourceBlock =
					Views.offsetInterval(
							Views.extendValue(
									sourceBinarized,
									new NativeBoolType(true)),
							paddedBlockMin,
							paddedBlockSize);
			
			/* make distance transform */				
			distanceTransform = ArrayImgs.floats(paddedBlockSize);
			
			DistanceTransform.binaryTransform(sourceBlock, distanceTransform, DISTANCE_TYPE.EUCLIDIAN);

			Arrays.setAll(minInside, i -> padding[i] );
			Arrays.setAll(dimensionsInside, i -> dimension[i] );

			final IntervalView<FloatType> insideBlock = Views.offsetInterval(Views.extendZero(distanceTransform), minInside, dimensionsInside);

			/* test whether distances at inside boundary are smaller than padding */
			for (int d = 0; d < 3; ++d) {

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
		return padding;
	}
	
	public static List<long[]> getSurfaceVoxels(RandomAccess<UnsignedLongType> sourceRA, long [] padding, long [] dimension){
		ArrayList<long[]> surfaceVoxels = new ArrayList<long[]>();
		for(long x=padding[0]; x<padding[0]+dimension[0]; x++) {
			for(long y=padding[1]; y<padding[1]+dimension[1]; y++) {
				for(long z=padding[2]; z<padding[2]+dimension[2]; z++) {
					long [] pos = new long[] {x,y,z};
					if(SparkContactSites.isSurfaceVoxel(sourceRA, pos)) {
						surfaceVoxels.add(pos);
					}
				}
			}
		}
		return surfaceVoxels;
	}
	
	public static <T extends NativeType<T>> T raSetGet(RandomAccess<T> ra, long [] pos){
		ra.setPosition(pos);
		return ra.get();
	}
	
	public static List<long[]> bressenham3D(long [] start, long [] end){
	// https://www.geeksforgeeks.org/bresenhams-algorithm-for-3-d-line-drawing/
	// Python3 code for generating points on a 3-D line  
	// using Bresenham's Algorithm 
		long x1 = start[0]; long y1 = start[1]; long z1 = start[2];
		long x2 = end[0]; long y2 = end[1]; long z2 = end[2];

	    ArrayList<long[]> listOfPoints = new ArrayList<long[]>();
	    listOfPoints.add(start);
	    long dx = Math.abs(x2 - x1);
	    long dy = Math.abs(y2 - y1);
	    long dz = Math.abs(z2 - z1);
	    
	    long xs, ys, zs;
	    if (x2 > x1) 
	        xs = 1;
	    else 
	        xs = -1;
	    if (y2 > y1) 
	        ys = 1;
	    else 
	        ys = -1;
	    if (z2 > z1) 
	        zs = 1;
	    else 
	        zs = -1;
	  
	    //# Driving axis is X-axis" 
	    if (dx >= dy && dx >= dz) {         
	        long p1 = 2 * dy - dx; 
	        long p2 = 2 * dz - dx ;
	        while (x1 != x2) { 
	            x1 += xs; 
	            if (p1 >= 0) { 
	                y1 += ys; 
	                p1 -= 2 * dx; 
	            }
	            if (p2 >= 0) { 
	                z1 += zs; 
	                p2 -= 2 * dx;
	            }
	            p1 += 2 * dy; 
	            p2 += 2 * dz; 
	    	    listOfPoints.add(new long [] {x1, y1, z1});
	        }
	    }
	    
	    //# Driving axis is Y-axis" 
	    else if (dy >= dx && dy >= dz) {        
	        long p1 = 2 * dx - dy; 
	        long p2 = 2 * dz - dy; 
	        while (y1 != y2) { 
	            y1 += ys; 
	            if (p1 >= 0){ 
	                x1 += xs; 
	                p1 -= 2 * dy; 
	            }
	            if (p2 >= 0) { 
	                z1 += zs; 
	                p2 -= 2 * dy;
	            }
	            p1 += 2 * dx; 
	            p2 += 2 * dz; 
	    	    listOfPoints.add(new long [] {x1, y1, z1});
	        }
	    }
	    //# Driving axis is Z-axis" 
	    else{         
	        long p1 = 2 * dy - dz; 
	        long p2 = 2 * dx - dz; 
	        while (z1 != z2) { 
	            z1 += zs; 
	            if (p1 >= 0) { 
	                y1 += ys; 
	                p1 -= 2 * dz;
	            }
	            if (p2 >= 0) { 
	                x1 += xs; 
	                p2 -= 2 * dz; 
	            }
	            p1 += 2 * dy; 
	            p2 += 2 * dx;
	    	    listOfPoints.add(new long [] {x1, y1, z1});
	        }
	    }
	    return listOfPoints;
	}
	
	public static boolean crossesObjectBoundary(RandomAccess<UnsignedLongType> objectsToCalculateDistanceToRA, long [] start, long [] end) {
		long objectValue = raSetGet(objectsToCalculateDistanceToRA, start).get();
		List<long[]> pointsConnectingObjectAtoObjectB = bressenham3D(start, end);
		for(long [] currentPointToCheck : pointsConnectingObjectAtoObjectB) {
			long currentPointToCheckValue = raSetGet(objectsToCalculateDistanceToRA, currentPointToCheck).get();
			if(currentPointToCheckValue!=objectValue) {//then crossed boundary
				return true;
			}
		}
		return false;
	}
	
	
	public static boolean mustCrossObjectBoundary(RandomAccess<UnsignedLongType> fromRA, RandomAccess<UnsignedLongType> toRA, long[] currentSurfaceVoxelPosition, Set<List<Integer>> deltas) throws Exception {
		boolean allLinesCrossObjectBoundary = true;
		boolean foundObjectAtDistance = false;
		for(List<Integer> currentDelta : deltas) {
			long [] posToCheck = new long [] {currentSurfaceVoxelPosition[0]+currentDelta.get(0),currentSurfaceVoxelPosition[1]+currentDelta.get(1),currentSurfaceVoxelPosition[2]+currentDelta.get(2)};
			long fromValue = raSetGet(fromRA, posToCheck).get();
			if(fromValue>0) {
				foundObjectAtDistance = true;
				if(!crossesObjectBoundary(toRA,currentSurfaceVoxelPosition,posToCheck)) {
					allLinesCrossObjectBoundary = false;
					break;
				}
			}
		}
		if(foundObjectAtDistance==false) throw new Exception("Didn't find object");
		return allLinesCrossObjectBoundary;
	}
	
	public static boolean anySurfaceVoxelPathsMustCrossBoundary(RandomAccess<UnsignedLongType> fromRA, RandomAccess<FloatType> distanceTransformRA, RandomAccess<UnsignedLongType> toRA, List<long[]> surfaceVoxels) throws Exception {
		boolean needToExpand = false;
		int numberOfSurfaceVoxelsChecked = 0;
		for(long[] currentSurfaceVoxelPosition : surfaceVoxels) {
			numberOfSurfaceVoxelsChecked++;
			float distanceSquared = raSetGet(distanceTransformRA, currentSurfaceVoxelPosition).get();
			Set<List<Integer>> deltas = SparkContactSites.getVoxelsToCheckBasedOnDistance(distanceSquared);
			
			if(mustCrossObjectBoundary(fromRA, toRA, currentSurfaceVoxelPosition, deltas)) {//then always had to cross object boundary
				for(int i=0; i<numberOfSurfaceVoxelsChecked; i++) { //remove all checked ones but the one that failed
					surfaceVoxels.remove(i);
				}
				needToExpand = true;
				break; 
			}
			
		}
		return needToExpand;
	}
	
	public static void updateDistanceTransformToPreventCrossingObjectBoundary(RandomAccessibleInterval<UnsignedLongType> from, IntervalView<FloatType> distanceTransform, RandomAccessibleInterval<UnsignedLongType> to, long[] offset, long[] dimension, long[] padding) throws Exception{
		RandomAccess<UnsignedLongType> fromRA = from.randomAccess();	
		RandomAccess<FloatType> distanceTransformRA = distanceTransform.randomAccess();
		RandomAccess<UnsignedLongType> toRA = to.randomAccess();	

		List<long[]> surfaceVoxels = getSurfaceVoxels(toRA, padding, dimension);
		boolean needToExpand = true;
		while(needToExpand) {
			needToExpand = anySurfaceVoxelPathsMustCrossBoundary(fromRA, distanceTransformRA, toRA, surfaceVoxels);
		}
	}
	
	public static List<BlockInformation> buildBlockInformationList(final String inputN5Path,
			final String inputN5DatasetName) throws IOException {
		//Get block attributes
		N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();
		
		//final long[] outputDimensions = new long[] {501,501,501};
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

		final SparkConf conf = new SparkConf().setAppName("SparkLengthAndThickness");
		
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
			
			
			
			sc.close();
		}
		//Remove temporary files
		SparkDirectoryDelete.deleteDirectories(conf, directoriesToDelete);

	}
	
}

