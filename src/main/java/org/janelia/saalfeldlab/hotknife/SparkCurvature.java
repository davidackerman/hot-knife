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
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.hotknife.util.Grid;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.common.io.Files;

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.algorithm.neighborhood.Neighborhood;
import net.imglib2.algorithm.neighborhood.RectangleShape;
import net.imglib2.algorithm.neighborhood.Shape;

/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkCurvature {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5DatasetSuffix", required = false, usage = "N5 suffix, e.g. _cc so output would be /mito_cc")
		private String outputN5DatasetSuffix = "_curvature";

		public Options(final String[] args) {

			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);

				if (outputN5Path == null)
					outputN5Path = inputN5Path;

				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				System.err.println(e.getMessage());
				parser.printUsage(System.err);
			}
		}

		public String getInputN5Path() {
			return inputN5Path;
		}

		public String getInputN5DatasetName() {
			return inputN5DatasetName;
		}

		public String getOutputN5DatasetSuffix() {
			return outputN5DatasetSuffix;
		}

		public String getOutputN5Path() {
			return outputN5Path;
		}

	}

	

	public static final void computeCurvature(final JavaSparkContext sc, final String n5Path,
			final String inputDatasetName, final String n5OutputPath, String outputDatasetName,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final int n = dimensions.length;

		/*
		 * grid block size for parallelization to minimize double loading of blocks
		 */
		boolean show = false;
		if (show)
			new ij.ImageJ();

		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		
		n5Writer.createDataset(outputDatasetName, dimensions, blockSize, DataType.FLOAT32, new GzipCompression());

		
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		

		rdd.foreach(blockInformation -> {
			final long[][] gridBlock = blockInformation.gridBlock;
			long[] offset = gridBlock[0];//new long[] {64,64,64};//gridBlock[0];////
			//System.out.println(Arrays.toString(offset));
			long[] dimension = gridBlock[1];
			int sigma = 2; //2 because need to know if surrounding voxels are removablel
			int padding = sigma+1;//Since need extra of 1 around each voxel for curvature
			long[] paddedOffset = new long[]{offset[0]-padding, offset[1]-padding, offset[2]-padding};
			long[] paddedDimension = new long []{dimension[0]+2*padding, dimension[1]+2*padding, dimension[2]+2*padding};
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			

			RandomAccessibleInterval<UnsignedLongType> source = (RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, inputDatasetName);
			final RandomAccessibleInterval<FloatType> sourceConverted =
					Converters.convert(
							source,
							(a, b) -> { 
								if(a.getLong()>0) {
									b.set(1);
								}
								else {
									b.set(0);
									}},
							new FloatType());
			
			final IntervalView<FloatType> sourceCropped = Views.offsetInterval(Views.extendZero(sourceConverted),paddedOffset, paddedDimension);

			
			//IntervalView<FloatType> curvatureImage = Views.offsetInterval(ArrayImgs.floats(paddedDimension),new long[]{0,0,0}, paddedDimension);
				        
	        RandomAccess<FloatType> sourceRandomAccess = sourceCropped.randomAccess();
	        //RandomAccess<FloatType> curvatureImageRandomAccess = curvatureImage.randomAccess();
	        Map<List<Integer>, Float> curvaturesForBoundaryVoxels = new HashMap<>();
			IntervalView<FloatType> outputImage = Views.offsetInterval(ArrayImgs.floats(paddedDimension),new long[]{0,0,0}, paddedDimension);
	        RandomAccess<FloatType> outputImageRandomAccess = outputImage.randomAccess();
	        
			for(int x=0; x<paddedDimension[0]; x++) {
				for(int y=0; y<paddedDimension[1]; y++) {
					for(int z=0; z<paddedDimension[2]; z++) {
						int currentPos[] = new int []{x,y,z};
						sourceRandomAccess.setPosition(currentPos);
						boolean isObject =  sourceRandomAccess.get().get()==1.0;

						if(isObject && isBoundaryVoxel(sourceRandomAccess,currentPos)){
		    				float curvature = computeHessianDeterminant(sourceRandomAccess, currentPos);
		    				curvaturesForBoundaryVoxels.put(Arrays.stream(currentPos).boxed().collect(Collectors.toList()),
		    						curvature);
						}
					}
				}
			}
			for(Map.Entry<List<Integer>,Float> entries : curvaturesForBoundaryVoxels.entrySet()) {
				List<Integer> voxelOfInterestPosition = entries.getKey();
				float voxelOfInterestCurvature = entries.getValue();
				
		        Map<List<Integer>, Float> allNeighborhoodCurvatures = new HashMap<>();
		        Set<List<Integer>> currentVoxelsBeingChecked = new HashSet<>();

		        allNeighborhoodCurvatures.put(voxelOfInterestPosition, voxelOfInterestCurvature);
		        currentVoxelsBeingChecked.add(voxelOfInterestPosition);
				for(int i=0; i<sigma;i++) {
			        Set<List<Integer>> nextVoxelsToCheck = new HashSet<>();
					for(List<Integer> currentCenterVoxelPosition : currentVoxelsBeingChecked) {
						for(int dx=-1; dx<=1; dx++) {
							for(int dy=-1; dy<=1; dy++) {
								for(int dz=-1; dz<=1; dz++) {
									if(!(dx==0 && dy==0 && dz==0)) {
										List<Integer> neighboringVoxelPosition = Arrays.asList(currentCenterVoxelPosition.get(0)+dx, currentCenterVoxelPosition.get(1)+dy, currentCenterVoxelPosition.get(2)+dz);
										if(curvaturesForBoundaryVoxels.containsKey(neighboringVoxelPosition)) {
											allNeighborhoodCurvatures.put(neighboringVoxelPosition, curvaturesForBoundaryVoxels.get(neighboringVoxelPosition));
											nextVoxelsToCheck.add(neighboringVoxelPosition);
										}
									}
								}
							}
						}
					}
					currentVoxelsBeingChecked = nextVoxelsToCheck;
				}
				
				float averageCurvature = 0;
				for(float currentCurvature : allNeighborhoodCurvatures.values()) {
					averageCurvature+=currentCurvature/allNeighborhoodCurvatures.size();
				}
				int [] voxelOfInterestPositionAsArray = new int[] { voxelOfInterestPosition.get(0), voxelOfInterestPosition.get(1), voxelOfInterestPosition.get(2)};
				outputImageRandomAccess.setPosition(voxelOfInterestPositionAsArray);
				outputImageRandomAccess.get().set(averageCurvature);
				
			}
			
			
			if(show) {
				ImageJFunctions.show(sourceCropped);
				ImageJFunctions.show(outputImage);
			}
				

	
				

			outputImage = Views.offsetInterval(outputImage,new long[]{padding,padding,padding}, dimension);

			final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);

			N5Utils.saveBlock(outputImage, n5BlockWriter, outputDatasetName, gridBlock[2]);
			
		});

	}
public static boolean isBoundaryVoxel(RandomAccess<FloatType> ra, int[] pos){
	for(int dx=-1; dx<=1; dx++) {
		for(int dy=-1; dy<=1; dy++) {
			for(int dz=-1; dz<=1; dz++) {
				int newPos [] = new int[]{pos[0]+dx, pos[1]+dy, pos[2]+dz};
				ra.setPosition(newPos);
				if(ra.get().get()==0) {
					return true;
				}
			}
		}
	}
	return false;
}

public static float computeHessianDeterminant(RandomAccess<FloatType> ra, int[] pos) {
	float[][] hessianMatrix = computeHessianMatrix(ra, pos);
	float a,b,c,d,e,f,g,h,i;
	a=hessianMatrix[0][0]; b=hessianMatrix[0][1]; c=hessianMatrix[0][2];
	d=hessianMatrix[1][0]; e=hessianMatrix[1][1]; f=hessianMatrix[2][2];
	g=hessianMatrix[2][0]; h=hessianMatrix[2][1]; i=hessianMatrix[2][2];
	float determinant = a*e*i + b*f*g + c*d*h - c*e*g - b*d*i - a*f*h;
	return determinant;
}

public static float[][] computeHessianMatrix(RandomAccess<FloatType> ra, int[] pos){
	//https://searchcode.com/file/113852753/src-plugins/VIB-lib/features/ComputeCurvatures.java
	int x = pos[0];
	int y = pos[1];
	int z = pos[2];
	float [][][]neighborhood = new float[3][3][3];
	for(int dx = -1; dx<=1; dx++) {
		for(int dy=-1; dy<=1; dy++) {
			for(int dz=-1; dz<=1; dz++) {
				ra.setPosition(new int[]{x+dx, y+dy, z+dz});
				neighborhood[dx+1][dy+1][dz+1]=ra.get().get();
			}
		}
	}
		
	float[][] hessianMatrix = new float[3][3]; // zeile, spalte
	
	x=1; y=1; z=1;//to center it for calculating hessian
	float temp = 2 * neighborhood[x][y][z];
	
	// xx
	hessianMatrix[0][0] = neighborhood[x + 1][ y][ z] - temp + neighborhood[x - 1][ y][ z];
	
	// yy
	hessianMatrix[1][1] = neighborhood[x][ y + 1][ z] - temp + neighborhood[x][ y - 1][ z];
	
	// zz
	hessianMatrix[2][2] = neighborhood[x][ y][ z + 1] - temp + neighborhood[x][ y][ z - 1];
	
	// xy
	hessianMatrix[0][1] = hessianMatrix[1][0] =
	    (
	        (neighborhood[x + 1][ y + 1][ z] - neighborhood[x - 1][ y + 1][ z]) / 2
	        -
	        (neighborhood[x + 1][ y - 1][z] - neighborhood[x - 1][ y - 1][ z]) / 2
	        ) / 2;
	
	// xz
	hessianMatrix[0][2] = hessianMatrix[2][0] =
	    (
	        (neighborhood[x + 1][ y][ z + 1] - neighborhood[x - 1][ y][ z + 1]) / 2
	        -
	        (neighborhood[x + 1][ y][ z - 1] - neighborhood[x - 1][ y][ z - 1]) / 2
	        ) / 2;
	
	// yz
	hessianMatrix[1][2] = hessianMatrix[2][1] =
	    (
	        (neighborhood[x][ y + 1][ z + 1] - neighborhood[x][ y - 1][ z + 1]) / 2
	        -
	        (neighborhood[x][ y + 1][ z - 1] - neighborhood[x][ y - 1][ z - 1]) / 2
	        ) / 2;
	return hessianMatrix;
}
	public static List<BlockInformation> buildBlockInformationList(final String inputN5Path,
			final String inputN5DatasetName) throws IOException {
		// Get block attributes
		N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();

		// Build list
		List<long[][]> gridBlockList = Grid.create(outputDimensions, blockSize);
		List<BlockInformation> blockInformationList = new ArrayList<BlockInformation>();
		for (int i = 0; i < gridBlockList.size(); i++) {
			long[][] currentGridBlock = gridBlockList.get(i);
			blockInformationList.add(new BlockInformation(currentGridBlock, null, null));
		}
		return blockInformationList;
	}

	

	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkCurvature");

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

		String finalOutputN5DatasetName = null;
		for (String currentOrganelle : organelles) {
			finalOutputN5DatasetName = currentOrganelle + options.getOutputN5DatasetSuffix();
			
			// Create block information list
			List<BlockInformation> blockInformationList = buildBlockInformationList(options.getInputN5Path(),
					currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			
			
			 computeCurvature(sc, options.getInputN5Path(), currentOrganelle, options.getOutputN5Path(),
					finalOutputN5DatasetName, blockInformationList);

			sc.close();
		}

	}
}
