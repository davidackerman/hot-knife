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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.hotknife.util.Grid;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.spark_project.guava.collect.Sets;

import ij.ImageJ;
import net.imagej.ops.Ops.Filter.Gauss;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.algorithm.neighborhood.Shape;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.*;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

import org.janelia.saalfeldlab.hotknife.IOHelper;
import org.janelia.saalfeldlab.hotknife.ops.*;

import net.imglib2.type.logic.NativeBoolType;


/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkCellVolume {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--ecsN5Path", required = true, usage = "input N5 path, e.g. /path/to/input/predictions.n5")
		private String ecsN5Path = null;

		@Option(name = "--plasmaMembraneN5Path", required = false, usage = "N5 dataset, e.g. organelle")
		private String plasmaMembraneN5Path = null;
		
		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /path/to/output/connected_components.n5")
		private String outputN5Path = null;

		@Option(name = "--thresholdDistance", required = false, usage = "Distance for thresholding (positive inside, negative outside) (nm)")
		private double thresholdDistance = 0;
		
		@Option(name = "--minimumVolumeCutoff", required = false, usage = "Volume above which objects will be kept (nm^3)")
		private double minimumVolumeCutoff = 20E6;
		
		@Option(name = "--onlyKeepLargestComponent", required = false, usage = "Keep only the largest connected component")
		private boolean onlyKeepLargestComponent = false;

		@Option(name = "--skipSmoothing", required = false, usage = "Keep only the largest connected component")
		private boolean skipSmoothing = false;
		
		public Options(final String[] args) {

			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);

				if (outputN5Path == null)
					outputN5Path = plasmaMembraneN5Path;

				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				System.err.println(e.getMessage());
				parser.printUsage(System.err);
			}
		}

		public String getEcsN5Path() {
			return ecsN5Path;
		}

		public String getPlasmaMembraneN5Path() {
			return plasmaMembraneN5Path;
		}

		public String getOutputN5Path() {
			return outputN5Path;
		}


		public double getThresholdDistance() {
			return thresholdDistance;
		}
		
		public double getMinimumVolumeCutoff() {
			return minimumVolumeCutoff;
		}
		
		public boolean getOnlyKeepLargestComponent() {
			return onlyKeepLargestComponent;
		}
	}

	
	/**
	 * Find connected components on a block-by-block basis and write out to
	 * temporary n5.
	 *
	 * Takes as input a threshold intensity, above which voxels are used for
	 * calculating connected components. Parallelization is done using a
	 * blockInformationList.
	 *
	 * @param sc
	 * @param inputN5Path
	 * @param inputN5DatasetName
	 * @param outputN5Path
	 * @param outputN5DatasetName
	 * @param maskN5PathName
	 * @param thresholdIntensity
	 * @param blockInformationList
	 * @throws IOException
	 */
	
	
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> List<BlockInformation> blockwiseConnectedComponents(
			final JavaSparkContext sc, final String ecsN5Path,final String plasmaMembraneN5Path,
			final String outputN5Path, final String outputN5DatasetName, double minimumVolumeCutoff, List<BlockInformation> blockInformationList) throws IOException {

		// Get attributes of input data set
		final N5Reader n5Reader = new N5FSReader(ecsN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes("ecs");
		final int[] blockSize = attributes.getBlockSize();
		final long[] blockSizeL = new long[] { blockSize[0], blockSize[1], blockSize[2] };
		final long[] outputDimensions = attributes.getDimensions();
		final double [] pixelResolution = IOHelper.getResolution(n5Reader, "ecs");
				
		// Create output dataset
		final N5Writer n5Writer = new N5FSWriter(outputN5Path);
		n5Writer.createGroup(outputN5DatasetName);
		n5Writer.createDataset(outputN5DatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
		n5Writer.setAttribute(outputN5DatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));

		// Set up rdd to parallelize over blockInformation list and run RDD, which will
		// return updated block information containing list of components on the edge of
		// the corresponding block
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<BlockInformation> javaRDDsets = rdd.map(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			long expandBy = 3;
			long expandBySquared = expandBy*expandBy;
			long [] paddedOffset = new long [] {offset[0]-expandBy,offset[1]-expandBy,offset[2]-expandBy};
			long [] paddedDimension = new long [] {dimension[0]+2*expandBy,dimension[1]+2*expandBy,dimension[2]+2*expandBy};
			//for smoothing
			/*double [] sigma = new double[] {12.0/pixelResolution[0],12.0/pixelResolution[0],12.0/pixelResolution[0]}; //gives 3 pixel sigma at 4 nm resolution
			int[] sizes = Gauss3.halfkernelsizes( sigma );
			long padding = sizes[0];
			long [] paddedOffset = new long [] {offset[0]-padding,offset[1]-padding,offset[2]-padding};
			long [] paddedDimension = new long [] {dimension[0]+2*padding,dimension[1]+2*padding,dimension[2]+2*padding};*/
			
			// Read in ecs and smooth
			final N5Reader ecsReaderLocal = new N5FSReader(ecsN5Path);
			RandomAccessibleInterval<UnsignedByteType> ecsPredictionsExpanded = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(ecsReaderLocal, "ecs")
					),paddedOffset, paddedDimension);
			
			final RandomAccessibleInterval<NativeBoolType> ecsPredictionsExpandedBinarized =
					Converters.convert(
							ecsPredictionsExpanded,
							(a, b) -> { b.set(a.getRealDouble()>=127 ? true : false);},
							new NativeBoolType());
			
			ArrayImg<FloatType, FloatArray> distanceFromExpandedEcs = ArrayImgs.floats(paddedDimension);
			DistanceTransform.binaryTransform(ecsPredictionsExpandedBinarized, distanceFromExpandedEcs, DISTANCE_TYPE.EUCLIDIAN);
			
		/*	final Img<UnsignedByteType> smoothedPredictions =  new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType()).create(paddedDimension);	
			SimpleGaussRA<UnsignedByteType> gauss = new SimpleGaussRA<UnsignedByteType>(sigma);
			gauss.compute(rawPredictions, smoothedPredictions);
			*/
			
			IntervalView<FloatType> distanceFromEcs = Views.offsetInterval(distanceFromExpandedEcs,new long[] {expandBy,expandBy,expandBy},dimension);

	
		
			
			final N5Reader plasmaMembraneReaderLocal = new N5FSReader(plasmaMembraneN5Path);
			IntervalView<UnsignedLongType> plasmaMembraneData = Views.offsetInterval(Views.extendZero(
						(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(plasmaMembraneReaderLocal, "plasma_membrane")
						),offset, dimension);
			
			final Img<UnsignedByteType> cellVolume = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType()).create(dimension);
			
			Cursor<FloatType> distanceFromEcsCursor = distanceFromEcs.cursor();
			Cursor<UnsignedLongType> plasmaMembraneCursor = plasmaMembraneData.cursor();
			Cursor<UnsignedByteType> cellVolumeCursor = cellVolume.cursor();
			
			while(distanceFromEcsCursor.hasNext()) {
				distanceFromEcsCursor.next();
				plasmaMembraneCursor.next();
				cellVolumeCursor.next();
				if(distanceFromEcsCursor.get().get()>expandBySquared && plasmaMembraneCursor.get().get()==0) { //then it is neither pm nor ecs
					cellVolumeCursor.get().set(255);
				}
			}
			// Create the output based on the current dimensions
			final Img<UnsignedLongType> output = new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType())
					.create(dimension);

			// Compute the connected components which returns the components along the block
			// edges, and update the corresponding blockInformation object
			int minimumVolumeCutoffInVoxels = (int) Math.ceil(minimumVolumeCutoff/Math.pow(pixelResolution[0],3));
			currentBlockInformation = SparkConnectedComponents.computeConnectedComponents(currentBlockInformation, cellVolume, output, outputDimensions,
					blockSizeL, offset, 1, minimumVolumeCutoffInVoxels);

			// Write out output to temporary n5 stack
			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);

			return currentBlockInformation;
		});

		// Run, collect and return blockInformationList
		blockInformationList = javaRDDsets.collect();

		return blockInformationList;
	}


	
	
	
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkCellVolume");
		String tempOutputN5DatasetName = "cellVolume_blockwise_temp_to_delete";
		String finalOutputN5DatasetName = "cellVolume";
		
		//Create block information list
		List<BlockInformation> blockInformationList = SparkConnectedComponents.buildBlockInformationList(options.getEcsN5Path(), "ecs");
		JavaSparkContext sc = new JavaSparkContext(conf);
	
		String outputN5Path = options.getOutputN5Path();
		blockInformationList = blockwiseConnectedComponents(sc, options.getEcsN5Path(),options.getPlasmaMembraneN5Path(),
				outputN5Path, tempOutputN5DatasetName, options.getMinimumVolumeCutoff(), blockInformationList);
		
		blockInformationList = SparkConnectedComponents.unionFindConnectedComponents(sc, outputN5Path, tempOutputN5DatasetName, options.getMinimumVolumeCutoff(),
				blockInformationList);
		
		SparkConnectedComponents.mergeConnectedComponents(sc, outputN5Path, tempOutputN5DatasetName, finalOutputN5DatasetName, false,blockInformationList);
		
		List<String> directoriesToDelete = new ArrayList<String>();
		directoriesToDelete.add(outputN5Path + "/" + tempOutputN5DatasetName);
		sc.close();
		SparkDirectoryDelete.deleteDirectories(conf, directoriesToDelete);
	}
}

