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
import org.spark_project.guava.collect.Sets;

import bdv.labels.labelset.Label;
import ij.ImageJ;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.NativeImg;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.IntervalView;
import net.imglib2.view.SubsampleIntervalView;
import net.imglib2.view.Views;

/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkConnectedComponents {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5DatasetSuffix", required = false, usage = "N5 suffix, e.g. _cc so output would be /mito_cc")
		private String outputN5DatasetSuffix = "_cc";

		@Option(name = "--maskN5Path", required = true, usage = "mask N5 path, e.g. /groups/cosem/cosem/data/HeLa_Cell3_4x4x4nm/HeLa_Cell3_4x4x4nm.n5")
		private String maskN5Path = null;

		@Option(name = "--thresholdDistance", required = false, usage = "Distance for thresholding (positive inside, negative outside)")
		private double thresholdDistance = 0;
		
		@Option(name = "--minimumVolumeCutoff", required = false, usage = "Volume above which objects will be kept")
		private int minimumVolumeCutoff = 100;

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

		public String getMaskN5Path() {
			return maskN5Path;
		}

		public double getThresholdIntensityCutoff() {
			return 128 * Math.tanh(thresholdDistance / 50) + 127;
		}
		
		public int getMinimumVolumeCutoff() {
			return minimumVolumeCutoff;
		}

	}

	public static final void calculateDistanceTransform(
			final JavaSparkContext sc,
			final String n5Path,
			final String datasetName,
			final String n5OutputPath,
			final String outputDatasetName,
			final long[] initialPadding,
			final double weight,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final int n = dimensions.length;

		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		n5Writer.createDataset(
				outputDatasetName,
				dimensions,
				blockSize,
				DataType.UINT16,
				new GzipCompression());

		/*
		 * grid block size for parallelization to minimize double loading of
		 * blocks
		 */
		boolean show=false;
		if(show) new ImageJ();
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(blockInformation -> {
			final long [][] gridBlock = blockInformation.gridBlock;
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			final RandomAccessibleInterval<FloatType> source;
			source = Converters.convert(
					// for OpenJDK 8, Eclipse could do without the intermediate raw cast...
					(RandomAccessibleInterval<IntegerType<?>>)(RandomAccessibleInterval)N5Utils.open(n5BlockReader, datasetName),
					(a, b) -> {
						final double x = (a.getInteger()-127)/128.0;
						final double d = 25*Math.log((1+x)/(1-x));
						b.set((float) Math.max(0,d));
						//b.set((float)a.getRealDouble()); 
						//if(d>0) b.set(1);
						//else b.set(0);
					},
					new FloatType());
			final long[] padding = initialPadding.clone();
A:			for (boolean paddingIsTooSmall = true; paddingIsTooSmall; Arrays.setAll(padding, i -> padding[i] + initialPadding[i])) {

				paddingIsTooSmall = false;

				final long maxPadding =  Arrays.stream(padding).max().getAsLong();
				final long squareMaxPadding = maxPadding * maxPadding;

				final long[] paddedBlockMin = new long[n];
				final long[] paddedBlockSize = new long[n];
				Arrays.setAll(paddedBlockMin, i -> gridBlock[0][i] - padding[i]);
				Arrays.setAll(paddedBlockSize, i -> gridBlock[1][i] + 2*padding[i]);
				System.out.println(Arrays.toString(gridBlock[0]) + ", padding = " + Arrays.toString(padding) + ", padded blocksize = " + Arrays.toString(paddedBlockSize));
				
				final long maxBlockDimension = Arrays.stream(paddedBlockSize).max().getAsLong();
				final IntervalView<FloatType> sourceBlock =
						Views.offsetInterval(
								Views.extendValue(
										source,
										new FloatType((float) Math.pow(maxBlockDimension,2))), //new FloatType(Label.OUTSIDE)),
								paddedBlockMin,
								paddedBlockSize);
				
				/* make distance transform */				
				if(show) ImageJFunctions.show(sourceBlock, "sourceBlock");
				final NativeImg<FloatType, ?> targetBlock = ArrayImgs.floats(paddedBlockSize);
				
				DistanceTransform.transform(sourceBlock, targetBlock, DISTANCE_TYPE.EUCLIDIAN,weight);
				if(show) ImageJFunctions.show(targetBlock,"dt");

				final long[] minInside = new long[n];
				final long[] dimensionsInside = new long[n];
				Arrays.setAll(minInside, i -> padding[i] );
				Arrays.setAll(dimensionsInside, i -> gridBlock[1][i] );

				final IntervalView<FloatType> insideBlock = Views.offsetInterval(Views.extendZero(targetBlock), minInside, dimensionsInside);
				if(show) ImageJFunctions.show(insideBlock,"inside");

				/* test whether distances at inside boundary are smaller than padding */
				for (int d = 0; d < n; ++d) {

					final IntervalView<FloatType> topSlice = Views.hyperSlice(insideBlock, d, 1);
					for (final FloatType t : topSlice)
						if (t.get() >= squareMaxPadding) {
							paddingIsTooSmall = true;
							System.out.println("padding too small");
							continue A;
						}

					final IntervalView<FloatType> botSlice = Views.hyperSlice(insideBlock, d, insideBlock.max(d));
					for (final FloatType t : botSlice)
						if (t.get() >= squareMaxPadding) {
							paddingIsTooSmall = true;
							System.out.println("padding too small");
							continue A;
						}
				}

				/* padding was sufficient, save */
				final RandomAccessibleInterval<UnsignedShortType> convertedOutputBlock = Converters.convert(
						(RandomAccessibleInterval<FloatType>) insideBlock,
						(a, b) -> b.set(Math.min(65535, (int)Math.round(256*Math.sqrt(a.get())))),
						new UnsignedShortType());
				if(show==true) ImageJFunctions.show(convertedOutputBlock,"output");

				final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
				N5Utils.saveNonEmptyBlock(convertedOutputBlock, n5BlockWriter, outputDatasetName, gridBlock[2], new UnsignedShortType(2));
			}
		});
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
			final JavaSparkContext sc, final String inputN5Path, final String inputN5DatasetName,
			final String outputN5Path, final String outputN5DatasetName, final String maskN5PathName,
			final double thresholdIntensityCutoff, int minimumVolumeCutoff, List<BlockInformation> blockInformationList) throws IOException {

		// Get attributes of input data set
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] blockSizeL = new long[] { blockSize[0], blockSize[1], blockSize[2] };
		final long[] outputDimensions = attributes.getDimensions();

		// Create output dataset
		final N5Writer n5Writer = new N5FSWriter(outputN5Path);
		n5Writer.createGroup(outputN5DatasetName);
		n5Writer.createDataset(outputN5DatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());

		// Set up rdd to parallelize over blockInformation list and run RDD, which will
		// return updated block information containing list of components on the edge of
		// the corresponding block
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<BlockInformation> javaRDDsets = rdd.map(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];

			// Read in source block
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			final RandomAccessibleInterval<UnsignedByteType> sourceInterval = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)
					),offset, dimension);

			// Read in mask block
			final N5Reader n5MaskReaderLocal = new N5FSReader(maskN5PathName);
			final RandomAccessibleInterval<UnsignedByteType> mask = N5Utils.open(n5MaskReaderLocal,
					"/volumes/masks/foreground");
			final RandomAccessibleInterval<UnsignedByteType> maskInterval = Views.offsetInterval(Views.extendZero(mask),
					new long[] { offset[0] / 2, offset[1] / 2, offset[2] / 2 },
					new long[] { dimension[0] / 2, dimension[1] / 2, dimension[2] / 2 });

			// Mask out appropriate region in source block; need to do it this way rather
			// than converter since mask is half the size of source
			Cursor<UnsignedByteType> sourceCursor = Views.flatIterable(sourceInterval).cursor();
			RandomAccess<UnsignedByteType> maskRandomAccess = maskInterval.randomAccess();
			while (sourceCursor.hasNext()) {
				final UnsignedByteType voxel = sourceCursor.next();
				final long[] positionInMask = { (long) Math.floor(sourceCursor.getDoublePosition(0) / 2),
						(long) Math.floor(sourceCursor.getDoublePosition(1) / 2),
						(long) Math.floor(sourceCursor.getDoublePosition(2) / 2) };
				maskRandomAccess.setPosition(positionInMask);
				if (maskRandomAccess.get().getRealDouble() == 0) {
					voxel.setInteger(0);
				}
			}

			// Create the output based on the current dimensions
			long[] currentDimensions = { 0, 0, 0 };
			sourceInterval.dimensions(currentDimensions);
			final Img<UnsignedLongType> output = new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType())
					.create(currentDimensions);

			// Compute the connected components which returns the components along the block
			// edges, and update the corresponding blockInformation object
			currentBlockInformation.edgeComponentIDtoVolumeMap = computeConnectedComponents(sourceInterval, output, outputDimensions,
					blockSizeL, offset, thresholdIntensityCutoff, minimumVolumeCutoff);

			// Write out output to temporary n5 stack
			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);

			return currentBlockInformation;
		});

		// Run, collect and return blockInformationList
		blockInformationList = javaRDDsets.collect();

		return blockInformationList;
	}


	
	/**
	 * Merge all necessary objects obtained from blockwise connected components.
	 *
	 * Determines which objects need to be fused based on which ones touch at the
	 * boundary between blocks. Then performs the corresponding union find so each
	 * complete object has a unique id. Parallelizes over block information list.
	 *
	 * @param sc
	 * @param inputN5Path
	 * @param inputN5DatasetName
	 * @param blockInformationList
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> List<BlockInformation> unionFindConnectedComponents(
			final JavaSparkContext sc, final String inputN5Path, final String inputN5DatasetName, int minimumVolumeCutoff,
			List<BlockInformation> blockInformationList) throws IOException {

		// Get attributes of input data set:
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();

		// Set up and run RDD, which will return the set of pairs of object IDs that
		// need to be fused
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<Set<List<Long>>> javaRDDsets = rdd.map(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];

			// Get source
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			final RandomAccessibleInterval<UnsignedLongType> source = (RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5ReaderLocal, inputN5DatasetName);
			long[] sourceDimensions = { 0, 0, 0 };
			source.dimensions(sourceDimensions);

			// Get hyperplanes of current block (x-most edge, y-most edge, z-most edge), and
			// the corresponding hyperplanes for the x+1 block, y+1 block and z+1 block.
			RandomAccessibleInterval<UnsignedLongType> xPlane1, yPlane1, zPlane1, xPlane2, yPlane2, zPlane2;
			xPlane1 = yPlane1 = zPlane1 = xPlane2 = yPlane2 = zPlane2 = null;

			long xOffset = offset[0] + blockSize[0];
			long yOffset = offset[1] + blockSize[1];
			long zOffset = offset[2] + blockSize[2];
			xPlane1 = Views.offsetInterval(Views.extendZero(source), new long[] { xOffset - 1, offset[1], offset[2] },
					new long[] { 1, dimension[1], dimension[2] });
			yPlane1 = Views.offsetInterval(Views.extendZero(source), new long[] { offset[0], yOffset - 1, offset[2] },
					new long[] { dimension[0], 1, dimension[2] });
			zPlane1 = Views.offsetInterval(Views.extendZero(source), new long[] { offset[0], offset[1], zOffset - 1 },
					new long[] { dimension[0], dimension[1], 1 });

			if (xOffset < sourceDimensions[0])
				xPlane2 = Views.offsetInterval(Views.extendZero(source), new long[] { xOffset, offset[1], offset[2] },
						new long[] { 1, dimension[1], dimension[2] });
			if (yOffset < sourceDimensions[1])
				yPlane2 = Views.offsetInterval(Views.extendZero(source), new long[] { offset[0], yOffset, offset[2] },
						new long[] { dimension[0], 1, dimension[2] });
			if (zOffset < sourceDimensions[2])
				zPlane2 = Views.offsetInterval(Views.extendZero(source), new long[] { offset[0], offset[1], zOffset },
						new long[] { dimension[0], dimension[1], 1 });

			// Calculate the set of object IDs that are touching and need to be merged
			Set<List<Long>> globalIDtoGlobalIDSet = new HashSet<>();
			getGlobalIDsToMerge(xPlane1, xPlane2, globalIDtoGlobalIDSet);
			getGlobalIDsToMerge(yPlane1, yPlane2, globalIDtoGlobalIDSet);
			getGlobalIDsToMerge(zPlane1, zPlane2, globalIDtoGlobalIDSet);

			return globalIDtoGlobalIDSet;
		});

		// Collect and combine the sets of objects to merge
		long t0 = System.currentTimeMillis();
		Set<List<Long>> globalIDtoGlobalIDFinalSet = javaRDDsets.reduce((a,b) -> {a.addAll(b); return a; });
		long t1 = System.currentTimeMillis();
		System.out.println(globalIDtoGlobalIDFinalSet.size());
		System.out.println("Total unions = " + globalIDtoGlobalIDFinalSet.size());

		// Perform union find to merge all touching objects
		UnionFindDGA unionFind = new UnionFindDGA(globalIDtoGlobalIDFinalSet);
		unionFind.getFinalRoots();

		long t2 = System.currentTimeMillis();

		System.out.println("collect time: " + (t1 - t0));
		System.out.println("union find time: " + (t2 - t1));
		System.out.println("Total edge objects: " + unionFind.globalIDtoRootID.values().stream().distinct().count());

		// Add block-specific relabel map to the corresponding block information object
		Map<Long, Long> rootIDtoVolumeMap= new HashMap<Long, Long>();
		for (BlockInformation currentBlockInformation : blockInformationList) {
			Map<Long, Long> currentGlobalIDtoRootIDMap = new HashMap<Long, Long>();
			for (Long currentEdgeComponentID : currentBlockInformation.edgeComponentIDtoVolumeMap.keySet()) {
				Long key, value;
				key = currentEdgeComponentID;
				if (unionFind.globalIDtoRootID.containsKey(key)) {// Need this check since not all edge objects will be
																	// connected to neighboring blocks
					value = unionFind.globalIDtoRootID.get(currentEdgeComponentID);
					currentGlobalIDtoRootIDMap.put(key, value);
					rootIDtoVolumeMap.put(value, rootIDtoVolumeMap.getOrDefault(value, 0L) + currentBlockInformation.edgeComponentIDtoVolumeMap.get(key));
				}
				else {
					currentGlobalIDtoRootIDMap.put(key, key);
				}
			}
			currentBlockInformation.edgeComponentIDtoRootIDmap = currentGlobalIDtoRootIDMap;
		}
		
		for (BlockInformation currentBlockInformation : blockInformationList) {
			for (Entry <Long,Long> e : currentBlockInformation.edgeComponentIDtoRootIDmap.entrySet()) {
				Long key = e.getKey();
				Long value = e.getValue();
				currentBlockInformation.edgeComponentIDtoRootIDmap.put(key, 
						currentBlockInformation.edgeComponentIDtoVolumeMap.get(key) <= minimumVolumeCutoff ? 0L : value);
			}
		}
		
		

		return blockInformationList;
	}

	
	/**
	 * Merge touching objects by relabeling them to common root
	 *
	 * Reads in blockwise-connected component data and relabels touching objects
	 * using the block information map, writing the data to the final output n5
	 *
	 * @param sc
	 * @param inputN5Path
	 * @param datasetName
	 * @param inputN5DatasetName
	 * @param outputN5DatasetName
	 * @param blockInformationList
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> void mergeConnectedComponents(final JavaSparkContext sc,
			final String inputN5Path, final String inputN5DatasetName, final String outputN5DatasetName,
			final List<BlockInformation> blockInformationList) throws IOException {

		// Set up reader to get n5 attributes
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();

		// Set up writer for output
		final N5Writer n5Writer = new N5FSWriter(inputN5Path);
		n5Writer.createGroup(outputN5DatasetName);
		n5Writer.createDataset(outputN5DatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());

		// Set up and run rdd to relabel objects and write out blocks
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(currentBlockInformation -> {
			// Get block-specific information
			final long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			final Map<Long, Long> edgeComponentIDtoRootIDmap = currentBlockInformation.edgeComponentIDtoRootIDmap;

			// Read in source data
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			final RandomAccessibleInterval<UnsignedLongType> sourceInterval = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)),
					offset, dimension);

			//Relabel objects
			Cursor<UnsignedLongType> sourceCursor = Views.flatIterable(sourceInterval).cursor();
			while (sourceCursor.hasNext()) {
				final UnsignedLongType voxel = sourceCursor.next();
				long currentValue = voxel.getLong();
				if (currentValue > 0 && edgeComponentIDtoRootIDmap.containsKey(currentValue)) {
					Long currentRoot = edgeComponentIDtoRootIDmap.get(currentValue);
					voxel.setLong(currentRoot);
				}
			}

			//Write out block
			final N5Writer n5WriterLocal = new N5FSWriter(inputN5Path);
			N5Utils.saveBlock(sourceInterval, n5WriterLocal, outputN5DatasetName, gridBlock[2]);
		});

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

	
	public static Map<Long,Long> computeConnectedComponents(RandomAccessibleInterval<UnsignedByteType> sourceInterval,
			RandomAccessibleInterval<UnsignedLongType> output, long[] sourceDimensions, long[] outputDimensions,
			long[] offset, double thresholdIntensityCutoff, int minimumVolumeCutoff) {

		// Threshold sourceInterval using thresholdIntensityCutoff
		final RandomAccessibleInterval<BoolType> thresholded = Converters.convert(sourceInterval,
				(a, b) -> b.set(a.getRealDouble() >= thresholdIntensityCutoff), new BoolType());

		// Run connected component analysis, storing results in components
		final ArrayImg<UnsignedLongType, LongArray> components = ArrayImgs
				.unsignedLongs(Intervals.dimensionsAsLongArray(thresholded));
		ConnectedComponentAnalysis.connectedComponents(thresholded, components);

		// Cursors over output and components
		Cursor<UnsignedLongType> o = Views.flatIterable(output).cursor();
		final Cursor<UnsignedLongType> c = Views.flatIterable(components).cursor();

		// Relabel object ids with unique ids corresponding to a global voxel index
		// within the object, and store/return object ids on edge
		long[] defaultIDtoGlobalID = new long[(int) (outputDimensions[0] * outputDimensions[1] * outputDimensions[2])];
		Set<Long> edgeComponentIDs = new HashSet<>();
		Map<Long,Long> allComponentIDtoVolumeMap = new HashMap<>();
		while (o.hasNext()) {

			final UnsignedLongType tO = o.next();
			final UnsignedLongType tC = c.next();

			int defaultID = tC.getInteger();
			if (defaultID > 0) {
				// If the voxel is part of an object, set the corresponding output voxel to a
				// unique global ID

				if (defaultIDtoGlobalID[defaultID] == 0) {
					// Get global ID

					long[] currentVoxelPosition = { o.getIntPosition(0), o.getIntPosition(1), o.getIntPosition(2) };
					currentVoxelPosition[0] += offset[0];
					currentVoxelPosition[1] += offset[1];
					currentVoxelPosition[2] += offset[2];

					// Unique global ID is based on pixel index, +1 to differentiate it from
					// background
					long globalID = sourceDimensions[0] * sourceDimensions[1] * currentVoxelPosition[2]
							+ sourceDimensions[0] * currentVoxelPosition[1] + currentVoxelPosition[0] + 1;

					defaultIDtoGlobalID[defaultID] = globalID;
				}

				tO.setLong(defaultIDtoGlobalID[defaultID]);

				// Store ids of objects on the edge of a block
				if (o.getIntPosition(0) == 0 || o.getIntPosition(0) == outputDimensions[0] - 1
						|| o.getIntPosition(1) == 0 || o.getIntPosition(1) == outputDimensions[1] - 1
						|| o.getIntPosition(2) == 0 || o.getIntPosition(2) == outputDimensions[2] - 1) {
					edgeComponentIDs.add(defaultIDtoGlobalID[defaultID]);
				}
				allComponentIDtoVolumeMap.put(defaultIDtoGlobalID[defaultID], allComponentIDtoVolumeMap.getOrDefault(defaultIDtoGlobalID[defaultID],0L)+1);	
			}
		}
		Set<Long> allComponentIDs = allComponentIDtoVolumeMap.keySet();
		Set<Long> selfContainedObjectIDs = Sets.difference(allComponentIDs, edgeComponentIDs);
		
		final Map<Long,Long> edgeComponentIDtoVolumeMap =  edgeComponentIDs.stream()
		        .filter(allComponentIDtoVolumeMap::containsKey)
		        .collect(Collectors.toMap(Function.identity(), allComponentIDtoVolumeMap::get));
		
		final Map<Long,Long> selfContainedComponentIDtoVolumeMap =  selfContainedObjectIDs.stream()
		        .filter(allComponentIDtoVolumeMap::containsKey)
		        .collect(Collectors.toMap(Function.identity(), allComponentIDtoVolumeMap::get));
		
		o = Views.flatIterable(output).cursor();
		while (o.hasNext()) {
			final UnsignedLongType tO = o.next();
			if (selfContainedComponentIDtoVolumeMap.getOrDefault(tO.get(), Long.MAX_VALUE) <= minimumVolumeCutoff){
				tO.set(0);
			}
		}

		return edgeComponentIDtoVolumeMap;
	}
	
	public static final void getGlobalIDsToMerge(RandomAccessibleInterval<UnsignedLongType> hyperSlice1,
			RandomAccessibleInterval<UnsignedLongType> hyperSlice2, Set<List<Long>> globalIDtoGlobalIDSet) {
		// The global IDS that need to be merged are those that are touching along the
		// hyperplane borders between adjacent blocks
		if (hyperSlice1 != null && hyperSlice2 != null) {
			Cursor<UnsignedLongType> hs1Cursor = Views.flatIterable(hyperSlice1).cursor();
			Cursor<UnsignedLongType> hs2Cursor = Views.flatIterable(hyperSlice2).cursor();
			while (hs1Cursor.hasNext()) {
				long hs1Value = hs1Cursor.next().getLong();
				long hs2Value = hs2Cursor.next().getLong();
				if (hs1Value > 0 && hs2Value > 0) {
					globalIDtoGlobalIDSet.add(Arrays.asList(hs1Value, hs2Value));// hs1->hs2 pair should always be
																					// distinct since hs1 is unique to
																					// first block
				}
			}

		}
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

		final SparkConf conf = new SparkConf().setAppName("SparkConnectedComponents");

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

		String tempOutputN5DatasetName = null;
		String finalOutputN5DatasetName = null;
		List<String> directoriesToDelete = new ArrayList<String>();
		for (String currentOrganelle : organelles) {
			logMemory(currentOrganelle);	
			tempOutputN5DatasetName = currentOrganelle + options.getOutputN5DatasetSuffix() + "_blockwise_temp_to_delete";
			finalOutputN5DatasetName = currentOrganelle + options.getOutputN5DatasetSuffix();
			
			//Create block information list
			List<BlockInformation> blockInformationList = buildBlockInformationList(options.getInputN5Path(),
				currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
			/*double weights [] = {0.0001}; 
			for (double weight : weights) {
				calculateDistanceTransform(sc, options.getInputN5Path(), currentOrganelle,
						options.getOutputN5Path(), "distance_transform_w"+String.valueOf(weight).replace('.', 'p'), new long[] {16,16,16}, weight, blockInformationList);
			}*/
			int minimumVolumeCutoff = options.getMinimumVolumeCutoff();
			if(currentOrganelle.equals("ribosomes") || currentOrganelle.equals("microtubules")) {
				minimumVolumeCutoff = 0;
			}
			blockInformationList = blockwiseConnectedComponents(sc, options.getInputN5Path(), currentOrganelle,
					options.getOutputN5Path(), tempOutputN5DatasetName, options.getMaskN5Path(),
					options.getThresholdIntensityCutoff(), minimumVolumeCutoff, blockInformationList);
			logMemory("Stage 1 complete");
			
			blockInformationList = unionFindConnectedComponents(sc, options.getOutputN5Path(), tempOutputN5DatasetName, minimumVolumeCutoff,
					blockInformationList);
			logMemory("Stage 2 complete");
			
			mergeConnectedComponents(sc, options.getOutputN5Path(), tempOutputN5DatasetName, finalOutputN5DatasetName,
					blockInformationList);
			logMemory("Stage 3 complete");

			directoriesToDelete.add(options.getOutputN5Path() + "/" + tempOutputN5DatasetName);
			
			sc.close();
		}

		//Remove temporary files
		SparkDirectoryDelete.deleteDirectories(conf, directoriesToDelete);
		logMemory("Stage 4 complete");

	}
}
