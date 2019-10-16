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
import java.io.PrintWriter;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;
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

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

/**
 * Connected components on entire stack
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkConnectedComponents {

	final static public String ownerFormat = "%s/owner/%s";
	final static public String stackListFormat = ownerFormat + "/stacks";
	final static public String stackFormat = ownerFormat + "/project/%s/stack/%s";
	final static public String stackBoundsFormat = stackFormat + "/bounds";
	final static public String boundingBoxFormat = stackFormat + "/z/%d/box/%d,%d,%d,%d,%f";
	final static public String renderParametersFormat = boundingBoxFormat + "/render-parameters";

	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputDatasetName = null;

		@Option(name = "--maskN5Path", required = true, usage = "mask N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String maskN5Path = null;

		@Option(name = "--thresholdDistance", required = false, usage = "Distance to threshold at")
		private double thresholdDistance = 0;

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

		public String getInputDatasetName() {
			return inputDatasetName;
		}

		public String getOutputN5Path() {
			return outputN5Path;
		}

		public String getMaskN5Path() {
			return maskN5Path;
		}

		public double getThresholdIntensity() {
			return 128 * Math.tanh(thresholdDistance / 50) + 127;
		}
	}

	/**
	 * Copy an existing N5 dataset into another with a different blockSize.
	 *
	 * Parallelizes over blocks of [max(input, output)] to reduce redundant loading.
	 * If blockSizes are integer multiples of each other, no redundant loading will
	 * happen.
	 *
	 * @param sc
	 * @param n5Path
	 * @param datasetName
	 * @param outDatasetName
	 * @param outBlockSize
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> List<BlockInformation> blockwiseConnectedComponents(
			final JavaSparkContext sc, final String inputN5Path, final String inputDatasetName,
			final String outputN5Path, final String outputDatasetName, final String maskN5PathName,
			final double thresholdIntensity, List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final N5Writer n5Writer = new N5FSWriter(outputN5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		final int n = attributes.getNumDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final long[] blockSizeL = new long[] { blockSize[0], blockSize[1], blockSize[2] };
		final long[] outputDimensions = attributes.getDimensions();
		n5Writer.createGroup(outputDatasetName);
		n5Writer.createDataset(outputDatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());

		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);

		JavaRDD<BlockInformation> javaRDDsets = rdd.map(currentBlockInformation -> {
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];

			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);

			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			final RandomAccessibleInterval<UnsignedByteType> source = N5Utils.open(n5ReaderLocal, inputDatasetName);
			final RandomAccessibleInterval<UnsignedByteType> sourceInterval = Views.offsetInterval(source, offset,
					dimension);

			final N5Reader n5MaskReaderLocal = new N5FSReader(maskN5PathName);
			final RandomAccessibleInterval<UnsignedByteType> mask = N5Utils.open(n5MaskReaderLocal,
					"/volumes/masks/foreground");
			final RandomAccessibleInterval<UnsignedByteType> maskInterval = Views.offsetInterval(mask,
					new long[] { offset[0] / 2, offset[1] / 2, offset[2] / 2 },
					new long[] { dimension[0] / 2, dimension[1] / 2, dimension[2] / 2 });

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

			long[] sourceDimensions = { 0, 0, 0 };
			source.dimensions(sourceDimensions);

			long[] currentDimensions = { 0, 0, 0 };
			sourceInterval.dimensions(currentDimensions);
			final Img<UnsignedLongType> output = new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType())
					.create(currentDimensions);
			Set<Long> edgeComponentIDs = computeConnectedComponents(sourceInterval, output, sourceDimensions,
					blockSizeL, offset, thresholdIntensity);
			currentBlockInformation.edgeComponentIDs = edgeComponentIDs;
			N5Utils.saveBlock(output, n5WriterLocal, outputDatasetName, gridBlock[2]);
			return currentBlockInformation;
		});
		blockInformationList = javaRDDsets.collect();
		return blockInformationList;
	}

	/**
	 * Copy an existing N5 dataset into another with a different blockSize.
	 *
	 * Parallelizes over blocks of [max(input, output)] to reduce redundant loading.
	 * If blockSizes are integer multiples of each other, no redundant loading will
	 * happen.
	 *
	 * @param sc
	 * @param n5Path
	 * @param datasetName
	 * @param outDatasetName
	 * @param outBlockSize
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> List<BlockInformation> unionFindConnectedComponents(
			final JavaSparkContext sc, final String inputN5Path, final String inputDatasetName,
			List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(inputN5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();

		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);

		JavaRDD<Set<List<Long>>> javaRDDsets = rdd.map(currentBlockInformation -> {
			final long[][] gridBlock = currentBlockInformation.gridBlock;
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			final RandomAccessibleInterval<UnsignedLongType> source = N5Utils.open(n5ReaderLocal, inputDatasetName);
			long[] sourceDimensions = { 0, 0, 0 };
			source.dimensions(sourceDimensions);
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];

			RandomAccessibleInterval<UnsignedLongType> xPlane1, yPlane1, zPlane1, xPlane2, yPlane2, zPlane2;
			xPlane1 = yPlane1 = zPlane1 = xPlane2 = yPlane2 = zPlane2 = null;

			long xOffset = offset[0] + blockSize[0];
			long yOffset = offset[1] + blockSize[1];
			long zOffset = offset[2] + blockSize[2];
			xPlane1 = Views.offsetInterval(source, new long[] { xOffset - 1, offset[1], offset[2] },
					new long[] { 1, dimension[1], dimension[2] });
			yPlane1 = Views.offsetInterval(source, new long[] { offset[0], yOffset - 1, offset[2] },
					new long[] { dimension[0], 1, dimension[2] });
			zPlane1 = Views.offsetInterval(source, new long[] { offset[0], offset[1], zOffset - 1 },
					new long[] { dimension[0], dimension[1], 1 });

			if (xOffset < sourceDimensions[0])
				xPlane2 = Views.offsetInterval(source, new long[] { xOffset, offset[1], offset[2] },
						new long[] { 1, dimension[1], dimension[2] });
			if (yOffset < sourceDimensions[1])
				yPlane2 = Views.offsetInterval(source, new long[] { offset[0], yOffset, offset[2] },
						new long[] { dimension[0], 1, dimension[2] });
			if (zOffset < sourceDimensions[2])
				zPlane2 = Views.offsetInterval(source, new long[] { offset[0], offset[1], zOffset },
						new long[] { dimension[0], dimension[1], 1 });

			Set<List<Long>> globalIDtoGlobalIDSet = new HashSet<List<Long>>();
			getGlobalIDsToMerge(xPlane1, xPlane2, globalIDtoGlobalIDSet);
			getGlobalIDsToMerge(yPlane1, yPlane2, globalIDtoGlobalIDSet);
			getGlobalIDsToMerge(zPlane1, zPlane2, globalIDtoGlobalIDSet);

			return globalIDtoGlobalIDSet;
		});

		// collect RDD for printing
		long t0 = System.currentTimeMillis();
		List<Set<List<Long>>> globalIDtoGlobalIDCollectedSets = javaRDDsets.collect();
		long t1 = System.currentTimeMillis();

		Set<List<Long>> globalIDtoGlobalIDFinalSet = new HashSet<List<Long>>();
		for (Set<List<Long>> currentGlobalIDtoGlobalIDSet : globalIDtoGlobalIDCollectedSets) {
			globalIDtoGlobalIDFinalSet.addAll(currentGlobalIDtoGlobalIDSet);
		}
		System.out.println(globalIDtoGlobalIDFinalSet.size());
		long[][] arrayOfUnions = new long[globalIDtoGlobalIDFinalSet.size()][2];

		int count = 0;
		for (List<Long> currentPair : globalIDtoGlobalIDFinalSet) {
			arrayOfUnions[count][0] = currentPair.get(0);
			arrayOfUnions[count][1] = currentPair.get(1);
			count++;
		}

		System.out.println("Total unions = " + arrayOfUnions.length);
		long t2 = System.currentTimeMillis();

		UnionFindDGA unionFind = new UnionFindDGA(arrayOfUnions);
		unionFind.getFinalRoots();

		long t3 = System.currentTimeMillis();

		System.out.println("collect time: " + (t1 - t0));
		System.out.println("build array time: " + (t2 - t1));
		System.out.println("union find time: " + (t3 - t2));
		for (BlockInformation currentBlockInformation : blockInformationList) {
			Map<Long, Long> currentGlobalIDtoRootIDMap = new HashMap<Long, Long>();
			for (Long currentEdgeComponentID : currentBlockInformation.edgeComponentIDs) {
				Long key, value;
				key = currentEdgeComponentID;
				if (unionFind.globalIDtoRootID.containsKey(key)) {// Need this check since not all edge objects will be
																	// connected to neighboring blocks
					value = unionFind.globalIDtoRootID.get(currentEdgeComponentID);
					currentGlobalIDtoRootIDMap.put(key, value);
				}
			}
			currentBlockInformation.edgeComponentIDtoRootIDmap = currentGlobalIDtoRootIDMap;
		}

		return blockInformationList;
	}

	/**
	 * Copy an existing N5 dataset into another with a different blockSize.
	 *
	 * Parallelizes over blocks of [max(input, output)] to reduce redundant loading.
	 * If blockSizes are integer multiples of each other, no redundant loading will
	 * happen.
	 *
	 * @param sc
	 * @param n5Path
	 * @param datasetName
	 * @param outDatasetName
	 * @param outBlockSize
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> void mergeConnectedComponents(final JavaSparkContext sc,
			final String inputN5Path, final String inputDatasetName, final String outputDatasetName,
			final List<BlockInformation> blockInformationList) throws IOException {

		final PrintWriter driverOut = new PrintWriter("/groups/cosem/cosem/ackermand/tmp/logDriver.txt");

		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final N5Writer n5Writer = new N5FSWriter(inputN5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();
		n5Writer.createGroup(outputDatasetName);
		n5Writer.createDataset(outputDatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());

		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);

		rdd.foreach(currentBlockInformation -> {
			final Map<Long, Long> edgeComponentIDtoRootIDmap = currentBlockInformation.edgeComponentIDtoRootIDmap;
			final long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			final PrintWriter out = null; // new
											// PrintWriter(String.format("/groups/cosem/cosem/ackermand/tmp/logMemory-%d_%d_%d.txt",
											// offset[0],offset[1],offset[2]));
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			final RandomAccessibleInterval<UnsignedLongType> source = N5Utils.open(n5ReaderLocal, inputDatasetName);
			// final Map<Long,Long> globalIDtoRootID = broadcastGlobalIDtoRootID.value();
			final RandomAccessibleInterval<UnsignedLongType> sourceInterval = Views.offsetInterval(source, offset,
					dimension);
			Cursor<UnsignedLongType> sourceCursor = Views.flatIterable(sourceInterval).cursor();
			while (sourceCursor.hasNext()) {
				final UnsignedLongType voxel = sourceCursor.next();
				long currentValue = voxel.getLong();
				if (currentValue > 0 && edgeComponentIDtoRootIDmap.containsKey(currentValue)) {
					Long currentRoot = edgeComponentIDtoRootIDmap.get(currentValue);
					voxel.setLong(currentRoot);
				}
			}

			final N5Writer n5WriterLocal = new N5FSWriter(inputN5Path);
			N5Utils.saveBlock(sourceInterval, n5WriterLocal, outputDatasetName, gridBlock[2]);
			// out.close();
		});

		driverOut.close();

	}

	public static void logMemory(final String context, final PrintWriter out) {
		final long freeMem = Runtime.getRuntime().freeMemory() / 1000000L;
		final long totalMem = Runtime.getRuntime().totalMemory() / 1000000L;
		logMsg(context + ", Total: " + totalMem + " MB, Free: " + freeMem + " MB, Delta: " + (totalMem - freeMem)
				+ " MB", out);
	}

	public static void logMsg(final String msg, final PrintWriter out) {
		final String ts = new SimpleDateFormat("HH:mm:ss").format(new Date()) + " ";
		System.out.println(ts + " " + msg);
		// out.println(ts + " " + msg);
		// out.flush();

	}

	public static List<BlockInformation> buildBlockInformationList(final String inputN5Path,
			final String inputDatasetName) throws IOException {
		N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();

		List<long[][]> gridBlockList = Grid.create(outputDimensions, blockSize);
		List<BlockInformation> blockInformationList = new ArrayList<BlockInformation>();
		for (int i = 0; i < gridBlockList.size(); i++) {
			long[][] currentGridBlock = gridBlockList.get(i);
			blockInformationList.add(new BlockInformation(currentGridBlock, null, null));
		}
		return blockInformationList;
	}

	public static final void getGlobalIDsToMerge(RandomAccessibleInterval<UnsignedLongType> hyperSlice1,
			RandomAccessibleInterval<UnsignedLongType> hyperSlice2, Set<List<Long>> globalIDtoGlobalIDSet) {
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

	public static Set<Long> computeConnectedComponents(RandomAccessibleInterval<UnsignedByteType> sourceInterval,
			RandomAccessibleInterval<UnsignedLongType> output, long[] sourceDimensions,
			long[] outputDimensions, long[] offset, double thresholdIntensity) {
		// threshold sourceInterval using cutoff of 127
		final RandomAccessibleInterval<BoolType> thresholded = Converters.convert(sourceInterval,
				(a, b) -> b.set(a.getRealDouble() >= thresholdIntensity), new BoolType());

		// run connected component analysis, storing results in components
		final ArrayImg<UnsignedLongType, LongArray> components = ArrayImgs
				.unsignedLongs(Intervals.dimensionsAsLongArray(thresholded));
		ConnectedComponentAnalysis.connectedComponents(thresholded, components);

		// cursors over output and components
		Cursor<UnsignedLongType> o = Views.flatIterable(output).cursor();
		final Cursor<UnsignedLongType> c = Views.flatIterable(components).cursor();

		// assign values from components to output and create array for relabeling
		// connected components based on the first voxel in the connected component

		double labelBasedOnMaxVoxelIndexInComponent[] = new double[(int) (outputDimensions[0] * outputDimensions[1]
				* outputDimensions[2])];

		while (o.hasNext()) {
			final UnsignedLongType tO = o.next();
			final UnsignedLongType tC = c.next();
			if (tC.getRealDouble() > 0) {
				tO.setReal(tC.getRealDouble());

				// if connected component exists, assign its value to output and update its new
				// label based on first voxel
				long[] currentVoxelPosition = { o.getIntPosition(0), o.getIntPosition(1), o.getIntPosition(2) };
				currentVoxelPosition[0] += offset[0];
				currentVoxelPosition[1] += offset[1];
				currentVoxelPosition[2] += offset[2];

				double currentVoxelIndex = (double) (sourceDimensions[0] * sourceDimensions[1] * currentVoxelPosition[2]
						+ // Z position
						sourceDimensions[0] * currentVoxelPosition[1] + currentVoxelPosition[0]);

				int defaultLabel = tC.getInteger();
				if (currentVoxelIndex > labelBasedOnMaxVoxelIndexInComponent[defaultLabel]) {
					labelBasedOnMaxVoxelIndexInComponent[defaultLabel] = currentVoxelIndex;
				}

			}
		}

		Set<Long> edgeComponentIDs = new HashSet<Long>();
		// update output labels based on max voxel labels
		o = Views.flatIterable(output).cursor();
		while (o.hasNext()) {
			final UnsignedLongType tO = o.next();
			if (tO.getRealDouble() != 0) {
				double newLabel = 0;
				newLabel = labelBasedOnMaxVoxelIndexInComponent[(int) tO.getRealDouble()];
				if (o.getIntPosition(0) == 0 || o.getIntPosition(0) == outputDimensions[0] - 1
						|| o.getIntPosition(1) == 0 || o.getIntPosition(1) == outputDimensions[1] - 1
						|| o.getIntPosition(2) == 0 || o.getIntPosition(2) == outputDimensions[2] - 1) {
					edgeComponentIDs.add((long) newLabel);
				}
				tO.setReal(newLabel);
			}
		}

		return edgeComponentIDs;
	}

	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkRandomSubsampleN5");

		// Get all organelles
		String[] organelles = { "" };
		if (options.getInputDatasetName() != null) {
			organelles = options.getInputDatasetName().split(",");
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

		for (String currentOrganelle : organelles) {

			List<BlockInformation> blockInformationList = buildBlockInformationList(options.getInputN5Path(),
					currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
			logMemory(currentOrganelle, null);
			final String tempOutputDatasetName = currentOrganelle + "_blockwise_cc_temp_to_delete";
			final String finalOutputDatasetName = currentOrganelle + "_cc";
			blockInformationList = blockwiseConnectedComponents(sc, options.getInputN5Path(), currentOrganelle,
					options.getOutputN5Path(), tempOutputDatasetName, options.getMaskN5Path(),
					options.getThresholdIntensity(), blockInformationList);
			logMemory("Stage 1 complete", null);
			blockInformationList = unionFindConnectedComponents(sc, options.getOutputN5Path(), tempOutputDatasetName,
					blockInformationList);
			logMemory("Stage 2 complete", null);
			mergeConnectedComponents(sc, options.getOutputN5Path(), tempOutputDatasetName, finalOutputDatasetName,
					blockInformationList);
			logMemory("Stage 3 complete", null);
			sc.close();
		}

		for (String currentOrganelle : organelles) {
			final String tempOutputDatasetName = currentOrganelle + "_blockwise_cc_temp_to_delete";
			FileUtils.deleteDirectory(new File(options.getOutputN5Path() + "/" + tempOutputDatasetName));
		}

	}
}
