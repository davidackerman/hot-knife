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
import org.janelia.saalfeldlab.hotknife.ops.SimpleGaussRA;
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
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.NativeImg;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.array.ArrayRandomAccess;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.type.numeric.real.DoubleType;
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
public class SparkCustomMask {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--datasetToMaskN5Path", required = false, usage = "dataset to mask n5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String datasetToMaskN5Path = null;
		
		@Option(name = "--datasetToUseAsMaskN5Path", required = false, usage = "dataset to mask n5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String datasetToUseAsMaskN5Path = null;

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String inputN5Path = null;
		
		@Option(name = "--datasetNameToThin", required = true, usage = "input N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String datasetNameToThin = null;
		
		@Option(name = "--outputN5Path", required = true, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--doStage1", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private boolean doStage1 = false;
		
		@Option(name = "--datasetNameToMask", required = false, usage = "N5 dataset, e.g. /mito")
		private String datasetNameToMask = null;
		
		@Option(name = "--datasetNameToUseAsMask", required = false, usage = "N5 dataset, e.g. /mito")
		private String datasetNameToUseAsMask = null;
		
		@Option(name = "--thresholdIntensityCutoff", required = false, usage = "N5 dataset, e.g. /mito")
		private double thresholdIntensityCutoff = 127;
		
		@Option(name = "--onlyKeepLargestComponent", required = false, usage = "Keep only the largest connected component")
		private boolean onlyKeepLargestComponent = false;
		
		@Option(name = "--skipConnectedComponents", required = false, usage = "Keep only the largest connected component")
		private boolean skipConnectedComponents = false;
		
		@Option(name = "--expansion", required = false, usage = "expansion in nm")
		private Integer expansion = 160;

		public Options(final String[] args) {

			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);
				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				System.err.println(e.getMessage());
				parser.printUsage(System.err);
			}
		}

		public String getDatasetToMaskN5Path() {
			return datasetToMaskN5Path;
		}
		
		public String getDatasetToUseAsMaskN5Path() {
			return datasetToUseAsMaskN5Path;
		}

		public String getDatasetNameToMask() {
			return datasetNameToMask;
		}
		

		public String getDatasetNameToUseAsMask() {
			return datasetNameToUseAsMask;
		}
		
		public String getDatasetNameToThin() {
			return datasetNameToThin;
		}
		
		public String getInputN5Path() {
			return inputN5Path;
		}

		public String getOutputN5Path() {
			return outputN5Path;
		}
		
		public double getThresholdIntensityCutoff() {
			return thresholdIntensityCutoff;
		}
		
		public boolean getOnlyKeepLargestComponent() {
			return onlyKeepLargestComponent;
		}
		
		public Integer getExpansion() {
			return expansion;
		}
		
		public boolean getSkipConnectedComponents() {
			return skipConnectedComponents;
		}
		
		public boolean getDoStage1() {
			return doStage1;
		}

	}

	public static final void thinDataset(final JavaSparkContext sc, final String n5Path, final String datasetName, final String n5OutputPath, final List<BlockInformation> blockInformationList) throws IOException {
		String outputName = datasetName+"_200_thinned200";
		final N5Reader n5Reader = new N5FSReader(n5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		n5Writer.createDataset(
				outputName,
				dimensions,
				blockSize,
				DataType.UINT8,
				new GzipCompression());
		double[] pixelResolution = IOHelper.getResolution(n5Reader, datasetName);
		n5Writer.setAttribute(outputName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
	
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(blockInformation -> {
			final long [] offset= blockInformation.gridBlock[0];
			final long [] dimension = blockInformation.gridBlock[1];
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			
			//smooth
			double [] sigma = new double[] {3,3,3};
			int[] sizes = Gauss3.halfkernelsizes( sigma );
			double thinDistanceInVoxels = 200/pixelResolution[0];
			long padding = (long) (sizes[0]+2+thinDistanceInVoxels); //200 nm shrinking, need to ensure 200 nm from edge, add 2 for buffer
			long [] paddedOffset = new long [] {offset[0]-padding,offset[1]-padding,offset[2]-padding};
			long [] paddedDimension = new long [] {dimension[0]+2*padding,dimension[1]+2*padding,dimension[2]+2*padding};
			RandomAccessibleInterval<UnsignedByteType> rawPredictions = Views.offsetInterval(Views.extendMirrorSingle(
					(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(n5BlockReader, datasetName)
					),paddedOffset, paddedDimension);	
			RandomAccessibleInterval<UnsignedByteType> smoothedPredictions =  new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType()).create(paddedDimension);	
			SimpleGaussRA<UnsignedByteType> gauss = new SimpleGaussRA<UnsignedByteType>(sigma);
			gauss.compute(rawPredictions, smoothedPredictions);
			
			//RandomAccessibleInterval<UnsignedByteType> smoothedPredictionsCropped = Views.offsetInterval(smoothedPredictions,new long[] {padding,padding,padding},dimension);
			
			///distance transform
			final RandomAccessibleInterval<NativeBoolType> nucleusPredictionConverted = Converters.convert(
					smoothedPredictions,
					(a, b) -> {
						b.set(a.getInteger()<200);
					},
					new NativeBoolType());

			RandomAccessibleInterval<FloatType> distanceTransform = ArrayImgs.floats(paddedDimension);
			DistanceTransform.binaryTransform(nucleusPredictionConverted, distanceTransform, DISTANCE_TYPE.EUCLIDIAN);
			distanceTransform = Views.offsetInterval(distanceTransform,new long [] {padding, padding, padding}, dimension);
			//shrink
			double distanceCutoff = thinDistanceInVoxels*thinDistanceInVoxels;
			RandomAccessibleInterval<UnsignedByteType> output = Converters.convert(
					distanceTransform,
					(a, b) -> {
						if(a.get()<=distanceCutoff) {//less than 200nm, shrink it
							b.set(0);
						}
						else {
							b.set(255);
						}
					},
					new UnsignedByteType());


			final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
			N5Utils.saveBlock(output, n5BlockWriter, outputName, blockInformation.gridBlock[2]);
		
			//write it out
		});
	}

	public static final List<BlockInformation> convertDataset(final JavaSparkContext sc,final String n5Path,  final String datasetName, long nucelusID, final List<BlockInformation> blockInformationList) throws IOException {
		String outputName = datasetName+"_converted";
		final N5Reader n5Reader = new N5FSReader(n5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final N5Writer n5Writer = new N5FSWriter(n5Path);
		n5Writer.createDataset(
				outputName,
				dimensions,
				blockSize,
				DataType.UINT8,
				new GzipCompression());
		double[] pixelResolution = IOHelper.getResolution(n5Reader, datasetName);
		n5Writer.setAttribute(outputName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
	
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<Set<List<Long>>> javaRDDsets = rdd.map(blockInformation -> {
			final long [] offset= blockInformation.gridBlock[0];
			final long [] dimension = blockInformation.gridBlock[1];
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			RandomAccessibleInterval<UnsignedLongType> cc = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5BlockReader, datasetName)
					),offset, dimension);	
			RandomAccessibleInterval<UnsignedByteType> ccConverted = Converters.convert(
					cc,
					(a, b) -> {
						b.set(a.get() ==nucelusID ? 1 : 0);
					},
					new UnsignedByteType());
			
			Set<List<Long>> blocksToCheck = getBlocksToCheck(ccConverted, blockInformation.gridBlock[2]);
			
			final N5FSWriter n5BlockWriter = new N5FSWriter(n5Path);
			N5Utils.saveBlock(ccConverted, n5BlockWriter, outputName, blockInformation.gridBlock[2]);
			
			return blocksToCheck;
		});
		Set<List<Long>> blocksToCheck = javaRDDsets.reduce((a,b) -> {a.addAll(b); return a; });
		
		int numBlocks  = blockInformationList.size()-1;
		for(int blockIndex = numBlocks; blockIndex>=0; blockIndex--) {
			BlockInformation currentBlock = blockInformationList.get(blockIndex);
			if(!blocksToCheck.contains(Arrays.asList(currentBlock.gridBlock[2][0],currentBlock.gridBlock[2][1],currentBlock.gridBlock[2][2]))) {
				blockInformationList.remove(blockIndex);
			}

		}
		return blockInformationList;
	}
	
	public static Set<List<Long>> getBlocksToCheck(RandomAccessibleInterval<UnsignedByteType> rai, long [] grid){
		IterableInterval<UnsignedByteType> raiFlatIterable = Views.flatIterable(rai);
		boolean containsNucleus = false;
		
		Cursor<UnsignedByteType> raiFlatIterableCursor = raiFlatIterable.cursor();
		while(raiFlatIterableCursor.hasNext() && !containsNucleus) {
			if(raiFlatIterableCursor.next().get()>0) {
				containsNucleus = true;
			}
		}
		Set<List<Long>> blocksToCheck = new HashSet<List<Long>>();

		if(containsNucleus) {
			for(long dx=-1; dx<=1; dx++) {
				for(long dy=-1; dy<=1;dy++) {
					for(long dz=-1; dz<=1; dz++) {
						blocksToCheck.add(Arrays.asList(grid[0]+dx, grid[1]+dy,grid[2]+dz));
					}
				}
			}
		}
		
		return blocksToCheck;
	}
	
	public static final void expandDataset(final JavaSparkContext sc,final String n5Path,  final String datasetName, long nucleusID, final List<BlockInformation> blockInformationList) throws IOException {
		String outputName = datasetName+"_expanded2000";
		final String convertedDatasetName = datasetName+ "_converted";
		final N5Reader n5Reader = new N5FSReader(n5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final N5Writer n5Writer = new N5FSWriter(n5Path);
		n5Writer.createDataset(
				outputName,
				dimensions,
				blockSize,
				DataType.UINT64,
				new GzipCompression());
		double[] pixelResolution = IOHelper.getResolution(n5Reader, convertedDatasetName);
		n5Writer.setAttribute(outputName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));

		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(blockInformation -> {
			final long [] offset= blockInformation.gridBlock[0];
			final long [] dimension = blockInformation.gridBlock[1];
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			
			//smooth
			double expandDistanceInVoxels = 2000/pixelResolution[0];
			long padding = (long) (1+expandDistanceInVoxels); //200 nm shrinking, need to ensure 200 nm from edge, add 2 for buffer
			long [] paddedOffset = new long [] {offset[0]-padding,offset[1]-padding,offset[2]-padding};
			long [] paddedDimension = new long [] {dimension[0]+2*padding,dimension[1]+2*padding,dimension[2]+2*padding};
			
			final RandomAccessibleInterval<NativeBoolType> nucleusCC = Converters.convert(
					(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(n5BlockReader, convertedDatasetName),
					(a, b) -> {
						b.set(a.get() >0);
					},
					new NativeBoolType());
			
			RandomAccessibleInterval<NativeBoolType> nucleusCCConverted = Views.offsetInterval(Views.extendZero(nucleusCC), paddedOffset, paddedDimension);


			RandomAccessibleInterval<FloatType> distanceTransform = ArrayImgs.floats(paddedDimension);
			DistanceTransform.binaryTransform(nucleusCCConverted, distanceTransform, DISTANCE_TYPE.EUCLIDIAN);
			distanceTransform = Views.offsetInterval(distanceTransform,new long [] {padding, padding, padding}, dimension);
			double distanceCutoff = expandDistanceInVoxels*expandDistanceInVoxels;
			RandomAccessibleInterval<UnsignedLongType> output = Converters.convert(
					distanceTransform,
					(a, b) -> {
						if(a.get()<=distanceCutoff) {//less than 200nm, shrink it
							b.set(1L);
						}
						else {
							b.set(0);
						}
					},
					new UnsignedLongType());


			final N5FSWriter n5BlockWriter = new N5FSWriter(n5Path);
			N5Utils.saveBlock(output, n5BlockWriter, outputName, blockInformation.gridBlock[2]);
			//write it out
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
		
		final SparkConf conf = new SparkConf().setAppName("SparkCustomMask");
		List<BlockInformation> blockInformationList = buildBlockInformationList(options.getInputN5Path(), options.getDatasetNameToThin());
		JavaSparkContext sc = new JavaSparkContext(conf);
		if(options.getDoStage1()) {
			thinDataset(sc, options.getInputN5Path(), options.getDatasetNameToThin(), options.getOutputN5Path(), blockInformationList);
			sc.close();
			SparkConnectedComponents.standardConnectedComponentAnalysisWorkflow(conf, options.getDatasetNameToThin()+"_200_thinned200", options.getOutputN5Path(), null, options.getOutputN5Path(), "_cc", 0, 0, false,false);
		}
		long nucleusID = 137029;
		blockInformationList = convertDataset(sc, options.getOutputN5Path(), options.getDatasetNameToThin()+"_200_thinned200_cc", nucleusID, blockInformationList);
		expandDataset(sc, options.getOutputN5Path(), options.getDatasetNameToThin()+"_200_thinned200_cc", nucleusID, blockInformationList);
		//nuclues at 77 largest component
		//ecs at 77 largestComponent
		
	}
}
