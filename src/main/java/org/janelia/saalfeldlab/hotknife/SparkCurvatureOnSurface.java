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
public class SparkCurvatureOnSurface {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;

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

		public String getOutputN5Path() {
			return outputN5Path;
		}

	}

	public static final void projectCurvatureToSurface(
			final JavaSparkContext sc,
			final String n5Path,
			final String datasetName,
			final String n5OutputPath,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final int n = dimensions.length;
		

		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		n5Writer.createDataset(
				datasetName + "_sheetnessOnSurface_0p50",
				dimensions,
				blockSize,
				DataType.UINT8,
				new GzipCompression());
		n5Writer.setAttribute(datasetName + "_sheetnessOnSurface_0p50", "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, datasetName)));

		n5Writer.createDataset(
				datasetName + "_sheetnessOnSurface_0p75",
				dimensions,
				blockSize,
				DataType.UINT8,
				new GzipCompression());
		n5Writer.setAttribute(datasetName + "_sheetnessOnSurface_0p75", "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, datasetName)));

		
		n5Writer.createDataset(
				datasetName + "_sheetnessOnSurface_0p90",
				dimensions,
				blockSize,
				DataType.UINT8,
				new GzipCompression());
		n5Writer.setAttribute(datasetName + "_sheetnessOnSurface_0p90", "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, datasetName)));
		
		n5Writer.createDataset(
				datasetName + "_sheetnessOnSurface_0p95",
				dimensions,
				blockSize,
				DataType.UINT8,
				new GzipCompression());
		n5Writer.setAttribute(datasetName + "_sheetnessOnSurface_0p95", "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, datasetName)));
		
		n5Writer.createDataset(
				datasetName + "_sheetnessOnSurface_0p99",
				dimensions,
				blockSize,
				DataType.UINT8,
				new GzipCompression());
		n5Writer.setAttribute(datasetName + "_sheetnessOnSurface_0p99", "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, datasetName)));
		

		/*
		 * grid block size for parallelization to minimize double loading of
		 * blocks
		 */
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(blockInformation -> {
			final long [][] gridBlock = blockInformation.gridBlock;
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			boolean show=false;
			if(show) new ImageJ();
			final RandomAccessibleInterval<NativeBoolType> source = Converters.convert(
					(RandomAccessibleInterval<UnsignedLongType>)(RandomAccessibleInterval)N5Utils.open(n5BlockReader, datasetName),
					(a, b) -> {
						b.set(a.getIntegerLong()<1);
					},
					new NativeBoolType());
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
				System.out.println(Arrays.toString(gridBlock[0]) + ", padding = " + Arrays.toString(padding) + ", padded blocksize = " + Arrays.toString(paddedBlockSize));
				
				final long maxBlockDimension = Arrays.stream(paddedBlockSize).max().getAsLong();
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
							System.out.println("padding too small");
							continue A;
						}
	
					final IntervalView<FloatType> botSlice = Views.hyperSlice(insideBlock, d, insideBlock.max(d));
					for (final FloatType t : botSlice)
						if (t.get() >= squareMaxPadding-shellPadding) {
							paddingIsTooSmall = true;
							System.out.println("padding too small");
							continue A;
						}
				}
			}
			
			final RandomAccessibleInterval<UnsignedByteType> medialSurface = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(n5BlockReader, datasetName+"_medialSurface")
					),paddedBlockMin, paddedBlockSize);
			if(show) ImageJFunctions.show(medialSurface,"ms");

			
			final RandomAccessibleInterval<DoubleType> sheetness = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<DoubleType>) N5Utils.open(n5BlockReader, datasetName+"_sheetness")
					),paddedBlockMin, paddedBlockSize);
			
			final Img<UnsignedByteType> output_0p50 = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType())
					.create(paddedBlockSize);
			final Img<UnsignedByteType> output_0p75 = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType())
					.create(paddedBlockSize);
			final Img<UnsignedByteType> output_0p90 = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType())
					.create(paddedBlockSize);
			final Img<UnsignedByteType> output_0p95 = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType())
					.create(paddedBlockSize);
			final Img<UnsignedByteType> output_0p99 = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType())
					.create(paddedBlockSize);
			
			RandomAccess<FloatType> distanceTransformRandomAccess = distanceTransform.randomAccess();
			RandomAccess<DoubleType> sheetnessRandomAccess = sheetness.randomAccess();
			
			RandomAccess<UnsignedByteType> outputRandomAccess_0p50 = output_0p50.randomAccess();
			RandomAccess<UnsignedByteType> outputRandomAccess_0p75 = output_0p75.randomAccess();
			RandomAccess<UnsignedByteType> outputRandomAccess_0p90 = output_0p90.randomAccess();
			RandomAccess<UnsignedByteType> outputRandomAccess_0p95 = output_0p95.randomAccess();
			RandomAccess<UnsignedByteType> outputRandomAccess_0p99 = output_0p99.randomAccess();

			
			Cursor<UnsignedByteType> medialSurfaceCursor = Views.flatIterable(medialSurface).cursor();
			
			while (medialSurfaceCursor.hasNext()) {
				final UnsignedByteType medialSurfaceVoxel = medialSurfaceCursor.next();
				if ( medialSurfaceVoxel.getInteger() ==1) { // then it is on medial surface
					int [] pos = {medialSurfaceCursor.getIntPosition(0),medialSurfaceCursor.getIntPosition(1),medialSurfaceCursor.getIntPosition(2) };
					distanceTransformRandomAccess.setPosition(pos);
					sheetnessRandomAccess.setPosition(pos);

					float radiusSquared = distanceTransformRandomAccess.get().getRealFloat();
					int radius = (int)Math.round(Math.sqrt(radiusSquared));
					int radiusPlusPadding = radius+shellPadding;
					int radiusMinusPadding = radius-shellPadding;
					
					float sheetnessMeasure = sheetnessRandomAccess.get().getRealFloat();
					int sheetORtube_0p50 = Math.round(sheetnessMeasure)*128+127; //255 is sheet, 127 is tube
					int sheetORtube_0p75 = (int) (Math.round(sheetnessMeasure-.25)*128+127); //255 is sheet, 127 is tube
					int sheetORtube_0p90 = (int) (Math.round(sheetnessMeasure-.4)*128+127); //255 is sheet, 127 is tube
					int sheetORtube_0p95 = (int) (Math.round(sheetnessMeasure-.45)*128+127); //255 is sheet, 127 is tube
					int sheetORtube_0p99 = (int) (Math.round(sheetnessMeasure-.49)*128+127); //255 is sheet, 127 is tube

					
					for(int x = pos[0]-radiusPlusPadding; x<=pos[0]+radiusPlusPadding; x++) {
						for(int y = pos[1]-radiusPlusPadding; y<=pos[1]+radiusPlusPadding; y++) {
							for(int z = pos[2]-radiusPlusPadding; z<=pos[2]+radiusPlusPadding; z++) {
								int dx = x-pos[0];
								int dy = y-pos[1];
								int dz = z-pos[2];
								int deltaSquared = dx*dx+dy*dy+dz*dz;
								
								if(deltaSquared<= radiusPlusPadding*radiusPlusPadding && deltaSquared>= radiusMinusPadding*radiusMinusPadding && (x>=0 && x<paddedBlockSize[0] && y>=0 && y < paddedBlockSize[1] && z >= 0 && z < paddedBlockSize[2])) { //then it is in shell
									int [] shellPos = {x,y,z};
								//	distanceTransformRandomAccess.setPosition(shellPos);

								//	if(distanceTransformRandomAccess.get().getRealFloat() <= 2 && distanceTransformRandomAccess.get().getRealFloat() > 0) {//Then is an edge voxel if it is a corner,would be sqrt2
										outputRandomAccess_0p50.setPosition(shellPos);
										outputRandomAccess_0p50.get().setInteger(sheetORtube_0p50);
										outputRandomAccess_0p75.setPosition(shellPos);
										outputRandomAccess_0p75.get().setInteger(sheetORtube_0p75);
										outputRandomAccess_0p90.setPosition(shellPos);
										outputRandomAccess_0p90.get().setInteger(sheetORtube_0p90);
										outputRandomAccess_0p95.setPosition(shellPos);
										outputRandomAccess_0p95.get().setInteger(sheetORtube_0p95);
										outputRandomAccess_0p99.setPosition(shellPos);
										outputRandomAccess_0p99.get().setInteger(sheetORtube_0p99);
								//	}
									
								}
							}
						}
					}
					
				}
			}
			//if(show) ImageJFunctions.show(output_0p50,"output");
			IntervalView<UnsignedByteType> outputCropped = Views.offsetInterval(Views.extendZero(output_0p50), minInside, dimensionsInside);		
			final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
			N5Utils.saveBlock(outputCropped, n5BlockWriter, datasetName + "_sheetnessOnSurface_0p50", gridBlock[2]);
			
			outputCropped = Views.offsetInterval(Views.extendZero(output_0p75), minInside, dimensionsInside);		
			N5Utils.saveBlock(outputCropped, n5BlockWriter, datasetName + "_sheetnessOnSurface_0p75", gridBlock[2]);
			
			outputCropped = Views.offsetInterval(Views.extendZero(output_0p90), minInside, dimensionsInside);		
			N5Utils.saveBlock(outputCropped, n5BlockWriter, datasetName + "_sheetnessOnSurface_0p90", gridBlock[2]);
			
			outputCropped = Views.offsetInterval(Views.extendZero(output_0p95), minInside, dimensionsInside);		
			N5Utils.saveBlock(outputCropped, n5BlockWriter, datasetName + "_sheetnessOnSurface_0p95", gridBlock[2]);
			
			outputCropped = Views.offsetInterval(Views.extendZero(output_0p99), minInside, dimensionsInside);		
			N5Utils.saveBlock(outputCropped, n5BlockWriter, datasetName + "_sheetnessOnSurface_0p99", gridBlock[2]);
		
		
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

		String tempOutputN5DatasetName = null;
		String finalOutputN5DatasetName = null;
		List<String> directoriesToDelete = new ArrayList<String>();
		for (String currentOrganelle : organelles) {
			logMemory(currentOrganelle);	
			
			//Create block information list
			List<BlockInformation> blockInformationList = buildBlockInformationList(options.getInputN5Path(),
				currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			projectCurvatureToSurface(
					sc,
					options.getInputN5Path(),
					options.getInputN5DatasetName(),
					options.getOutputN5Path(),
					blockInformationList);
			/*double weights [] = {0.0001}; 
			for (double weight : weights) {
				calculateDistanceTransform(sc, options.getInputN5Path(), currentOrganelle,
						options.getOutputN5Path(), "distance_transform_w"+String.valueOf(weight).replace('.', 'p'), new long[] {16,16,16}, weight, blockInformationList);
			}*/
		
			/*int minimumVolumeCutoff = options.getMinimumVolumeCutoff();
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
			*/
			
			sc.close();
		}
		//Remove temporary files
		SparkDirectoryDelete.deleteDirectories(conf, directoriesToDelete);

	}
}
