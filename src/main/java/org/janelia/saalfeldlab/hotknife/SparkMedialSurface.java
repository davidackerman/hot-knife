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
import java.util.Set;
import java.util.concurrent.ExecutionException;

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

import bdv.labels.labelset.Label;
import ij.ImageJ.*;
import ij.IJ;
import ij.ImagePlus;
import net.imagej.ImageJ;
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
import net.imglib2.img.basictypeaccess.array.BooleanArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.label.Test;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.SubsampleIntervalView;
import net.imglib2.view.Views;
import net.imagej.ops.morphology.thin.*;
import net.imglib2.type.logic.BitType;

import ij.plugin.*;

/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkMedialSurface {
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
	}

	public static final void performThinning(final JavaSparkContext sc, final String n5Path,
			final String datasetName, final String n5OutputPath, final String outputDatasetName,
			final long[] initialPadding, final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final int n = dimensions.length;

		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		n5Writer.createDataset(outputDatasetName, dimensions, blockSize, DataType.UINT8, new GzipCompression());

		/*
		 * grid block size for parallelization to minimize double loading of blocks
		 */
		boolean show = false;
		if (show)
			new ImageJ();
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(blockInformation -> {
			final long[][] gridBlock = blockInformation.gridBlock;
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			final RandomAccessibleInterval<NativeBoolType> source;
			source = Converters.convert(
					(RandomAccessibleInterval<IntegerType<?>>) (RandomAccessibleInterval) N5Utils.open(n5BlockReader,
							datasetName),
					(a, b) -> {
						final double x = (a.getInteger() - 127) / 128.0;
						final double d = 25 * Math.log((1 + x) / (1 - x));
						b.set(a.getInteger() < 127);
					}, new NativeBoolType());
			
			final long[] padding = initialPadding.clone();
			A: for (boolean paddingIsTooSmall = true; paddingIsTooSmall; Arrays.setAll(padding,
					i -> padding[i] + initialPadding[i])) {

				paddingIsTooSmall = false;

				final long maxPadding = Arrays.stream(padding).max().getAsLong();
				final long squareMaxPadding = maxPadding * maxPadding;
				final long[] paddedBlockMin = new long[n];
				final long[] paddedBlockSize = new long[n];
				Arrays.setAll(paddedBlockMin, i -> gridBlock[0][i] - padding[i]);
				Arrays.setAll(paddedBlockSize, i -> gridBlock[1][i] + 2 * padding[i]);
				System.out.println(Arrays.toString(gridBlock[0]) + ", padding = " + Arrays.toString(padding)
						+ ", padded blocksize = " + Arrays.toString(paddedBlockSize));

				final long maxBlockDimension = Arrays.stream(paddedBlockSize).max().getAsLong();
				
				final IntervalView<NativeBoolType> sourceBlock = Views.offsetInterval(
						Views.extendValue(source, new NativeBoolType(true)), // new FloatType(Label.OUTSIDE)),
						paddedBlockMin, paddedBlockSize);
				
				
				//System.out.println(count);

				/* make distance transform */
				final NativeImg<FloatType, ?> targetBlock = ArrayImgs.floats(paddedBlockSize);
				
				DistanceTransform.binaryTransform(sourceBlock, targetBlock, DISTANCE_TYPE.EUCLIDIAN);
				if (show) ImageJFunctions.show(targetBlock, "dt");

				final long[] minInside = new long[n];
				final long[] dimensionsInside = new long[n];
				Arrays.setAll(minInside, i -> padding[i]);
				Arrays.setAll(dimensionsInside, i -> gridBlock[1][i]);

				final IntervalView<FloatType> insideBlock = Views.offsetInterval(targetBlock, minInside,dimensionsInside);
				if (show) ImageJFunctions.show(insideBlock, "inside");

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
				RandomAccessibleInterval<UnsignedByteType> inputForSkeleton = (RandomAccessibleInterval) N5Utils.open(n5BlockReader,
						datasetName); 

				final IntervalView<UnsignedByteType> inputForSkeletonBlock = Views.offsetInterval(
						Views.extendZero(inputForSkeleton),
						paddedBlockMin, paddedBlockSize);
				
				ImagePlusImg< UnsignedByteType, ? > intermediateOutputImage = new ImagePlusImgFactory<>( new UnsignedByteType() ).create( paddedBlockSize );
				final Cursor<UnsignedByteType> intermediateOutputImageCursor = intermediateOutputImage.cursor();
				final Cursor<UnsignedByteType> inputForSkeletonBlockCursor = inputForSkeletonBlock.cursor();
				while(inputForSkeletonBlockCursor.hasNext()) {
					UnsignedByteType v1 = inputForSkeletonBlockCursor.next();
					UnsignedByteType v2 = intermediateOutputImageCursor.next();
					if(v1.get()>=127) {
						v2.set(255);
					}
				}
				if(show) {
					ImageJFunctions.show(intermediateOutputImage,"inputImage");
				}
				IJ.run(intermediateOutputImage.getImagePlus(),"Skeletonize (2D/3D)","");

				final RandomAccessibleInterval<UnsignedByteType> finalOutputImage =  Views.offsetInterval(intermediateOutputImage,minInside,dimensionsInside);
				if(show == true) {
					ImageJFunctions.show(intermediateOutputImage,"inputImage");
					ImageJFunctions.show(finalOutputImage,"inputblock");
				}
				
				
				
				/*net.imagej.ImageJ ij = new net.imagej.ImageJ();	
				final Img<BitType> outImg = (Img<BitType>) ij.op().run(ThinZhangSuen.class, Img.class, inputForSkeletonBlock);
				final IntervalView<BitType> outImgCrop = Views.offsetInterval(outImg, minInside,dimensionsInside);

				final NativeImg<UnsignedShortType, ?> finalOutputImage = ArrayImgs.unsignedShorts(dimensionsInside);
				final Cursor<UnsignedShortType> finalOutputImageCursor = finalOutputImage.cursor();
				final Cursor<BitType> outImgCropCursor = outImgCrop.cursor();
				while(outImgCropCursor.hasNext()) {
					BitType outImgVoxel = outImgCropCursor.next();
					UnsignedShortType finalOutputImageVoxel = finalOutputImageCursor.next();
					if(outImgVoxel.get()) {
						finalOutputImageVoxel.set(255);
					}
				}
				if (show == true) {
					ImageJFunctions.show(outImg, "outImg");
					ImageJFunctions.show(outImgCrop, "outImgCrop");
					ImageJFunctions.show(finalOutputImage, "finalOutputImage");
				}
				*/

				final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
				N5Utils.saveBlock(finalOutputImage, n5BlockWriter, outputDatasetName, gridBlock[2]);
			}
		});
	}

	public static final void tempPerformThinning(final JavaSparkContext sc, final String n5Path,
			final String datasetName, final String n5OutputPath, final String outputDatasetName,
			final long[] initialPadding, final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final int n = dimensions.length;

		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		n5Writer.createDataset(outputDatasetName, dimensions, blockSize, DataType.UINT8, new GzipCompression());

		/*
		 * grid block size for parallelization to minimize double loading of blocks
		 */
		boolean show = true;
		if (show)
			new ij.ImageJ();
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(blockInformation -> {
			final long[][] gridBlock = blockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			
			long [] paddedOffset = {offset[0]-1, offset[1]-1, offset[2]-1};
			long [] paddedDimension = {dimension[0]+1, dimension[1]+1, dimension[2]+1};
			
			
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			final RandomAccessibleInterval<UnsignedLongType> source = (RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, datasetName);
			final IntervalView<UnsignedLongType> inputForSkeletonization = Views.offsetInterval(
					Views.extendValue(source, new UnsignedLongType(1)),
					paddedOffset, paddedDimension);
			
			ImagePlusImg< UnsignedByteType, ? > intermediateOutputImage = new ImagePlusImgFactory<>( new UnsignedByteType() ).create( paddedDimension  );
			final Cursor<UnsignedByteType> intermediateOutputImageCursor = intermediateOutputImage.cursor();
			
			final Cursor<UnsignedLongType> inputForSkeletonizationCursor = inputForSkeletonization.cursor();

			int objectVoxelCount = 0;
			while(inputForSkeletonizationCursor.hasNext()) {
				UnsignedLongType v1 = inputForSkeletonizationCursor.next();
				UnsignedByteType v2 = intermediateOutputImageCursor.next();
				if(v1.getLong()>0) {
					v2.set(255);
					objectVoxelCount++;
				}
			}
			if (show)
				intermediateOutputImage.getImagePlus().show();
			
			boolean needToThinAgain = false;
			if (objectVoxelCount == 0 || objectVoxelCount == dimensions[0]*dimensions[1]*dimensions[2]) {
				//cant thin yet;
				needToThinAgain = true;
			}
			else {
				//Skeletonize3D_ skeletonize3D = new Skeletonize3D_();
				///needToThinAgain = skeletonize3D.thinningForParallelization(intermediateOutputImage.getImagePlus());
			}
			if (show)
				intermediateOutputImage.getImagePlus().show();
			
			final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
			N5Utils.saveBlock(intermediateOutputImage, n5BlockWriter, outputDatasetName, gridBlock[2]);
		});
	}

	
	public static final void calculateMedialSurface(final JavaSparkContext sc, final String n5Path,
			final String datasetName, final String n5OutputPath, final String outputDatasetName,
			final long[] initialPadding, final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final int n = dimensions.length;

		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		n5Writer.createDataset(outputDatasetName, dimensions, blockSize, DataType.UINT16, new GzipCompression());

		/*
		 * grid block size for parallelization to minimize double loading of blocks
		 */
		boolean show = true;
		if (show)
			new ImageJ();
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(blockInformation -> {
			final long[][] gridBlock = blockInformation.gridBlock;
			// final long [][] gridBlock= {{64, 128, 64}, {64, 64, 64}, {1, 2, 1}};
			//gridBlock[0][0]=490;
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			final RandomAccessibleInterval<NativeBoolType> source;
			source = Converters.convert(
					// for OpenJDK 8, Eclipse could do without the intermediate raw cast...
					(RandomAccessibleInterval<IntegerType<?>>) (RandomAccessibleInterval) N5Utils.open(n5BlockReader,
							datasetName),
					(a, b) -> {
						final double x = (a.getInteger() - 127) / 128.0;
						final double d = 25 * Math.log((1 + x) / (1 - x));
						b.set(a.getInteger() < 127);
						// b.set((float)a.getRealDouble());
						// if(d>0) b.set(1);
						// else b.set(0);
					}, new NativeBoolType());
			
			final long[] padding = initialPadding.clone();
			A: for (boolean paddingIsTooSmall = true; paddingIsTooSmall; Arrays.setAll(padding,
					i -> padding[i] + initialPadding[i])) {

				paddingIsTooSmall = false;

				final long maxPadding = Arrays.stream(padding).max().getAsLong();
				final long squareMaxPadding = maxPadding * maxPadding;
				final long[] paddedBlockMin = new long[n];
				final long[] paddedBlockSize = new long[n];
				Arrays.setAll(paddedBlockMin, i -> gridBlock[0][i] - padding[i]);
				Arrays.setAll(paddedBlockSize, i -> gridBlock[1][i] + 2 * padding[i]);
				System.out.println(Arrays.toString(gridBlock[0]) + ", padding = " + Arrays.toString(padding)
						+ ", padded blocksize = " + Arrays.toString(paddedBlockSize));

				final long maxBlockDimension = Arrays.stream(paddedBlockSize).max().getAsLong();
				
				final IntervalView<NativeBoolType> sourceBlock = Views.offsetInterval(
						Views.extendValue(source, new NativeBoolType(true)), // new FloatType(Label.OUTSIDE)),
						paddedBlockMin, paddedBlockSize);
				
				RandomAccessibleInterval temp = Views.offsetInterval(
						(RandomAccessibleInterval) N5Utils.open(n5BlockReader,
								datasetName), // new FloatType(Label.OUTSIDE)),
						new long[]{450,-10,-10}, paddedBlockSize);
				if (show) ImageJFunctions.show(temp, "sourcestuff");
				Cursor tempCursor = Views.flatIterable(temp).cursor();
				int count=0;
				while(tempCursor.hasNext()) {
					tempCursor.next();
					count++;
				}
				//System.out.println(count);

				/* make distance transform */
				final NativeImg<FloatType, ?> targetBlock = ArrayImgs.floats(paddedBlockSize);
				/*
				final NativeImg<NativeBoolType, ?> tempSourceBlock = ArrayImgs.booleans(paddedBlockSize);

				final Cursor<NativeBoolType> tempSourceBlockCursor = Views.flatIterable(tempSourceBlock).cursor();
				while (tempSourceBlockCursor.hasNext()) {
					final NativeBoolType voxel = tempSourceBlockCursor.next();
					int[] sourcePosition = new int[3];
					Arrays.setAll(sourcePosition, i -> tempSourceBlockCursor.getIntPosition(i));
					voxel.set(true);
					if (sourcePosition[0] > 30 && sourcePosition[0] < 70 && sourcePosition[1] > 30 && sourcePosition[1] < 70 && sourcePosition[2] > 30 && sourcePosition[2] < 70) {
						//if (Math.pow(sourcePosition[0] - 50, 2) + Math.pow(sourcePosition[1] - 50, 2) <= 200) {
							voxel.set(false);
					//	}
					}
				}
				
				if (show) ImageJFunctions.show(tempSourceBlock, "sourceBlock");
				DistanceTransform.binaryTransform(tempSourceBlock, targetBlock, DISTANCE_TYPE.EUCLIDIAN);
				 */
				
				DistanceTransform.binaryTransform(sourceBlock, targetBlock, DISTANCE_TYPE.EUCLIDIAN);
				if (show) ImageJFunctions.show(targetBlock, "dt");

				final long[] minInside = new long[n];
				final long[] dimensionsInside = new long[n];
				Arrays.setAll(minInside, i -> padding[i]);
				Arrays.setAll(dimensionsInside, i -> gridBlock[1][i]);

				final IntervalView<FloatType> insideBlock = Views.offsetInterval(targetBlock, minInside,dimensionsInside);
				if (show) ImageJFunctions.show(insideBlock, "inside");

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
				
				RandomAccessibleInterval<FloatType> medialAxisTransformImage = medialAxisTransform(sourceBlock, insideBlock, padding);
				
				/* padding was sufficient, save */
				final RandomAccessibleInterval<UnsignedShortType> convertedOutputBlock = Converters.convert(
						(RandomAccessibleInterval<FloatType>) insideBlock, (a, b) -> b.set((int) a.get()),
						new UnsignedShortType());
				if (show == true)
					ImageJFunctions.show(medialAxisTransformImage, "output");

				final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
				N5Utils.saveNonEmptyBlock(convertedOutputBlock, n5BlockWriter, outputDatasetName, gridBlock[2],
						new UnsignedShortType(2));
			}
		});
	}

	public static RandomAccessibleInterval<FloatType> medialAxisTransform(final RandomAccessibleInterval<NativeBoolType> sourceImage) {
		final NativeImg<FloatType, ?> targetImage = ArrayImgs
				.floats(new long[] { sourceImage.dimension(0), sourceImage.dimension(1), sourceImage.dimension(2) });
		DistanceTransform.binaryTransform(sourceImage, targetImage, DISTANCE_TYPE.EUCLIDIAN);
		final RandomAccessibleInterval<FloatType> medialAxisTransformImage = medialAxisTransform(sourceImage, targetImage,new long[] {0,0,0});
		return medialAxisTransformImage;
	}

	public static  RandomAccessibleInterval<FloatType> medialAxisTransform(final RandomAccessibleInterval<NativeBoolType> sourceImage,
			final RandomAccessibleInterval<FloatType> binaryDistanceTransform, final long[] padding) {
		RandomAccess<NativeBoolType> sourceImageRandomAccess = sourceImage.randomAccess();
		final Cursor<FloatType> binaryDistanceTransformCursor = Views.flatIterable(binaryDistanceTransform).cursor();

		// Calculate medial axis from distance transform
		while (binaryDistanceTransformCursor.hasNext()) {
			FloatType voxel = binaryDistanceTransformCursor.next();
			if (voxel.get() > 0) {
				int nearestBoundaryCount = 0;
				Set<List<Long>> voxelsToCheck = getVoxelsToCheck((int) voxel.get());
				ArrayList<List<Long>> edgeVoxels = new ArrayList<List<Long>>();
				boolean greaterThanNinety = false;
				long[] currentPosition = new long[] { binaryDistanceTransformCursor.getLongPosition(0)+padding[0],
						binaryDistanceTransformCursor.getLongPosition(1)+padding[1],
						binaryDistanceTransformCursor.getLongPosition(2)+padding[2] };
				for (List<Long> currentVoxelToCheck : voxelsToCheck) {
					final long[] sourcePosition = new long[3];
					Arrays.setAll(sourcePosition, i -> currentPosition[i] + currentVoxelToCheck.get(i));
					sourceImageRandomAccess.setPosition(sourcePosition);
					if (sourceImageRandomAccess.get().get() == true) {
						edgeVoxels.add(currentVoxelToCheck);
						nearestBoundaryCount++;
						//check to see if they are at least 90 degrees apart
						List<Long> v1 = edgeVoxels.get(edgeVoxels.size()-1);
						for(int i=0; i<edgeVoxels.size()-1; i++) {
							List<Long> v2 = edgeVoxels.get(i);
							double angle = Math.acos( (v1.get(0)*v2.get(0) + v1.get(1)*v2.get(1)+v1.get(2)*v2.get(2))/voxel.get() );
							greaterThanNinety = angle>Math.PI/2;
						}
					}
					if (nearestBoundaryCount > 1) {
						break;
					}
				}
				if (nearestBoundaryCount <= 1) {
					voxel.set(0);
				}
			}
		}
		return binaryDistanceTransform;
	}

	public static RandomAccessibleInterval<UnsignedShortType> scaledMedialAxisTransform(final RandomAccessibleInterval<NativeBoolType> sourceImage, final RandomAccessibleInterval<FloatType> binaryDistanceTransform, double scaleFactor, final long [] padding) {
		final RandomAccessibleInterval<FloatType> medialAxisTransformImage = medialAxisTransform(sourceImage, binaryDistanceTransform,padding);
		ImageJFunctions.show(medialAxisTransformImage, "medial");
		final RandomAccessibleInterval<NativeBoolType> inverseScaledMedialAxisTransformImage = inverseScaledMedialAxisTransform(medialAxisTransformImage, scaleFactor);
		ImageJFunctions.show(inverseScaledMedialAxisTransformImage,"inversescaled");
		final RandomAccessibleInterval<FloatType> scaledMedialAxisTransformImage = medialAxisTransform(inverseScaledMedialAxisTransformImage);
		ImageJFunctions.show(scaledMedialAxisTransformImage,"scaled");
		final RandomAccessibleInterval<UnsignedShortType> scaledMedialAxisImageBinarized = Converters.convert(
				scaledMedialAxisTransformImage, (a, b) -> {
					if(a.get()>0) {
						b.set(255);
					}
				},
				new UnsignedShortType());
		ImageJFunctions.show(scaledMedialAxisImageBinarized,"scaledbinary");
		return scaledMedialAxisImageBinarized;
	}

	public static RandomAccessibleInterval<NativeBoolType> inverseScaledMedialAxisTransform(final RandomAccessibleInterval<FloatType> medialAxisTransformImage, double scalingFactor) {
		final Cursor<FloatType> medialAxisTransformImageCursor = Views.flatIterable(medialAxisTransformImage).cursor();
		
		final NativeImg<NativeBoolType, ?> inverseScaledMedialAxisTransformImage = ArrayImgs.booleans(new long[] { medialAxisTransformImage.dimension(0), medialAxisTransformImage.dimension(1), medialAxisTransformImage.dimension(2) });
		final Cursor<NativeBoolType> inverseScaledMedialAxisCursor = Views.flatIterable(inverseScaledMedialAxisTransformImage).cursor();
		final RandomAccess<NativeBoolType> inverseScaledMedialAxisTransformRandomAccess = inverseScaledMedialAxisTransformImage.randomAccess();
		
		while (inverseScaledMedialAxisCursor.hasNext()) {
			inverseScaledMedialAxisCursor.next().set(true);
		}
		
		while (medialAxisTransformImageCursor.hasNext()) {
			FloatType voxel = medialAxisTransformImageCursor.next();
			long[] currentPosition = new long[] { medialAxisTransformImageCursor.getLongPosition(0), medialAxisTransformImageCursor.getLongPosition(1), medialAxisTransformImageCursor.getLongPosition(2) };
			inverseScaledMedialAxisTransformRandomAccess.setPosition(new long[] { currentPosition[0], currentPosition[1], currentPosition[2] });
			if (voxel.get() > 0) {
				final int r = (int) Math.ceil(scalingFactor * Math.sqrt(voxel.get()));
				for (int x = -r; x <= r; x++) {
					for (int y = -r; y <= r; y++) {
						for (int z = -r; z <= r; z++) {
							if (Math.pow(x, 2) + Math.pow(y, 2) + Math.pow(z, 2) <= Math.pow(r, 2)) {
								final int [] xyz = {x,y,z};
								final long[] inverseScaledMedialAxisTransformPosition = new long[3];
								Arrays.setAll(inverseScaledMedialAxisTransformPosition, i->currentPosition[i]+xyz[i]);
								inverseScaledMedialAxisTransformRandomAccess.setPosition(inverseScaledMedialAxisTransformPosition);
								inverseScaledMedialAxisTransformRandomAccess.get().set(false);
							}
						}
					}
				}
			}
		}
		return inverseScaledMedialAxisTransformImage;
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

	static Set<List<Long>> getVoxelsToCheck(int n) {
		Set<List<Long>> voxelsToCheck = new HashSet<>();
		// should be the sum of 3 cubes
		for (long i = 0; i * i <= n; i++) {
			for (long j = 0; j * j <= n; j++) {
				for (long k = 0; k * k <= n; k++) {
					if (i * i + j * j + k * k == n) {
						int plusOrMinus[][] = { { 1, 1, 1 }, { -1, 1, 1 }, { 1, -1, 1 }, { 1, 1, -1 }, { -1, -1, 1 },
								{ -1, 1, -1 }, { 1, -1, -1 }, { -1, -1, -1 } };
						for (int index = 0; index < 8; index++) {
							voxelsToCheck.add(Arrays.asList(plusOrMinus[index][0] * i, plusOrMinus[index][1] * j, plusOrMinus[index][2] * k));
							voxelsToCheck.add(Arrays.asList(plusOrMinus[index][0] * i, plusOrMinus[index][1] * k, plusOrMinus[index][2] * j));
							voxelsToCheck.add(Arrays.asList(plusOrMinus[index][0] * j, plusOrMinus[index][1] * i, plusOrMinus[index][2] * k));
							voxelsToCheck.add(Arrays.asList(plusOrMinus[index][0] * j, plusOrMinus[index][1] * k, plusOrMinus[index][2] * i));
							voxelsToCheck.add(Arrays.asList(plusOrMinus[index][0] * k, plusOrMinus[index][1] * i, plusOrMinus[index][2] * j));
							voxelsToCheck.add(Arrays.asList(plusOrMinus[index][0] * k, plusOrMinus[index][1] * j, plusOrMinus[index][2] * i));
						}
						return voxelsToCheck;

					}
				}
			}
		}
		if (voxelsToCheck.isEmpty()) {
			throw new RuntimeException("Voxels to check is empty");
		}
		return voxelsToCheck;
	}

	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {
		System.setProperty("plugins.dir", "/groups/scicompsoft/home/ackermand/Fiji.app/plugins/");

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
		for (String currentOrganelle : organelles) {
			tempOutputN5DatasetName = currentOrganelle + options.getOutputN5DatasetSuffix()
					+ "_blockwise_temp_to_delete";
			finalOutputN5DatasetName = currentOrganelle + options.getOutputN5DatasetSuffix();

			// Create block information list
			List<BlockInformation> blockInformationList = buildBlockInformationList(options.getInputN5Path(),
					currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);

			tempPerformThinning(sc, options.getInputN5Path(), currentOrganelle, options.getOutputN5Path(),
					"skeletonize", new long[] { 16, 16, 16 }, blockInformationList);

			sc.close();
		}

		// Remove temporary files
		for (String currentOrganelle : organelles) {
			tempOutputN5DatasetName = currentOrganelle + options.getOutputN5DatasetSuffix()
					+ "_blockwise_temp_to_delete";
			FileUtils.deleteDirectory(new File(options.getOutputN5Path() + "/" + tempOutputN5DatasetName));
		}

	}
}
