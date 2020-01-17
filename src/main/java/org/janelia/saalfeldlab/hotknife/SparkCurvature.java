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
import org.janelia.saalfeldlab.hotknife.ops.CurvatureDGA;
import org.janelia.saalfeldlab.hotknife.ops.GradientCenter;
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

import com.google.common.io.Files;

import ij.IJ;
import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
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
		private String outputN5DatasetSuffix = "_curvatureTesting";

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
		
		n5Writer.createDataset(outputDatasetName, dimensions, blockSize, DataType.FLOAT64, new GzipCompression());

		
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		

		rdd.foreach(blockInformation -> {
			final long[][] gridBlock = blockInformation.gridBlock;
			long[] offset = gridBlock[0];//new long[] {64,64,64};//gridBlock[0];////
			//System.out.println(Arrays.toString(offset));
			long[] dimension = gridBlock[1];
			int sigma = 50; //2 because need to know if surrounding voxels are removablel
			int padding = sigma+1;//Since need extra of 1 around each voxel for curvature
			long[] paddedOffset = new long[]{offset[0]-padding, offset[1]-padding, offset[2]-padding};
			long[] paddedDimension = new long []{dimension[0]+2*padding, dimension[1]+2*padding, dimension[2]+2*padding};
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			

			RandomAccessibleInterval<UnsignedLongType> source = (RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, inputDatasetName);
			
			final RandomAccessibleInterval<DoubleType> sourceConverted =
					Converters.convert(
							source,
							(a, b) -> { b.set(a.getRealDouble()>0 ? 1 : 0);},
							new DoubleType());
			
			final IntervalView<DoubleType> sourceCropped = Views.offsetInterval(Views.extendZero(sourceConverted),paddedOffset, paddedDimension);

			RandomAccessibleInterval<UnsignedByteType> medialSurface = (RandomAccessibleInterval<UnsignedByteType>)N5Utils.open(n5BlockReader, inputDatasetName+"_medialSurface");
			final IntervalView<UnsignedByteType> medialSurfaceCropped = Views.offsetInterval(Views.extendZero(medialSurface),paddedOffset, paddedDimension);

			IntervalView<DoubleType> output = Views.offsetInterval(ArrayImgs.doubles(paddedDimension),new long[]{0,0,0}, paddedDimension);
			curvatureAnalysis(sourceCropped, medialSurfaceCropped, output); 
			
			
			output = Views.offsetInterval(output,new long[]{padding,padding,padding}, dimension);
			
			final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);

			N5Utils.saveBlock(output, n5BlockWriter, outputDatasetName, gridBlock[2]);
			
		});

	}
	
	private static double[][][] sigmaSeries(
			final double[] resolution,
			final int stepsPerOctave,
			final int steps) {

		final double factor = Math.pow(2, 1.0 / stepsPerOctave);

		final int n = resolution.length;
		final double[][][] series = new double[3][steps][n];
		final double minRes = Arrays.stream(resolution).min().getAsDouble();

		double targetSigma = 0.5;
		for (int i = 0; i < steps; ++i) {
			for (int d = 0; d < n; ++d) {
				series[0][i][d] = targetSigma / resolution[d] * minRes;
				series[1][i][d] = Math.max(0.5, series[0][i][d]);
			}
			targetSigma *= factor;
		}
		for (int i = 1; i < steps; ++i) {
			for (int d = 0; d < n; ++d) {
				series[2][i][d] = Math.sqrt(Math.max(0, series[1][i][d] * series[1][i][d] - series[1][i - 1][d] * series[1][i - 1][d]));
			}
		}

		
		return series;
	}

	public static void curvatureAnalysis(RandomAccessibleInterval<DoubleType> converted, RandomAccessibleInterval<UnsignedByteType> medialSurface, RandomAccessibleInterval<DoubleType> output) {
		final int scaleSteps = 11;
		final int octaveSteps = 2;
		final double[] resolution = new double[]{1,  1,  1};
		
		long[] paddedDimension = new long[] {converted.dimension(0), converted.dimension(1), converted.dimension(2)};
		IntervalView<DoubleType> smoothed = Views.offsetInterval(ArrayImgs.doubles(paddedDimension),new long[]{0,0,0}, paddedDimension);

		IntervalView<DoubleType> minimumLaplacian = Views.offsetInterval(ArrayImgs.doubles(paddedDimension),new long[]{0,0,0}, paddedDimension);
		
		IntervalView<DoubleType> e1 = Views.offsetInterval(ArrayImgs.doubles(paddedDimension),new long[]{0,0,0}, paddedDimension);
		IntervalView<DoubleType> e2 = Views.offsetInterval(ArrayImgs.doubles(paddedDimension),new long[]{0,0,0}, paddedDimension);
		IntervalView<DoubleType> e3 = Views.offsetInterval(ArrayImgs.doubles(paddedDimension),new long[]{0,0,0}, paddedDimension);

		
		final double[][][] sigmaSeries = sigmaSeries(resolution, octaveSteps, scaleSteps);
	/*	
		for (int i = 0; i < scaleSteps; ++i) {

		
			System.out.println(
					i + ": " +
					Arrays.toString(sigmaSeries[0][i]) + " : " +
					Arrays.toString(sigmaSeries[1][i]) + " : " +
					Arrays.toString(sigmaSeries[2][i]));
		}
		*/
			
		ExtendedRandomAccessibleInterval<DoubleType, RandomAccessibleInterval<DoubleType>> source =
				Views.extendMirrorSingle(converted);
		final RandomAccessible[] gradients = new RandomAccessible[converted.numDimensions()];
		boolean show = false;
	
		for (int i = 0; i < scaleSteps; ++i) {
			final SimpleGaussRA<DoubleType> op = new SimpleGaussRA<>(sigmaSeries[2][i]);
			op.setInput(source);
			op.run(smoothed);
			source = Views.extendMirrorSingle(smoothed);
		
			System.out.println(i);
			/* gradients */
			for (int d = 0; d < converted.numDimensions(); ++d) {
				final GradientCenter<DoubleType> gradientOp =
						new GradientCenter<>(
								Views.extendBorder(smoothed),
								d,
								sigmaSeries[0][i][d]);
				final IntervalView<DoubleType> gradient = Views.offsetInterval(ArrayImgs.doubles(paddedDimension),new long[]{0,0,0}, paddedDimension);
				gradientOp.accept(gradient);
				gradients[d] = Views.extendZero(gradient);
			}
			
			/* tubeness */
			CurvatureDGA<DoubleType> curvatureOp = new CurvatureDGA<>(gradients, medialSurface, minimumLaplacian, sigmaSeries[0][i]);
			//curvatureOp.accept(output);
			curvatureOp.getEigenvalues(output);//, e1, e2, e3);
			if (show && i>12) {
				ImageJ ij = new ImageJ();
				ImagePlus imp = ImageJFunctions.show(smoothed);
			//	imp.setDimensions(1, 300, 1);
				imp = ImageJFunctions.show(e1);
			//	imp.setDimensions(1, 300, 1);
			//	imp.setDisplayRange(-1, 1);
				imp = ImageJFunctions.show(e2);
			//	imp.setDimensions(1, 300, 1);
			//	imp.setDisplayRange(-1, 1);
				imp = ImageJFunctions.show(e3);
			//	imp.setDimensions(1, 300, 1);
			//	imp.setDisplayRange(-1, 1);
			//	IJ.run(imp, "Merge Channels...", "c1=Image 1 c2=Image 2 c3=Image 3 c4=Image 0 create keep");
				ImageJFunctions.show(output);
			}
		}
		
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
