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
import org.ejml.data.DMatrixRMaj;
import org.ejml.dense.row.decomposition.eig.SymmetricQRAlgorithmDecomposition_DDRM;
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
import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;
import net.imglib2.Cursor;
import net.imglib2.IterableInterval;
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
public class SparkCurvatureBinarized {
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
		
		n5Writer.createDataset(outputDatasetName+"_tubeORsheet", dimensions, blockSize, DataType.UINT8, new GzipCompression());
		n5Writer.setAttribute(outputDatasetName+"_tubeORsheet", "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, inputDatasetName)));

		
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
			
			final IntervalView<DoubleType> sourceCropped = Views.offsetInterval(Views.extendZero(sourceConverted), paddedOffset, paddedDimension);
			
			RandomAccessibleInterval<UnsignedLongType> sourceUnchanging = (RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, inputDatasetName);
			final RandomAccessibleInterval<DoubleType> sourceConvertedUnchanging =
					Converters.convert(
							sourceUnchanging,
							(a, b) -> { b.set(a.getRealDouble()>0 ? 1 : 0);},
							new DoubleType());
			final IntervalView<DoubleType> unchangingSourceCropped = Views.offsetInterval(Views.extendZero(sourceConvertedUnchanging), paddedOffset, paddedDimension);
			
			RandomAccessibleInterval<UnsignedByteType> medialSurface = (RandomAccessibleInterval<UnsignedByteType>)N5Utils.open(n5BlockReader, inputDatasetName+"_medialSurface");
			final IntervalView<UnsignedByteType> medialSurfaceCropped = Views.offsetInterval(Views.extendZero(medialSurface),paddedOffset, paddedDimension);

			IntervalView<UnsignedByteType> sheetness = Views.offsetInterval(ArrayImgs.unsignedBytes(paddedDimension),new long[]{0,0,0}, paddedDimension);
	
			curvatureAnalysis(sourceCropped, unchangingSourceCropped, medialSurfaceCropped, sheetness, new long[]{padding,padding,padding}, dimension); 
			
			final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
			sheetness = Views.offsetInterval(sheetness,new long[]{padding,padding,padding}, dimension);			
			N5Utils.saveBlock(sheetness, n5BlockWriter, outputDatasetName+"_tubeORsheet", gridBlock[2]);
						
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

	public static void curvatureAnalysis(RandomAccessibleInterval<DoubleType> converted, 
			RandomAccessibleInterval<DoubleType> unchangingSourceCropped, RandomAccessibleInterval<UnsignedByteType> medialSurface, 
			RandomAccessibleInterval<UnsignedByteType> sheetness,long[] padding, long[] dimension) {
		final int scaleSteps = 11;
		final int octaveSteps = 2;
		final double[] resolution = new double[]{1,  1,  1};
		
		long[] paddedDimension = new long[] {converted.dimension(0), converted.dimension(1), converted.dimension(2)};
		IntervalView<DoubleType> smoothed = Views.offsetInterval(ArrayImgs.doubles(paddedDimension),new long[]{0,0,0}, paddedDimension);

		IntervalView<DoubleType> minimumLaplacian = Views.offsetInterval(ArrayImgs.doubles(paddedDimension),new long[]{0,0,0}, paddedDimension);
		
		final double[][][] sigmaSeries = sigmaSeries(resolution, octaveSteps, scaleSteps);
		
		for (int i = 0; i < scaleSteps; ++i) {

		
			System.out.println(
					i + ": " +
					Arrays.toString(sigmaSeries[0][i]) + " : " +
					Arrays.toString(sigmaSeries[1][i]) + " : " +
					Arrays.toString(sigmaSeries[2][i]));
		}
		
		ExtendedRandomAccessibleInterval<DoubleType, RandomAccessibleInterval<DoubleType>> source =
				Views.extendZero(converted);
		final RandomAccessible[] gradients = new RandomAccessible[converted.numDimensions()];
		boolean show = false;
	
		for (int i = 0; i < scaleSteps; ++i) {
			final SimpleGaussRA<DoubleType> op = new SimpleGaussRA<>(sigmaSeries[2][i]);
			op.setInput(source);
			op.run(smoothed);
			source = Views.extendZero(smoothed);

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
				getCurvatureInformation(converted,null, gradients, medialSurface, minimumLaplacian, 
					sheetness, null, null,false, sigmaSeries[0][i],padding, dimension);
			
			
			if (show && i>12) {
				ImageJ ij = new ImageJ();
				ImagePlus imp = ImageJFunctions.show(smoothed);
			//	imp.setDimensions(1, 300, 1);
			//	imp.setDisplayRange(-1, 1);
			//	IJ.run(imp, "Merge Channels...", "c1=Image 1 c2=Image 2 c3=Image 3 c4=Image 0 create keep");
			//	ImageJFunctions.show(output);
			}
		}
		
	}
	
	public static <T> void getCurvatureInformation(final RandomAccessibleInterval<DoubleType> converted, 
			final RandomAccessibleInterval<DoubleType> unchangingSourceCropped,
			final RandomAccessible<DoubleType>[] gradients, 
			final RandomAccessible<UnsignedByteType> medialSurface, 
			final RandomAccessible<DoubleType> minimumLaplacian, 
			final RandomAccessible <UnsignedByteType> sheetness,
			final RandomAccessibleInterval<DoubleType> meanCurvature, final RandomAccessibleInterval<DoubleType> gaussianCurvature, 
			boolean getSurfaceCurvatures, 
			final double[] sigmaSeries, long[] padding, long[] dimension) {

		final int n = gradients[0].numDimensions();
		final RandomAccessible<DoubleType>[][] gradientsA = new RandomAccessible[n][n];
		final RandomAccessible<DoubleType>[][] gradientsB = new RandomAccessible[n][n];
		
		double[] norms = new double[n];

		for (int d = 0; d < n; ++d) {
			norms[d] = 2.0 / sigmaSeries[d];//sigmas[d] / 2.0;
			final long[] offset = new long[n];
			offset[d] = -1;
			for (int e = d; e < n; ++e) {
				gradientsA[d][e] = Views.offset(gradients[e], offset);
				gradientsB[d][e] = Views.translate(gradients[e], offset);
			}
		}
		
		final Cursor<DoubleType>[][] a = new Cursor[n][n];
		final Cursor<DoubleType>[][] b = new Cursor[n][n];
		for (int d = 0; d < n; ++d) {
			for (int e = d; e < n; ++e) {
				a[d][e] = Views.flatIterable(Views.interval(gradientsA[d][e], converted)).cursor();
				b[d][e] = Views.flatIterable(Views.interval(gradientsB[d][e], converted)).cursor();
			}
		}
		
		IterableInterval<DoubleType> temp = Views.flatIterable(converted);
	
		final Cursor<DoubleType> sourceCursor = Views.flatIterable(converted).cursor();
		
		final Cursor<UnsignedByteType> sheetnessCursor = Views.flatIterable(Views.interval(sheetness, converted)).cursor();

		final Cursor<UnsignedByteType> medialSurfaceCursor = Views.flatIterable(Views.interval(medialSurface, converted)).cursor();
		final Cursor<DoubleType> minimumLaplacianCursor = Views.flatIterable(Views.interval(minimumLaplacian, converted)).cursor();

		final DMatrixRMaj hessian = new DMatrixRMaj(n, n);
		final SymmetricQRAlgorithmDecomposition_DDRM eigen = new SymmetricQRAlgorithmDecomposition_DDRM(false);//TODO: SWITCH TRUE TO FALSE IF WE DON'T NEED EIGENVECTORS!!!!!
		final double[] eigenvalues = new double[n];

		final int n1 = n - 1;
		final double oneOverN1 = 1.0 / n1;
		int newCount = 0;
		int updatedCount = 0;
A:		while (sourceCursor.hasNext()) {
			/* TODO Is test if n == 1 and set to 1 meaningful? */

			final DoubleType tsource = sourceCursor.next();
			
			final UnsignedByteType tsheetnessCursor = sheetnessCursor.next();
			
			final UnsignedByteType tmedialSurface = medialSurfaceCursor.next();
			final DoubleType tminimumLaplacian = minimumLaplacianCursor.next();

			for (int d = 0; d < n; ++d) {
				for (int e = d; e < n; ++e) {
					final double hde = (b[d][e].next().getRealDouble() - a[d][e].next().getRealDouble()) * norms[e];
//					final double hde = (b[d][e].next().getRealDouble() - a[d][e].next().getRealDouble());
					hessian.set(d, e, hde);
					hessian.set(e, d, hde);
				}
			}
			
			//final T Tsheetness = sheetnessCursor.next();
			if(tmedialSurface.getRealDouble()>0 && isWithinOutputBlock(new long [] {sourceCursor.getLongPosition(0), sourceCursor.getLongPosition(1), sourceCursor.getLongPosition(2)}, padding, dimension)) {

				eigen.decompose(hessian);
				DMatrixRMaj largestEigenvector;
				double largestEigenvalue=-1;
				
				for (int d = 0; d < n; ++d)
					eigenvalues[d] = eigen.getEigenvalue(d).getReal();
				
				DoubleArrays.quickSort(eigenvalues, absDoubleComparator);
				
				double tubeness = 0;
				
				 
				// http://www.cim.mcgill.ca/~shape/publications/miccai05b.pdf
				if(eigenvalues[2]>0) {
					continue A;
				}
				
				double laplacian = hessian.get(0,0)+ hessian.get(1,1) + hessian.get(2,2);

				if(laplacian<tminimumLaplacian.getRealDouble()) {
					if(tminimumLaplacian.getRealDouble()==0) {
						newCount++;
					}
					else {
						updatedCount++;
					}
				//	System.out.println(tminimumLaplacian.getRealDouble()+" "+laplacian);
					double Rsheet = Math.abs(eigenvalues[1]/eigenvalues[2]);
					double alpha = 0.5;
					double sheetEnhancementTerm = Math.exp(-Rsheet*Rsheet/(2*alpha*alpha));
					double Rblob = Math.abs(2*Math.abs(eigenvalues[2])-Math.abs(eigenvalues[1])-Math.abs(eigenvalues[0]))/Math.abs(eigenvalues[2]);
					double beta = 0.5;
					double blobEliminationTerm = 1-Math.exp(-Rblob*Rblob/(2*beta*beta));
					
					double equation1 = sheetEnhancementTerm*blobEliminationTerm;
					tminimumLaplacian.setReal( laplacian);
					tsheetnessCursor.setReal((Math.round(equation1)+1)*127);
					
				}
			}		
		}
		
		System.out.println("Num new: "+newCount + ", Num updated: "+updatedCount+", Total: "+(newCount+updatedCount));
		
		if (getSurfaceCurvatures) {
			sourceCursor.reset();
			final RandomAccess<DoubleType> sourceRandomAccess = unchangingSourceCropped.randomAccess();
			final Cursor<DoubleType> meanCurvatureCursor = Views.flatIterable(Views.interval(meanCurvature, converted)).cursor();
			final Cursor<DoubleType> gaussianCurvatureCursor = Views.flatIterable(Views.interval(gaussianCurvature, converted)).cursor();

			while (sourceCursor.hasNext()) {
				/* TODO Is test if n == 1 and set to 1 meaningful? */
	
				final DoubleType tsource = sourceCursor.next();
				final DoubleType tmeanCurvature = meanCurvatureCursor.next();
				final DoubleType tgaussianCurvature = gaussianCurvatureCursor.next();
	
				for (int d = 0; d < n; ++d) {
					for (int e = d; e < n; ++e) {
						final double hde = (b[d][e].next().getRealDouble() - a[d][e].next().getRealDouble()) * norms[e];
						hessian.set(d, e, hde);
						hessian.set(e, d, hde);
					}
				}
				
				if(isBoundaryVoxel(sourceRandomAccess, new long [] {sourceCursor.getIntPosition(0), sourceCursor.getIntPosition(1), sourceCursor.getIntPosition(2)}, padding, dimension)) {
	
					eigen.decompose(hessian);
				
					
					for (int d = 0; d < n; ++d)
						eigenvalues[d] = eigen.getEigenvalue(d).getReal();
					
					DoubleArrays.quickSort(eigenvalues, absDoubleComparator);

					if (eigenvalues[2]<0) {
						eigenvalues[0]*=-1;
						eigenvalues[1]*=-1;
						tmeanCurvature.setReal((eigenvalues[0]+eigenvalues[1])/2.0);
						tgaussianCurvature.setReal(eigenvalues[0]*eigenvalues[1]);
					}
					/*if(eigenvalues[2]>0) {
						System.out.println("greater than 0");
						DoubleArrays.quickSort(eigenvalues, absDoubleComparator);
					
						tgaussianCurvature.setReal(1);//(eigenvalues[0]*eigenvalues[1]);
					}
					else {
						tgaussianCurvature.setReal(-1);//(eigenvalues[0]*eigenvalues[1]);
					}*/
				}
			}
		}
	}
	
	static DoubleComparator absDoubleComparator = new DoubleComparator() {

		@Override
		public int compare(final double k1, final double k2) {

			final double absK1 = Math.abs(k1);
			final double absK2 = Math.abs(k2);

			return absK1 == absK2 ? 0 : absK1 < absK2 ? -1 : 1;
		}
	};
	
	public static boolean isWithinOutputBlock(long [] sourcePosition, long [] padding, long [] dimension) {
		for(int i =0; i<3; i++) {
			if( !(sourcePosition[i]>=padding[i] && sourcePosition[i]<padding[i]+dimension[i])) {
				return false;
			}
		}
		return true;		
	}
	
	public static boolean isBoundaryVoxel(RandomAccess<DoubleType> ra, long[] sourcePosition, long [] padding, long[] dimension){
		ra.setPosition(sourcePosition);
		if(ra.get().getRealDouble()>0) {
			for(int dx=-1; dx<=1; dx++) {
				for(int dy=-1; dy<=1; dy++) {
					for(int dz=-1; dz<=1; dz++) {
						long newPos [] = new long[]{sourcePosition[0]+dx, sourcePosition[1]+dy, sourcePosition[2]+dz};
						ra.setPosition(newPos);
						if(ra.get().getRealDouble()==0) {
							ra.setPosition(sourcePosition);
							return true;
						}
					}
				}
			}
			ra.setPosition(sourcePosition);
		}
		return false;
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
			finalOutputN5DatasetName = currentOrganelle;// + options.getOutputN5DatasetSuffix();
			
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
