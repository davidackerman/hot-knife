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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
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

import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;


/**
 * Calculate curvature for a dataset
 * Borrowed from https://github.com/saalfeldlab/hot-knife/blob/tubeness/src/test/java/org/janelia/saalfeldlab/hotknife/LazyBehavior.java
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkCurvature {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /path/to/input/data.n5.")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /path/to/output/data.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. organelle. Requires organelle_medialSurface as well.")
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

	/**
	 * Compute curvatures for objects in images.
	 *
	 * Calculates the sheetness of objects in images at their medial surfaces. Repetitively smoothes image, stopping for a given medial surface voxel when the laplacian at that voxel is smallest. Then calculates sheetness based on all corresponding eigenvalues of hessian.
	 * 
	 * @param sc
	 * @param n5Path
	 * @param inputDatasetName
	 * @param n5OutputPath
	 * @param outputDatasetName
	 * @param blockInformationList
	 * @throws IOException
	 */
	public static final void computeCurvature(final JavaSparkContext sc, final String n5Path,
			final String inputDatasetName, final String n5OutputPath, String outputDatasetName,
			final List<BlockInformation> blockInformationList) throws IOException {

		//Read in input block information
		final N5Reader n5Reader = new N5FSReader(n5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final double [] pixelResolution = IOHelper.getResolution(n5Reader, inputDatasetName);

		//Create output
		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);	
		n5Writer.createDataset(outputDatasetName+"_sheetnessTest3", dimensions, blockSize, DataType.FLOAT64, new GzipCompression());
		n5Writer.setAttribute(outputDatasetName+"_sheetnessTest3", "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
		
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(blockInformation -> {
			//Get information for processing blocks
			final long[][] gridBlock = blockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			int sigma = 50;
			int padding = sigma+1;//Since need extra of 1 around each voxel for curvature
			long[] paddedOffset = new long[]{offset[0]-padding, offset[1]-padding, offset[2]-padding};
			long[] paddedDimension = new long []{dimension[0]+2*padding, dimension[1]+2*padding, dimension[2]+2*padding};
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			
			//Binarize segmentation data and read in medial surface info
			RandomAccessibleInterval<UnsignedLongType> source = (RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, inputDatasetName);
			final RandomAccessibleInterval<DoubleType> sourceConverted =
					Converters.convert(
							source,
							(a, b) -> { b.set(a.getRealDouble()>0 ? 1 : 0);},
							new DoubleType());
			final IntervalView<DoubleType> sourceCropped = Views.offsetInterval(Views.extendZero(sourceConverted), paddedOffset, paddedDimension);

			
			RandomAccessibleInterval<UnsignedLongType> medialSurface = (RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, inputDatasetName+"_medialSurface");
			
			final IntervalView<UnsignedLongType> medialSurfaceCropped = Views.offsetInterval(Views.extendZero(medialSurface),paddedOffset, paddedDimension);
			RandomAccess<UnsignedLongType> medialSurfaceCroppedRA = medialSurfaceCropped.randomAccess();
			HashMap<List<Long>,SheetnessInformation> medialSurfaceCoordinatesToSheetnessInformationMap = new HashMap<List<Long>,SheetnessInformation>();
			for(long x=padding; x<padding+dimension[0]; x++) {
				for(long y=padding; y<padding+dimension[1]; y++) {
					for(long z=padding; z<padding+dimension[2]; z++) {
						long [] pos = new long[] {x,y,z};
						medialSurfaceCroppedRA.setPosition(pos);
						if(medialSurfaceCroppedRA.get().get()>0) {
							medialSurfaceCoordinatesToSheetnessInformationMap.put(Arrays.asList(pos[0],pos[1],pos[2]),new SheetnessInformation());
						}
					}
				}
			}
			
			//Create sheetness output
			IntervalView<DoubleType> sheetness = null;			
			
			//Perform curvature analysis
			if(!medialSurfaceCoordinatesToSheetnessInformationMap.isEmpty()) {
				getSheetness(sourceCropped, medialSurfaceCoordinatesToSheetnessInformationMap, new long[]{padding,padding,padding}, dimension, pixelResolution); 
				sheetness = Views.offsetInterval(ArrayImgs.doubles(paddedDimension),new long[]{0,0,0}, paddedDimension);
				RandomAccess<DoubleType> sheetnessRA = sheetness.randomAccess();
				for(Entry<List<Long>, SheetnessInformation> entry : medialSurfaceCoordinatesToSheetnessInformationMap.entrySet()) {
					long[] pos = new long[] {entry.getKey().get(0), entry.getKey().get(1), entry.getKey().get(2)};
					sheetnessRA.setPosition(pos);
					sheetnessRA.get().set(entry.getValue().sheetness);
				}
				sheetness = Views.offsetInterval(sheetness,new long[]{padding,padding,padding}, dimension);
			}
			else{
				sheetness = Views.offsetInterval(ArrayImgs.doubles(dimension),new long[]{0,0,0}, dimension);
			}
			
			final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
			N5Utils.saveBlock(sheetness, n5BlockWriter, outputDatasetName+"_sheetnessTest3", gridBlock[2]);
						
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

	public static void getSheetness(RandomAccessibleInterval<DoubleType> converted, HashMap<List<Long>, SheetnessInformation> medialSurfaceCoordinatesToSheetnessInformationMap, 
			long[] padding, long[] dimension, double[] resolution) {
		
		//Define scale steps and octave steps
		final int scaleSteps = 11;
		final int octaveSteps = 2;
		long[] paddedDimension = new long[] {converted.dimension(0), converted.dimension(1), converted.dimension(2)};
		
		//Create required images for calculating sheetness
		ExtendedRandomAccessibleInterval<DoubleType, RandomAccessibleInterval<DoubleType>> source = Views.extendZero(converted);
		IntervalView<DoubleType> smoothed = Views.offsetInterval(ArrayImgs.doubles(paddedDimension),new long[]{0,0,0}, paddedDimension);
		final RandomAccessible<DoubleType>[] gradients = new RandomAccessible[converted.numDimensions()];
			
		//Loop over scale steps to calculate smoothed image
		final double[][][] sigmaSeries = sigmaSeries(resolution, octaveSteps, scaleSteps);
		for (int i = 0; i < scaleSteps; ++i) {
			final SimpleGaussRA<DoubleType> op = new SimpleGaussRA<>(sigmaSeries[2][i]);
			op.setInput(source);
			op.run(smoothed);
			source = Views.extendZero(smoothed);

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
			
			//Update sheetness if necessary
			updateSheetness(converted, gradients, medialSurfaceCoordinatesToSheetnessInformationMap, 
					sigmaSeries[0][i],padding, dimension, i);
		}
		
	}
	
	private static void updateSheetness(final RandomAccessibleInterval<DoubleType> converted, 
			final RandomAccessible<DoubleType>[] gradients, 
			final HashMap<List<Long>, SheetnessInformation> medialSurfaceCoordinatesToSheetnessInformationMap, 
			final double[] sigmaSeries, long[] padding, long[] dimension, int scaleStep) {

		//Create gradients
		final int n = gradients[0].numDimensions();
		
		double[] norms = new double[n];
		for (int d = 0; d < n; ++d) {
			norms[d] = 2.0 / sigmaSeries[d];//sigmas[d] / 2.0;
		}
		
		RandomAccess<DoubleType> gradientA_RA = null;
		RandomAccess<DoubleType> gradientB_RA = null;
		for (int d = 0; d < n; ++d) {
			final long[] offset = new long[n];
			offset[d] = -1;
			for (int e = d; e < n; ++e) {
				gradientA_RA = Views.offset(gradients[e], offset).randomAccess();
				gradientB_RA = Views.translate(gradients[e], offset).randomAccess();
				for(Entry<List<Long>, SheetnessInformation> entry : medialSurfaceCoordinatesToSheetnessInformationMap.entrySet()) {
					List<Long> pos = entry.getKey();
					SheetnessInformation sheetnessInformation = entry.getValue();
					long [] pos_array = new long[] {pos.get(0),pos.get(1),pos.get(2)};
					gradientA_RA.setPosition(pos_array);
					gradientB_RA.setPosition(pos_array);
					sheetnessInformation.b_minus_a_normalized[d][e] = (gradientB_RA.get().get() - gradientA_RA.get().get())*norms[e];			
					medialSurfaceCoordinatesToSheetnessInformationMap.put(pos,sheetnessInformation);
				}
			}
		}
		
		/*for (int d = 0; d < n; ++d) {
			for (int e = d; e < n; ++e) {
				a[d][e] = Views.interval(gradientsA[d][e], converted).randomAccess();
				b[d][e] = Views.interval(gradientsB[d][e], converted).randomAccess();
			}
		}*/
			
		
		//Create cursors
		//final Cursor<DoubleType> sourceCursor = Views.flatIterable(converted).cursor();
		//final Cursor<UnsignedByteType> medialSurfaceCursor = Views.flatIterable(Views.interval(medialSurface, converted)).cursor();

		//Create necessary info for calculating hessian
		final DMatrixRMaj hessian = new DMatrixRMaj(n, n);
		final SymmetricQRAlgorithmDecomposition_DDRM eigen = new SymmetricQRAlgorithmDecomposition_DDRM(false);//TODO: SWITCH TRUE TO FALSE IF WE DON'T NEED EIGENVECTORS!!!!!
		final double[] eigenvalues = new double[n];

		int newCount = 0;
		int updatedCount = 0;
		//Loop over source image
		long tic = System.currentTimeMillis();
		
		for ( Entry<List<Long>,SheetnessInformation>entry : medialSurfaceCoordinatesToSheetnessInformationMap.entrySet()) {
			/* TODO Is test if n == 1 and set to 1 meaningful? */
			//Increment cursors
			List<Long> currentMedialSurfaceCoordinate = entry.getKey();
			long [] currentMedialSurfaceCoordinateArray = new long[] {currentMedialSurfaceCoordinate.get(0),currentMedialSurfaceCoordinate.get(1),currentMedialSurfaceCoordinate.get(2)};
			//Increment cgradients and calculate hessian
			SheetnessInformation sheetnessInformation = entry.getValue();
			for (int d = 0; d < n; ++d) {
				for (int e = d; e < n; ++e) {
					//b[d][e].setPosition(currentMedialSurfaceCoordinate);
					//a[d][e].setPosition(currentMedialSurfaceCoordinate);
					final double hde = sheetnessInformation.b_minus_a_normalized[d][e];

//					final double hde = (b[d][e].get().getRealDouble() - a[d][e].get().getRealDouble()) * norms[e];
//					final double hde = (b[d][e].next().getRealDouble() - a[d][e].next().getRealDouble());
					hessian.set(d, e, hde);
					hessian.set(e, d, hde);
				}
			}
			
			//if(medialSurfaceVoxel.getRealDouble()>0 && isWithinOutputBlock(new long [] {sourceCursor.getLongPosition(0), sourceCursor.getLongPosition(1), sourceCursor.getLongPosition(2)}, padding, dimension)) {//Only need to evaluate if it is a medial surface and it is within the output block

				eigen.decompose(hessian);
				for (int d = 0; d < n; ++d)
					eigenvalues[d] = eigen.getEigenvalue(d).getReal();
				
				DoubleArrays.quickSort(eigenvalues, absDoubleComparator);
								 
				// Based on this paper http://www.cim.mcgill.ca/~shape/publications/miccai05b.pdf
				if(eigenvalues[2]>0) { //Only calculate if largest magnitude eigenvalue is negative
					continue;
				}
				
				//If laplacian at current voxel is the smallest it has been, then update sheetness and laplacian
				double laplacian = hessian.get(0,0)+ hessian.get(1,1) + hessian.get(2,2);
				
				if(laplacian<sheetnessInformation.minimumLaplacian) {
					if(sheetnessInformation.minimumLaplacian==0) {
						newCount++;
					}
					else {
						updatedCount++;
					}
					double Rsheet = Math.abs(eigenvalues[1]/eigenvalues[2]);
					double alpha = 0.5;
					double sheetEnhancementTerm = Math.exp(-Rsheet*Rsheet/(2*alpha*alpha));
					double Rblob = Math.abs(2*Math.abs(eigenvalues[2])-Math.abs(eigenvalues[1])-Math.abs(eigenvalues[0]))/Math.abs(eigenvalues[2]);
					double beta = 0.5;
					double blobEliminationTerm = 1-Math.exp(-Rblob*Rblob/(2*beta*beta));
					
					double equation1 = sheetEnhancementTerm*blobEliminationTerm;
					
					sheetnessInformation.minimumLaplacian = laplacian;
					sheetnessInformation.sheetness = equation1;
					medialSurfaceCoordinatesToSheetnessInformationMap.put(currentMedialSurfaceCoordinate, sheetnessInformation);
					
				}
			//}		
		}
	
		System.out.println("Time: " + (System.currentTimeMillis()-tic)/1000 + ". Scale step: " + scaleStep +". Num new: "+newCount + ", Num updated: "+updatedCount+", Total: "+(newCount+updatedCount));
	}
	
	static DoubleComparator absDoubleComparator = new DoubleComparator() {

		@Override
		public int compare(final double k1, final double k2) {

			final double absK1 = Math.abs(k1);
			final double absK2 = Math.abs(k2);

			return absK1 == absK2 ? 0 : absK1 < absK2 ? -1 : 1;
		}
	};
	
	
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
			finalOutputN5DatasetName = currentOrganelle;
			
			// Create block information list
			List<BlockInformation> blockInformationList = buildBlockInformationList(options.getInputN5Path(),
					currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
			computeCurvature(sc, options.getInputN5Path(), currentOrganelle, options.getOutputN5Path(), finalOutputN5DatasetName, blockInformationList);

			sc.close();
		}

	}
}

class SheetnessInformation implements Serializable{
	//use map to associate object ID with radii, edges etc
	public double[][] b_minus_a_normalized;
	public double minimumLaplacian;
	public double sheetness; 
	
	public SheetnessInformation() 
	{ 
		this.b_minus_a_normalized = new double[3][3];
		this.minimumLaplacian = 0;
		this.sheetness = 0;
	}
}
