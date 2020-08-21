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

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.view.Views;

/**
 * Expand a given dataset to mask out predictions for improving downstream analysis.
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkCreateValidationDatasets {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--n5PathTrainingData", required = true, usage = "Dataset to mask N5 path")
		private String n5PathTrainingData = null;
		
		@Option(name = "--n5PathRawPredictions", required = true, usage = "Dataset to use as mask N5 path")
		private String n5PathRawPredictions = null;
		
		@Option(name = "--n5PathRefinedPredictions", required = true, usage = "Dataset to use as mask N5 path")
		private String n5PathRefinedPredictions = null;

		@Option(name = "--outputPath", required = true, usage = "Output N5 path")
		private String outputPath = null;


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

		public String getN5PathTrainingData() {
			return n5PathTrainingData;
		}
		
		public String getN5PathRawPredictions() {
			return n5PathRawPredictions;
		}
		
		public String getN5PathRefinedPredictions() {
			return n5PathRefinedPredictions;
		}

		public String getOutputPath() {
			return outputPath;
		}

	}

	@SuppressWarnings("unchecked")
	public static final <T extends IntegerType<T>> void createValidationDatasets(
			final JavaSparkContext sc,
			final String n5PathTrainingData,
			final String n5PathRawPredictions,
			final String n5PathRefinedPredictions,
			final String outputPath) throws IOException {

		Map<String, List<Integer>> organelleToIDs = new HashMap<String,List<Integer>>();
		organelleToIDs.put("plasma_membrane",Arrays.asList(2));
		organelleToIDs.put("mito_membrane",Arrays.asList(3));
		organelleToIDs.put("mito",Arrays.asList(3,4,5));
		organelleToIDs.put("golgi",Arrays.asList(6,7));
		organelleToIDs.put("vesicle", Arrays.asList(8, 9));
		organelleToIDs.put("MVB", Arrays.asList(10, 11));
		organelleToIDs.put("er",Arrays.asList(16, 17, 18, 19, 20, 21, 22, 23));
		organelleToIDs.put("nucleus",Arrays.asList(20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 37));
		organelleToIDs.put("microtubules_out",Arrays.asList(30));
		organelleToIDs.put("microtubules",Arrays.asList(30, 36));
		organelleToIDs.put("ribosomes",Arrays.asList(34));
		 
		final N5Reader n5ReaderTraining = new N5FSReader(n5PathTrainingData);
		
		String n5OutputTraining = outputPath+"/training.n5";
		final N5Writer n5WriterTraining = new N5FSWriter(n5OutputTraining);
		
		String n5OutputRefinedPredictions = outputPath+"/refinedPredictions.n5";
		final N5Writer n5WriterRefinedPredictions = new N5FSWriter(n5OutputRefinedPredictions);
		
		String n5OutputRawPredictions = outputPath+"/rawPredictions.n5";
		final N5Writer n5WriterRawPredictions = new N5FSWriter(n5OutputRawPredictions);
		double[] pixelResolutionTraining = null;
		int[] blockSizeTraining = null;
		long[] dimensionsTraining = null;
		int[] offsetTraining =null;
		for( Entry<String, List<Integer>> entry : organelleToIDs.entrySet()) {
			final String organelle = entry.getKey();
			List<Integer> ids = entry.getValue();
			
			String organellePathInN5 = organelle=="ribosomes" ? "/"+organelle : "/all";
		
			DatasetAttributes attributesTraining = n5ReaderTraining.getDatasetAttributes(organellePathInN5);
			dimensionsTraining = attributesTraining.getDimensions();
			blockSizeTraining = attributesTraining.getBlockSize();
			
			pixelResolutionTraining = IOHelper.getResolution(n5ReaderTraining, organellePathInN5);
			offsetTraining = IOHelper.getOffset(n5ReaderTraining, organellePathInN5);
			
			final String organelleRibosomeAdjustedNameTraining = organelle=="ribosomes"? "ribosomes_centers":organelle;
			
			n5WriterTraining.createDataset(
					organelleRibosomeAdjustedNameTraining,
					dimensionsTraining,
					blockSizeTraining,
					DataType.UINT8,
					new GzipCompression());
			n5WriterTraining.setAttribute(organelleRibosomeAdjustedNameTraining, "offset", offsetTraining);
			n5WriterTraining.setAttribute(organelleRibosomeAdjustedNameTraining, "pixelResolution",new IOHelper.PixelResolution(pixelResolutionTraining));
	
			List<BlockInformation> blockInformationListTraining = BlockInformation.buildBlockInformationList(n5PathTrainingData,organellePathInN5);
			JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationListTraining);
			rdd.foreach(blockInformation -> {
				final long [] offset= blockInformation.gridBlock[0];
				final long [] dimension = blockInformation.gridBlock[1];
				final N5Reader n5ReaderLocal = new N5FSReader(n5PathTrainingData);
				
				final RandomAccessibleInterval<UnsignedLongType> source = Views.offsetInterval((RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5ReaderLocal, organellePathInN5), offset, dimension);
				final RandomAccessibleInterval<UnsignedByteType> sourceConverted = Converters.convert(
						source,
						(a, b) -> {
							Integer id = (int) a.get();
							b.set(ids.contains(id) ? 255 : 0 );
						},
						new UnsignedByteType());
				final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputTraining);
				
				N5Utils.saveBlock(sourceConverted, n5BlockWriter, organelleRibosomeAdjustedNameTraining, blockInformation.gridBlock[2]);
						
			});
		}
		Set<String> organelleSet = new HashSet<String>();
		organelleSet.addAll(organelleToIDs.keySet());
		//refined and raw predictions
		for(String n5PredictionsPath : Arrays.asList(n5PathRawPredictions, n5PathRefinedPredictions)) {
			organelleSet.remove("microtubules_out");
			organelleSet.remove("microtubules");
			if(n5PredictionsPath==n5PathRefinedPredictions) {
				organelleSet.add("microtubules");
				organelleSet.add("er_reconstructed");
				organelleSet.add("er_maskedWith_nucleus_expanded");
				organelleSet.add("mito_maskedWith_er");
				organelleSet.add("er_maskedWith_nucleus_expanded_maskedWith_ribosomes");
				organelleSet.add("er_reconstructed_maskedWith_nucleus_expanded");
				organelleSet.add("mito_maskedWith_er_reconstructed");
				organelleSet.add("er_reconstructed_maskedWith_nucleus_expanded_maskedWith_ribosomes");
			}
			for(String organelle : organelleSet) {

				final String organelleRibosomeAdjustedName = (organelle=="ribosomes" && n5PredictionsPath==n5PathRefinedPredictions)? "ribosomes_centers":organelle;

				final N5Reader n5ReaderPredictions = new N5FSReader(n5PredictionsPath);		
				double[] pixelResolutionPredictions = IOHelper.getResolution(n5ReaderPredictions, organelleRibosomeAdjustedName);
				double resolutionRatio = pixelResolutionTraining[0]/pixelResolutionPredictions[0];
				long [] dimensionsPredictions = new long [] {(long) (dimensionsTraining[0]*resolutionRatio),(long) (dimensionsTraining[1]*resolutionRatio),(long) (dimensionsTraining[2]*resolutionRatio)};
				long [] offsetInVoxels = new long [] {(long) (offsetTraining[0]/pixelResolutionPredictions[0]),(long) (offsetTraining[1]/pixelResolutionPredictions[1]),(long) (offsetTraining[2]/pixelResolutionPredictions[2])};
				int[] blockSizeRefinedPredictions = blockSizeTraining;
				N5Writer n5WriterPredictions = n5PredictionsPath==n5PathRawPredictions ? n5WriterRawPredictions : n5WriterRefinedPredictions;
				n5WriterPredictions.createDataset(
						organelleRibosomeAdjustedName,
						dimensionsPredictions,
						blockSizeTraining,
						DataType.UINT8,
						new GzipCompression());
				n5WriterPredictions.setAttribute(organelleRibosomeAdjustedName, "offset", offsetTraining);
				n5WriterPredictions.setAttribute(organelleRibosomeAdjustedName, "pixelResolution", new IOHelper.PixelResolution(pixelResolutionPredictions));
		
				List<BlockInformation> blockInformationListRefinedPredictions = BlockInformation.buildBlockInformationList(dimensionsPredictions, blockSizeRefinedPredictions);
				JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationListRefinedPredictions);			
				rdd.foreach(blockInformation -> {
					final long [] offset= new long [] {blockInformation.gridBlock[0][0]+offsetInVoxels[0],
							blockInformation.gridBlock[0][1]+offsetInVoxels[1],
							blockInformation.gridBlock[0][2]+offsetInVoxels[2]
					};
					final long [] dimension = blockInformation.gridBlock[1];
					final N5Reader n5ReaderLocal = new N5FSReader(n5PredictionsPath);
					final long threshold = n5PredictionsPath==n5PathRawPredictions ? 127 : 1 ;
					final RandomAccessibleInterval<T> source = Views.offsetInterval((RandomAccessibleInterval<T>)N5Utils.open(n5ReaderLocal, organelleRibosomeAdjustedName), offset, dimension);
					final RandomAccessibleInterval<UnsignedByteType> sourceConverted = Converters.convert(
							source,
							(a, b) -> {
								b.set(a.getIntegerLong()>=threshold ? 255 : 0 );
							},
							new UnsignedByteType());
					final N5FSWriter n5BlockWriter = new N5FSWriter(n5PredictionsPath == n5PathRawPredictions ? n5OutputRawPredictions : n5OutputRefinedPredictions);
					N5Utils.saveBlock(sourceConverted, n5BlockWriter, organelleRibosomeAdjustedName, blockInformation.gridBlock[2]);
									
				});
			}
		}
	}
	
	/**
	 * Perform connected components on mask - if it is not already a segmented dataset - and use expanded version of segmented dataset as mask for prediction dataset.
	 * 
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;
		
		SparkConf conf = new SparkConf().setAppName("SparkCreateValidationDatasets");
		JavaSparkContext sc = new JavaSparkContext(conf);
		createValidationDatasets(sc, options.getN5PathTrainingData(), options.getN5PathRawPredictions(),options.getN5PathRefinedPredictions(), options.getOutputPath());
		sc.close();
		
				
	}
}
