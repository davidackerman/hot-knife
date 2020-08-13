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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
		
		public String getN5PathRefinedPredictions() {
			return n5PathRefinedPredictions;
		}

		public String getOutputPath() {
			return outputPath;
		}

	}

	@SuppressWarnings("unchecked")
	public static final <T extends NumericType<T>> void createValidationDatasets(
			final JavaSparkContext sc,
			final String n5PathTrainingData,
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
		//organelleToIDs.put("microtubules",Arrays.asList(30, 36));
		organelleToIDs.put("ribosomes",Arrays.asList(34));
		 
		final N5Reader n5ReaderTraining = new N5FSReader(n5PathTrainingData);
		
		String n5OutputTraining = outputPath+"/training.n5";
		String n5OutputRefinedPredictions = outputPath+"/refinedPredictions.n5";

		final N5Writer n5WriterTraining = new N5FSWriter(n5OutputTraining);
		final N5Writer n5WriterRefinedPredictions = new N5FSWriter(n5OutputRefinedPredictions);
		
		for( Entry<String, List<Integer>> entry : organelleToIDs.entrySet()) {
			final String organelle = entry.getKey();
			List<Integer> ids = entry.getValue();
			
			String organellePathInN5 = organelle=="ribosomes" ? "/"+organelle : "/all";
		
			DatasetAttributes attributesTraining = n5ReaderTraining.getDatasetAttributes(organellePathInN5);
			long[] dimensionsTraining = attributesTraining.getDimensions();
			int[] blockSizeTraining = attributesTraining.getBlockSize();
			
			double[] pixelResolutionTraining = IOHelper.getResolution(n5ReaderTraining, organellePathInN5);
			int[] offsetTraining = IOHelper.getOffset(n5ReaderTraining, organellePathInN5);
			
			final String organelleRibosomeAdjustedName = organelle=="ribosomes"? "ribosomes_centers":organelle;
			
			n5WriterTraining.createDataset(
					organelleRibosomeAdjustedName,
					dimensionsTraining,
					blockSizeTraining,
					DataType.UINT8,
					new GzipCompression());
			n5WriterTraining.setAttribute(organelleRibosomeAdjustedName, "offset", offsetTraining);
			n5WriterTraining.setAttribute(organelleRibosomeAdjustedName, "pixelResolution",new IOHelper.PixelResolution(pixelResolutionTraining));
	
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
				
				N5Utils.saveBlock(sourceConverted, n5BlockWriter, organelleRibosomeAdjustedName, blockInformation.gridBlock[2]);
						
			});
		
			
			
			final N5Reader n5ReaderRefinedPredictions = new N5FSReader(n5PathRefinedPredictions);		
			double[] pixelResolutionRefined = IOHelper.getResolution(n5ReaderRefinedPredictions, organelleRibosomeAdjustedName);
			double resolutionRatio = pixelResolutionTraining[0]/pixelResolutionRefined[0];
			long [] dimensionsRefinedPredictions = new long [] {(long) (dimensionsTraining[0]*resolutionRatio),(long) (dimensionsTraining[1]*resolutionRatio),(long) (dimensionsTraining[2]*resolutionRatio)};
			long [] offsetInVoxels = new long [] {(long) (offsetTraining[0]/pixelResolutionRefined[0]),(long) (offsetTraining[1]/pixelResolutionRefined[1]),(long) (offsetTraining[2]/pixelResolutionRefined[2])};
			int[] blockSizeRefinedPredictions = blockSizeTraining;
			//refined predictions
			n5WriterRefinedPredictions.createDataset(
					organelleRibosomeAdjustedName,
					dimensionsRefinedPredictions,
					blockSizeTraining,
					DataType.UINT8,
					new GzipCompression());
			n5WriterRefinedPredictions.setAttribute(organelleRibosomeAdjustedName, "offset", offsetTraining);
			n5WriterRefinedPredictions.setAttribute(organelleRibosomeAdjustedName, "pixelResolution", new IOHelper.PixelResolution(pixelResolutionRefined));
	
			List<BlockInformation> blockInformationListRefinedPredictions = BlockInformation.buildBlockInformationList(dimensionsRefinedPredictions, blockSizeRefinedPredictions);
			rdd = sc.parallelize(blockInformationListRefinedPredictions);			
			rdd.foreach(blockInformation -> {
				final long [] offset= new long [] {blockInformation.gridBlock[0][0]+offsetInVoxels[0],
						blockInformation.gridBlock[0][1]+offsetInVoxels[1],
						blockInformation.gridBlock[0][2]+offsetInVoxels[2]
				};
				final long [] dimension = blockInformation.gridBlock[1];
				final N5Reader n5ReaderLocal = new N5FSReader(n5PathRefinedPredictions);
			
				final RandomAccessibleInterval<UnsignedLongType> source = Views.offsetInterval((RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5ReaderLocal, organelleRibosomeAdjustedName), offset, dimension);
				final RandomAccessibleInterval<UnsignedByteType> sourceConverted = Converters.convert(
						source,
						(a, b) -> {
							b.set(a.get()>0 ? 255 : 0 );
						},
						new UnsignedByteType());
				final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputRefinedPredictions);
				N5Utils.saveBlock(sourceConverted, n5BlockWriter, organelleRibosomeAdjustedName, blockInformation.gridBlock[2]);
								
			});
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
		createValidationDatasets(sc, options.getN5PathTrainingData(), options.getN5PathRefinedPredictions(), options.getOutputPath());
		sc.close();
		
				
	}
}
