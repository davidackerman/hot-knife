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
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.view.Views;

/**
 * Calculate sheetness at contact sites. Outputs file with histogram of sheetness vs surface area at contact sites based on sheetness in inputN5SheetnessDatasetName
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkCalculateSheetnessOfContactSites {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "Input N5 path")
		private String inputN5Path = null;

		@Option(name = "--outputDirectory", required = false, usage = "Output N5 path")
		private String outputDirectory = null;

		@Option(name = "--inputN5SheetnessDatasetName", required = false, usage = "Volume averaged sheetness N5 dataset")
		private String inputN5SheetnessDatasetName = null;
		
		@Option(name = "--inputN5ContactSiteDatasetName", required = false, usage = "Contact site N5 dataset")
		private String inputN5ContactSiteDatasetName = null;
		

		public Options(final String[] args) {
			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);
				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				parser.printUsage(System.err);
			}
		}
		
		public String getInputN5Path() {
			return inputN5Path;
		}

		public String getInputN5SheetnessDatasetName() {
			return inputN5SheetnessDatasetName;
		}
		
		public String getInputN5ContactSiteDatasetName() {
			return inputN5ContactSiteDatasetName;
		}
		
		public String getOutputDirectory() {
			if(outputDirectory == null) {
				outputDirectory = inputN5Path.split(".n5")[0]+"_results";
			}
			return outputDirectory;
		}
		
		

	}
	
	/**
	 * Calculates histograms of the sheetness of the desired contact site surface voxels.
	 * 
	 * @param sc									Spark context
	 * @param n5Path								Input N5 path
	 * @param volumeAveragedSheetnessDatasetName	Dataset name for volume averaged sheetness
	 * @param contactSiteName						Dataset name corresponding to desired contact sites
	 * @param blockInformationList					Block information list
	 * @return										Map of histogram data
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final Map<Integer,Double> getContactSiteSheetness(
			final JavaSparkContext sc,
			final String n5Path,
			final String volumeAveragedSheetnessDatasetName,
			final String contactSiteName,
			final List<BlockInformation> blockInformationList) throws IOException {

		//Set up reader and get information about dataset
		final N5Reader n5Reader = new N5FSReader(n5Path);
		double [] pixelResolution = IOHelper.getResolution(n5Reader, volumeAveragedSheetnessDatasetName);
		double voxelFaceArea = pixelResolution[0]*pixelResolution[1];

		//Acquire histograms in a blockwise manner
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<Map<Integer,Double>> javaRDDHistogramMap = rdd.map(blockInformation -> {
			final long [][] gridBlock = blockInformation.gridBlock;
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			long[] paddedOffset = new long[] {offset[0]-1,offset[1]-1,offset[2]-1};
			long[] paddedDimension = new long[] {dimension[0]+2,dimension[1]+2,dimension[2]+2};

			//Set up random access for datasets
			RandomAccessibleInterval<UnsignedByteType> volumeAveragedSheetness = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(n5BlockReader, volumeAveragedSheetnessDatasetName)
					),paddedOffset, paddedDimension);
			RandomAccessibleInterval<UnsignedLongType> contactSites = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5BlockReader, contactSiteName)
					),paddedOffset, paddedDimension);

			RandomAccess<UnsignedByteType> volumeAveragedSheetnessRA = volumeAveragedSheetness.randomAccess();
			RandomAccess<UnsignedLongType> contactSitesRA = contactSites.randomAccess();			
			
			//Build histogram
			Map<Integer,Double> sheetnessAndSurfaceAreaHistogram = buildSheetnessAndSurfaceAreaHistogram(paddedDimension, volumeAveragedSheetnessRA, contactSitesRA, voxelFaceArea);
			return sheetnessAndSurfaceAreaHistogram;
		});
		
		//Collect histograms
		Map<Integer, Double> sheetnessAndSurfaceAreaHistogram = javaRDDHistogramMap.reduce((a,b) -> {
			 for(Entry<Integer,Double> entry : b.entrySet())
					a.put(entry.getKey(), a.getOrDefault(entry.getKey(), 0.0) + entry.getValue() );
			return a;
		});
		
		return sheetnessAndSurfaceAreaHistogram;
	}
	
	/**
	 * Loops over voxels to build up a histogram of the sheetness of the volume averaged sheetness at contact sites
	 * 
	 * @param paddedDimension			Padded dimensions of block
	 * @param volumeAveragedSheetnessRA Random access for volume averaged sheetness dataset
	 * @param contactSitesRA			Random access for contact site dataset
	 * @param voxelFaceArea				Surface area of voxel face
	 * @return							Map containing the histogram data
	 */
	public static Map<Integer,Double> buildSheetnessAndSurfaceAreaHistogram(long[] paddedDimension, RandomAccess<UnsignedByteType> volumeAveragedSheetnessRA, RandomAccess<UnsignedLongType> contactSitesRA, double voxelFaceArea){
		
		Map<Integer,Double> sheetnessAndSurfaceAreaHistogram = new HashMap<Integer,Double>();
		for(long x=1; x<paddedDimension[0]-1;x++) {
			for(long y=1; y<paddedDimension[1]-1;y++) {
				for(long z=1; z<paddedDimension[2]-1;z++) {
					long [] pos = new long[]{x,y,z};
					volumeAveragedSheetnessRA.setPosition(pos);
					contactSitesRA.setPosition(pos);
					int sheetnessMeasureBin = volumeAveragedSheetnessRA.get().get();
	
					if(sheetnessMeasureBin>0 && contactSitesRA.get().get()>0) {//Then is on surface and contact site
						int faces = SparkCosemHelper.getSurfaceAreaContributionOfVoxelInFaces(volumeAveragedSheetnessRA);
						if(faces>0) {
							sheetnessAndSurfaceAreaHistogram.put(sheetnessMeasureBin, sheetnessAndSurfaceAreaHistogram.getOrDefault(sheetnessMeasureBin,0.0)+faces*voxelFaceArea);
						}
					}
				}
			}
		}
		
		return sheetnessAndSurfaceAreaHistogram;
	}
	
	/**
	 * 
	 * @param sheetnessAndSurfaceAreaHistogram	Histogram of sheetness vs surface area as map
	 * @param outputDirectory					Directory to write results to
	 * @param filePrefix						Prefix for file name
	 * @throws IOException
	 */
	public static void writeData( Map<Integer, Double> sheetnessAndSurfaceAreaHistogram, String outputDirectory, String filePrefix) throws IOException {
		if (! new File(outputDirectory).exists()){
			new File(outputDirectory).mkdirs();
	    }
		
		FileWriter sheetnessVolumeAndAreaHistogramFW = new FileWriter(outputDirectory+"/"+filePrefix+"_sheetnessSurfaceAreaHistograms.csv");
		sheetnessVolumeAndAreaHistogramFW.append("Sheetness,Surface Area(nm^2)\n");
				
		for(int sheetnessBin=0;sheetnessBin<256;sheetnessBin++) {
			double surfaceArea = sheetnessAndSurfaceAreaHistogram.getOrDefault(sheetnessBin, 0.0);
			String sheetnessBinString = Double.toString(sheetnessBin/256.0+0.5/256.0);
			sheetnessVolumeAndAreaHistogramFW.append(sheetnessBinString+","+Double.toString(surfaceArea)+"\n");
		}
		sheetnessVolumeAndAreaHistogramFW.flush();
		sheetnessVolumeAndAreaHistogramFW.close();
	
	}
	
	/**
	 * Take input args and perform analysis.
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
		
		// Get all organelles
		String[] organelles = { "" };
		if (options.getInputN5ContactSiteDatasetName() != null) {
			organelles = options.getInputN5ContactSiteDatasetName().split(",");
		} else {
			File file = new File(options.getInputN5Path());
			organelles = file.list(new FilenameFilter() {
				@Override
				public boolean accept(File current, String name) {
					return new File(current, name).isDirectory();
				}
			});
		}

		final SparkConf conf = new SparkConf().setAppName("SparkCalculateSheetnessOfContactSites");
		
		//Create block information list
		List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(),
			options.getInputN5SheetnessDatasetName());
		for(String organelle : organelles) {
			JavaSparkContext sc = new JavaSparkContext(conf);
			Map<Integer, Double> sheetnessAndSurfaceAreaHistogram = getContactSiteSheetness(sc, options.getInputN5Path(), options.getInputN5SheetnessDatasetName(), organelle, blockInformationList);
			writeData(sheetnessAndSurfaceAreaHistogram, options.getOutputDirectory(),  organelle);
			sc.close();
		}

	}
	
}

