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
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.common.collect.Sets;

import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.view.Views;


/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkGeneralCosemObjectInformation {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputDirectory", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputDirectory = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;
		
		@Option(name = "--skipContactSites", required = false, usage = "Get general information for contact sites")
		private boolean skipContactSites = false;

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

		public String getInputN5DatasetName() {
			return inputN5DatasetName;
		}
		
		public String getOutputDirectory() {
			if(outputDirectory == null) {
				outputDirectory = inputN5Path.split(".n5")[0]+"_results";
			}
			return outputDirectory;
		}
		
		public boolean getSkipContactSites() {
			return skipContactSites;
		}
		
	}

	/**
	 * Find connected components on a block-by-block basis and write out to
	 * temporary n5.
	 *
	 * Takes as input a threshold intensity, above which voxels are used for
	 * calculating connected components. Parallelization is done using a
	 * blockInformationList.
	 *
	 * @param sc
	 * @param inputN5Path
	 * @param inputN5DatasetName
	 * @param outputN5Path
	 * @param outputN5DatasetName
	 * @param maskN5PathName
	 * @param thresholdIntensity
	 * @param blockInformationList
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> void calculateVolumeAreaCount(
			final JavaSparkContext sc, final String inputN5Path, final String[] datasetNames, final String outputDirectory,
			List<BlockInformation> blockInformationList) throws IOException {
		
		final String inputN5DatasetName, organelle1N5Dataset, organelle2N5Dataset;
		if(datasetNames.length==1) {
			organelle1N5Dataset=null;
			organelle2N5Dataset=null;
			inputN5DatasetName = datasetNames[0];
		}
		else {
			organelle1N5Dataset = datasetNames[0]+"_contact_boundary_temp_to_delete";
			organelle2N5Dataset = datasetNames[1]+"_contact_boundary_temp_to_delete";
			inputN5DatasetName = datasetNames[2];
		}
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final long[] outputDimensions = attributes.getDimensions();
		final long outOfBoundsValue = outputDimensions[0]*outputDimensions[1]*outputDimensions[2]+1;
		double [] pixelResolution = IOHelper.getResolution(n5Reader, inputN5DatasetName);

		// Set up rdd to parallelize over blockInformation list and run RDD, which will
		// return updated block information containing list of components on the edge of
		// the corresponding block
		// Set up reader to get n5 attributes
				
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<Map<Long,long[]>> javaRDDvolumeAreaCount  = rdd.map(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] extendedOffset = gridBlock[0];
			long[] dimension = gridBlock[1].clone(), extendedDimension = gridBlock[1].clone();
			
			//extend by 1 on each edge
			Arrays.setAll(extendedOffset, i->extendedOffset[i]-1);
			Arrays.setAll(extendedDimension, i->extendedDimension[i]+2);
			
			// Read in source block
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);	
			final RandomAccessibleInterval<UnsignedLongType> sourceInterval = Views.offsetInterval(Views.extendValue(
					(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName),new UnsignedLongType(outOfBoundsValue)), extendedOffset, extendedDimension);
			final RandomAccess<UnsignedLongType> sourceRandomAccess = sourceInterval.randomAccess();
			
			RandomAccess<UnsignedLongType> organelle1RandomAccess = null,organelle2RandomAccess=null;
			if(datasetNames.length>1) {
				organelle1RandomAccess = Views.offsetInterval(Views.extendValue(
						(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, organelle1N5Dataset),new UnsignedLongType(outOfBoundsValue)), extendedOffset, extendedDimension).randomAccess();
				organelle2RandomAccess = Views.offsetInterval(Views.extendValue(
						(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, organelle2N5Dataset),new UnsignedLongType(outOfBoundsValue)), extendedOffset, extendedDimension).randomAccess();
			}
			
			Map<Long, long[]> objectIDtoInformationMap = new HashMap<>(); //Volume, Surface Area, com xyz, min xyz, max xyz
			
			//For surface area
			List<long[]> voxelsToCheck = new ArrayList(); 
			voxelsToCheck.add(new long[] {-1, 0, 0});
			voxelsToCheck.add(new long[] {1, 0, 0});
			voxelsToCheck.add(new long[] {0, -1, 0});
			voxelsToCheck.add(new long[] {0, 1, 0});
			voxelsToCheck.add(new long[] {0, 0, -1});
			voxelsToCheck.add(new long[] {0, 0, 1});
			
			for(long x=1; x<=dimension[0]; x++) {
				for(long y=1; y<=dimension[1]; y++) {
					for(long z=1; z<=dimension[2]; z++) {
						sourceRandomAccess.setPosition(new long[] {x,y,z});
						long currentVoxelValue=sourceRandomAccess.get().get();
						
						if (currentVoxelValue >0  && currentVoxelValue != outOfBoundsValue ) {
							
							int surfaceAreaContributionOfVoxelInFaces = getSurfaceAreaContributionOfVoxelInFaces(sourceRandomAccess, outOfBoundsValue, voxelsToCheck);
							
							long[] absolutePosition = {x+extendedOffset[0],y+extendedOffset[1],z+extendedOffset[2]};
							long[] organelleIDs = {-1, -1};
							if(datasetNames.length>1) {
								organelle1RandomAccess.setPosition(new long[] {x,y,z});
								organelle2RandomAccess.setPosition(new long[] {x,y,z});
								organelleIDs[0] = organelle1RandomAccess.get().get();
								organelleIDs[1] = organelle2RandomAccess.get().get();
							}
							addNewVoxelToObjectInformation(objectIDtoInformationMap, currentVoxelValue, absolutePosition, surfaceAreaContributionOfVoxelInFaces, organelleIDs);
						}
					}
				}
			}
						
			return objectIDtoInformationMap;
		});
		
		Map<Long, long[]> collectedObjectInformation = javaRDDvolumeAreaCount.reduce((a,b) -> {
				combineObjectInformationMaps(a,b);
				return a;
			});
		
		System.out.println("Total objects: "+collectedObjectInformation.size());
		writeData(collectedObjectInformation, outputDirectory, datasetNames, pixelResolution[0]);//Assuming it is isotropic
	}
	
	public static void addNewVoxelToObjectInformation(Map<Long,long[]> objectIDtoInformationMap, long objectID, long[] position, long surfaceAreaContributionOfVoxelInFaces, long[] organelleIDs) {
		if(!objectIDtoInformationMap.containsKey(objectID)) {
			objectIDtoInformationMap.put(objectID, new long[]{1,surfaceAreaContributionOfVoxelInFaces,position[0],position[1],position[2],position[0],position[1],position[2],position[0],position[1],position[2], organelleIDs[0], organelleIDs[1]});
		}
		else {
			long[] objectInformation = objectIDtoInformationMap.get(objectID);
			
			objectInformation[0]+=1; //Volume
			
			objectInformation[1]+=surfaceAreaContributionOfVoxelInFaces;
			
			for(int i=0; i<3; i++) {
				objectInformation[2+i]+=position[i]; //COM (will divide by volume at end)
				objectInformation[5+i] = Math.min(objectInformation[5+i], position[i]); //xyz min
				objectInformation[8+i] = Math.max(objectInformation[8+i], position[i]); //xyz max
			}
			 objectInformation[11] = organelleIDs[0];
			 objectInformation[12] = organelleIDs[1];
			
		}
	}
	
	public static Map<Long,long[]> combineObjectInformationMaps(Map<Long,long[]> objectInformationMapA, Map<Long,long[]> objectInformationMapB) {
		for(long objectID : objectInformationMapB.keySet() ) {
			if(objectInformationMapA.containsKey(objectID)) {
				long[] objectInformationA = objectInformationMapA.get(objectID);
				long[] objectInformationB = objectInformationMapB.get(objectID);
				
				for(int i=0; i<2; i++) {
					objectInformationA[i]+=objectInformationB[i]; //Volume, surface area
				}
				for(int i=0; i<3; i++) {
					objectInformationA[2+i]+= objectInformationB[2+i]; //com xyz
					objectInformationA[5+i] = Math.min(objectInformationA[5+i], objectInformationB[5+i]); //min xyz
					objectInformationA[8+i] = Math.max(objectInformationA[8+i], objectInformationB[8+i]); //max xyz
				}
				objectInformationMapA.put(objectID, objectInformationA);
			}
			else {
				objectInformationMapA.put(objectID, objectInformationMapB.get(objectID));
			}
		}
		return objectInformationMapA;
	}
	
	
	
	
	public static int getSurfaceAreaContributionOfVoxelInFaces(final RandomAccess<UnsignedLongType> sourceRandomAccess, long outOfBoundsValue, List<long[]> voxelsToCheck) {
		long referenceVoxelValue = sourceRandomAccess.get().get();
		final long sourceRandomAccessPosition[] = {sourceRandomAccess.getLongPosition(0), sourceRandomAccess.getLongPosition(1), sourceRandomAccess.getLongPosition(2)};
		int surfaceAreaContributionOfVoxelInFaces = 0;


		for(long[] currentVoxel : voxelsToCheck) {
			final long currentPosition[] = {sourceRandomAccessPosition[0]+currentVoxel[0], sourceRandomAccessPosition[1]+currentVoxel[1], sourceRandomAccessPosition[2]+currentVoxel[2]};
			sourceRandomAccess.setPosition(currentPosition);
			if(sourceRandomAccess.get().get() != referenceVoxelValue && sourceRandomAccess.get().get() !=outOfBoundsValue) {
				surfaceAreaContributionOfVoxelInFaces ++;
			}
		}

		return surfaceAreaContributionOfVoxelInFaces;	
	
	}
	
	public static void writeData(Map<Long,long[]> collectedObjectInformation, String outputDirectory, String [] datasetNames, double pixelDimension) throws IOException {
		if (! new File(outputDirectory).exists()){
			new File(outputDirectory).mkdirs();
	    }
		
		String outputFile, organelle1=null, organelle2=null;
		if(datasetNames.length == 1) {
			outputFile = datasetNames[0];
		}
		else {
			organelle1 = datasetNames[0];
			organelle2 = datasetNames[1];
			outputFile = datasetNames[2];
		}
		FileWriter csvWriter = new FileWriter(outputDirectory+"/"+outputFile+".csv");
		if(datasetNames.length == 1) {
			csvWriter.append("Object ID,Volume (nm^3),Surface Area (nm^2),COM X (nm),COM Y (nm),COM Z (nm),MIN X (nm),MIN Y (nm),MIN Z (nm),MAX X (nm),MAX Y (nm),MAX Z (nm),,Total Objects\n");
		}
		else {
			csvWriter.append("Object ID,Volume (nm^3),Surface Area (nm^2),COM X (nm),COM Y (nm),COM Z (nm),MIN X (nm),MIN Y (nm),MIN Z (nm),MAX X (nm),MAX Y (nm),MAX Z (nm),"+organelle1+" ID,"+organelle2+" ID,,Total Objects\n");
		}
		boolean firstLine = true;
		for(Entry<Long,long[]> objectIDandInformation: collectedObjectInformation.entrySet()) {
			String outputString = Long.toString(objectIDandInformation.getKey());
			long [] objectInformation = objectIDandInformation.getValue();
			outputString+=","+Double.toString(objectInformation[0]*Math.pow(pixelDimension,3)); // volume
			outputString+=","+Double.toString(objectInformation[1]*Math.pow(pixelDimension,2)); //surface area
			outputString+=","+Double.toString(pixelDimension*objectInformation[2]/objectInformation[0]); //com x
			outputString+=","+Double.toString(pixelDimension*objectInformation[3]/objectInformation[0]); //com y
			outputString+=","+Double.toString(pixelDimension*objectInformation[4]/objectInformation[0]); //com z
			for(int i=5;i<11;i++) {
				outputString+=","+Double.toString(objectInformation[i]*pixelDimension);// min and max xyz
			}
			if(datasetNames.length>1) {
				outputString+=","+Long.toString(objectInformation[11])+","+Long.toString(objectInformation[12]);//organelle ids
			}
			if(firstLine) {
				outputString+=",,"+collectedObjectInformation.size()+"\n";
				firstLine = false;
			}
			else {
				outputString+=",\n";
			}
			csvWriter.append(outputString);
		}
		csvWriter.flush();
		csvWriter.close();
		
		boolean firstLineInAllCountsFile = false;
		if (! new File(outputDirectory+"/allCounts.csv").exists()) {
			firstLineInAllCountsFile = true;
		}
		csvWriter = new FileWriter(outputDirectory+"/allCounts.csv", true);
		if(firstLineInAllCountsFile) csvWriter.append("Object,Count\n");

		csvWriter.append(outputFile+","+collectedObjectInformation.size()+"\n");			
		csvWriter.flush();
		csvWriter.close();
	}
	
	public static <T> List<String> convertMapToStringList(Map<T,Long> map){
		List<String> s = new ArrayList<String>();
		for(Entry<T,Long> e : map.entrySet()) {
			s.add(e.getKey()+","+e.getValue());
		}
		return s;
	}
	
	public static List<String> addToString(List<String> outputString, List<String> s,int index) {
		outputString.add( index<s.size() ? s.get(index) : ",");		
		return outputString;
	}
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkGeneralCosemInformation");

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

		new File(options.getOutputDirectory()+"/allCounts.csv").delete();
		
		System.out.println(Arrays.toString(organelles));
		for (String currentOrganelle : organelles) {
			System.out.println(currentOrganelle);
			String [] datasetNames = {currentOrganelle};
			JavaSparkContext sc = new JavaSparkContext(conf);
			List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), datasetNames[0]);
			calculateVolumeAreaCount(sc, options.getInputN5Path(), datasetNames, options.getOutputDirectory(), blockInformationList);
			sc.close();
		}
		
		if (!options.getSkipContactSites()) {
			for (int i=0; i<organelles.length; i++) {
				for(int j=i; j<organelles.length;j++) {
					String [] datasetNames = {organelles[i],i==j ? organelles[j] : organelles[j]+"_pairs",organelles[i]+"_to_"+organelles[j]+"_cc"};
					System.out.println(Arrays.toString(datasetNames));
					
					JavaSparkContext sc = new JavaSparkContext(conf);
					List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), datasetNames[2]);
					calculateVolumeAreaCount(sc, options.getInputN5Path(), datasetNames, options.getOutputDirectory(), blockInformationList);
					sc.close();
				}
			}
		}
	}
}
