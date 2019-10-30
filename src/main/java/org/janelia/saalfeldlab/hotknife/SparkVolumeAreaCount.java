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
public class SparkVolumeAreaCount {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputDirectory", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputDirectory = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;

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
			final JavaSparkContext sc, final String inputN5Path, final String inputN5DatasetName, final String outputDirectory,
			List<BlockInformation> blockInformationList) throws IOException {
		
		
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final long[] outputDimensions = attributes.getDimensions();
		final long outOfBoundsValue = outputDimensions[0]*outputDimensions[1]*outputDimensions[2]+1;
		// Set up rdd to parallelize over blockInformation list and run RDD, which will
		// return updated block information containing list of components on the edge of
		// the corresponding block
		// Set up reader to get n5 attributes
				
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<VolumeAreaCountClass> javaRDDvolumeAreaCount  = rdd.map(currentBlockInformation -> {
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
			
			Set<Long> allObjectIDs = new HashSet<>();
			Set<Long> edgeObjectIDs = new HashSet<>();
			Map<Long, Long> objectIDtoVolume = new HashMap<>();
			Map<Long, Long> objectIDtoSurfaceArea = new HashMap<>();

			for(long x=1; x<=dimension[0]; x++) {
				for(long y=1; y<=dimension[1]; y++) {
					for(long z=1; z<=dimension[2]; z++) {
						sourceRandomAccess.setPosition(new long[] {x,y,z});
						long currentVoxelValue=sourceRandomAccess.get().get();
						
						if (currentVoxelValue >0  && currentVoxelValue != outOfBoundsValue ) {
							incrementCountInMap(objectIDtoVolume, currentVoxelValue);

							allObjectIDs.add(currentVoxelValue);			
							if((x==1 || x==dimension[0]) || (y==1 || y==dimension[1]) || (z==1 || z==dimension[2]) ) {
								edgeObjectIDs.add(currentVoxelValue);
							}
						
							if(isSurfaceVoxel(sourceRandomAccess, outOfBoundsValue)){
								incrementCountInMap(objectIDtoSurfaceArea, currentVoxelValue);
							}
						}
					}
				}
			}
			
			final Set<Long> selfContainedObjectIDs = Sets.difference(allObjectIDs, edgeObjectIDs);
			final Map<Long,Long> volumeHistogramAsMap = new HashMap<>();
			final Map<Long,Long> surfaceHistogramAsMap = new HashMap<>();
			final Map<Float,Long> surfaceAreaToVolumeRatioHistogramAsMap = new HashMap<>();
			for(Long objectID : selfContainedObjectIDs) {
				final long currentObjectVolume = objectIDtoVolume.get(objectID);
				incrementCountInMap(volumeHistogramAsMap, currentObjectVolume);
				
				final long currentObjectSurfaceArea = objectIDtoSurfaceArea.get(objectID);
				incrementCountInMap(surfaceHistogramAsMap, currentObjectSurfaceArea);
				
				final float currentObjectSurfaceAreaToVolumeRatio = Math.round(1000.0F*currentObjectSurfaceArea/currentObjectVolume)/1000.0F;
				incrementCountInMap(surfaceAreaToVolumeRatioHistogramAsMap, currentObjectSurfaceAreaToVolumeRatio);
			}
			
			
			final Map<Long,Long> edgeObjectIDtoVolume =  edgeObjectIDs.stream()
			        .filter(objectIDtoVolume::containsKey)
			        .collect(Collectors.toMap(Function.identity(), objectIDtoVolume::get));		
			
			final Map<Long,Long> edgeObjectIDtoSurfaceArea =  edgeObjectIDs.stream()
			        .filter(objectIDtoSurfaceArea::containsKey)
			        .collect(Collectors.toMap(Function.identity(), objectIDtoSurfaceArea::get));		
			
			VolumeAreaCountClass volumeAreaCount = new VolumeAreaCountClass(volumeHistogramAsMap, surfaceHistogramAsMap, surfaceAreaToVolumeRatioHistogramAsMap, edgeObjectIDtoVolume, edgeObjectIDtoSurfaceArea);
			
			return volumeAreaCount;
		});
		
		VolumeAreaCountClass collectedVolumeAreaCount = javaRDDvolumeAreaCount.reduce((a,b) -> {
				combineCountsFromTwoMaps(a.edgeObjectIDtoVolume, b.edgeObjectIDtoVolume);
				combineCountsFromTwoMaps(a.edgeObjectIDtoSurfaceArea, b.edgeObjectIDtoSurfaceArea);
				combineCountsFromTwoMaps(a.volumeHistogramAsMap, b.volumeHistogramAsMap);
				combineCountsFromTwoMaps(a.surfaceAreaHistogramAsMap, b.surfaceAreaHistogramAsMap);
				combineCountsFromTwoMaps(a.surfaceAreaToVolumeRatioHistogramAsMap, b.surfaceAreaToVolumeRatioHistogramAsMap);
				return a;
			});
		
		addEdgeObjectValuesToHistogramMap(collectedVolumeAreaCount.edgeObjectIDtoVolume, collectedVolumeAreaCount.volumeHistogramAsMap); 
		addEdgeObjectValuesToHistogramMap(collectedVolumeAreaCount.edgeObjectIDtoSurfaceArea, collectedVolumeAreaCount.surfaceAreaHistogramAsMap); 
		for(Long currentObjectID : collectedVolumeAreaCount.edgeObjectIDtoSurfaceArea.keySet()) {
			float currentObjectSurfaceAreaToVolumeRatio = Math.round(1000.0F*collectedVolumeAreaCount.edgeObjectIDtoSurfaceArea.get(currentObjectID)/collectedVolumeAreaCount.edgeObjectIDtoVolume.get(currentObjectID))/1000.0F;
			incrementCountInMap(collectedVolumeAreaCount.surfaceAreaToVolumeRatioHistogramAsMap, currentObjectSurfaceAreaToVolumeRatio);
		}
		collectedVolumeAreaCount.volumeHistogramAsMap = new TreeMap<Long, Long>(collectedVolumeAreaCount.volumeHistogramAsMap);
		collectedVolumeAreaCount.surfaceAreaHistogramAsMap = new TreeMap<Long, Long>(collectedVolumeAreaCount.surfaceAreaHistogramAsMap);
		collectedVolumeAreaCount.surfaceAreaToVolumeRatioHistogramAsMap = new TreeMap<Float, Long>(collectedVolumeAreaCount.surfaceAreaToVolumeRatioHistogramAsMap);
		
		collectedVolumeAreaCount.totalObjectCounts = 0;
		for (Entry <Long,Long> entry : collectedVolumeAreaCount.volumeHistogramAsMap.entrySet()) {
			collectedVolumeAreaCount.totalObjectCounts+=entry.getValue();
		}
		System.out.println("Number of objects: " + collectedVolumeAreaCount.totalObjectCounts);
		writeData(collectedVolumeAreaCount, outputDirectory, inputN5DatasetName);
	}
	
	public static <T> void incrementCountInMap(final Map<T,Long> map, T key) {
		map.put(key, map.getOrDefault(key, 0L)+1);
	}

	public static <T> void combineCountsFromTwoMaps(Map<T,Long> a, Map<T,Long> b){
		for( T key : b.keySet()) {
			a.put(key, a.getOrDefault(key,0L)+b.get(key));
		}	
	}
	
	public static<T> void addEdgeObjectValuesToHistogramMap(Map<Long,Long> edgeIDtoValue, Map<Long,Long> valueHistogram) {
		for( Entry<Long, Long> idAndValue : edgeIDtoValue.entrySet()) {
			long currentValue = idAndValue.getValue();
			incrementCountInMap(valueHistogram, currentValue);
		}
	}
	
	
	public static boolean isSurfaceVoxel(final RandomAccess<UnsignedLongType> sourceRandomAccess, long outOfBoundsValue ) {
		long referenceVoxelValue = sourceRandomAccess.get().get();
		final long sourceRandomAccessPosition[] = {sourceRandomAccess.getLongPosition(0), sourceRandomAccess.getLongPosition(1), sourceRandomAccess.getLongPosition(2)};		
		for(int x=-1; x<=1; x++) {
			for(int y=-1; y<=1; y++) {
				for(int z=-1; z<=1; z++) {
					if(!(x==0 && y==0 && z==0)) {
						final long currentPosition[] = {sourceRandomAccessPosition[0]+x, sourceRandomAccessPosition[1]+y, sourceRandomAccessPosition[2]+z};
						sourceRandomAccess.setPosition(currentPosition);
						if(sourceRandomAccess.get().get() != referenceVoxelValue && sourceRandomAccess.get().get() !=outOfBoundsValue) {
							return true;
						}
					}
				}
			}
		}

		return false;	
	
	}
	
	public static void writeData(VolumeAreaCountClass vac, String outputDirectory, String organelle) throws IOException {
		if (! new File(outputDirectory).exists()){
			new File(outputDirectory).mkdirs();
	    }
		FileWriter csvWriter = new FileWriter(outputDirectory+"/"+organelle+".csv");
		List<String> v = convertMapToStringList(vac.volumeHistogramAsMap);
		List<String> s = convertMapToStringList(vac.surfaceAreaHistogramAsMap);
		List<String> svr = convertMapToStringList(vac.surfaceAreaToVolumeRatioHistogramAsMap);
		final long lines = Math.max(Math.max(v.size(), s.size()), svr.size());
		csvWriter.append("Volume Bin,Volume Count,Surface Area Bin,Surface Area Count,Surface Area To Volume Ratio Bin,Surface Area To Volume Ratio Count,Total Objects\n");
		for(int i=0; i<lines; i++) {
			List<String> outputString = new ArrayList<>();
			outputString = addToString(outputString, v,i);
			outputString = addToString(outputString, s, i);
			outputString = addToString(outputString, svr, i);
			outputString.add(i==0 ? Long.toString(vac.totalObjectCounts): "");
			String csvLine = String.join(",", outputString)+"\n";
			csvWriter.append(csvLine);
		}
		csvWriter.flush();
		csvWriter.close();
		
		csvWriter = new FileWriter(outputDirectory+"/allCounts.csv", true);
		if (! new File(outputDirectory+"/allCounts.csv").exists()) {
			csvWriter.append("Object,Count\n");
		}
		csvWriter.append(organelle+","+vac.totalObjectCounts+"\n");			
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

		final SparkConf conf = new SparkConf().setAppName("SparkVolumeAreaCount");

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
			JavaSparkContext sc = new JavaSparkContext(conf);
			List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), currentOrganelle);
			calculateVolumeAreaCount(sc, options.getInputN5Path(), currentOrganelle, options.getOutputDirectory(), blockInformationList);
			sc.close();
		}
	}
}
