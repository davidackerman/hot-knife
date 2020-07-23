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
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.array.ArrayRandomAccess;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.FloatType;
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
public class SparkFillHolesInConnectedComponents {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5DatasetSuffix", required = false, usage = "N5 suffix, e.g. _cc so output would be /mito_cc")
		private String outputN5DatasetSuffix = "";

		@Option(name = "--skipVolumeFilter", required = false, usage = "N5 suffix, e.g. _cc so output would be /mito_cc")
		private boolean skipVolumeFilter = false;
		
		@Option(name = "--skipCreatingHoleDataset", required = false, usage = "N5 suffix, e.g. _cc so output would be /mito_cc")
		private boolean skipCreatingHoleDataset = false;
		
		@Option(name = "--minimumVolumeCutoff", required = false, usage = "Volume above which objects will be kept")
		private float minimumVolumeCutoff = 20000000;

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

		public String getInputN5Path() {
			return inputN5Path;
		}

		public String getInputN5DatasetName() {
			return inputN5DatasetName;
		}

		public String getOutputN5DatasetSuffix() {
			return outputN5DatasetSuffix;
		}
		
		public boolean getSkipVolumeFilter() {
			return skipVolumeFilter;
		}
		
		public boolean getSkipCreatingHoleDataset() {
			return skipCreatingHoleDataset;
		}
		
		public float getMinimumVolumeCutoff() {
			return minimumVolumeCutoff;
		}
	

	}
	
	/*public static final <T extends NativeType<T>> void volumeFilterConnectedComponents(
			final JavaSparkContext sc, final String inputN5Path, final String inputN5DatasetName, final String outputN5DatasetName, float minimumVolumeCutoff,
			List<BlockInformation> blockInformationList) throws IOException {
				// Get attributes of input data set
				final N5Reader n5Reader = new N5FSReader(inputN5Path);
				final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
				final int[] blockSize = attributes.getBlockSize();
				final double[] pixelResolution = IOHelper.getResolution(n5Reader, inputN5DatasetName);
				final float minimumVolumeCutoffInVoxels = (float) (minimumVolumeCutoff/Math.pow(pixelResolution[0],3));
				// Create output dataset
				final N5Writer n5Writer = new N5FSWriter(inputN5Path);
				n5Writer.createGroup(outputN5DatasetName);
				n5Writer.createDataset(outputN5DatasetName, attributes.getDimensions(), blockSize,
						org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
				n5Writer.setAttribute(outputN5DatasetName, "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, inputN5DatasetName)));
				
				// Set up rdd to parallelize over blockInformation list and run RDD, which will
				// return updated block information containing list of components on the edge of
				// the corresponding block
				JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
				JavaRDD<HashMap<Long,Long>> objectIDtoVolumeMaps = rdd.map(currentBlockInformation -> {
					// Get information for reading in/writing current block
					long[][] gridBlock = currentBlockInformation.gridBlock;
					long[] offset = gridBlock[0];
					long[] dimension = gridBlock[1];
			
					// Read in source block
					final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
					final RandomAccessibleInterval<UnsignedLongType> objects = Views.offsetInterval(Views.extendZero((RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)),offset, dimension); 
					Cursor<UnsignedLongType> objectsCursor = Views.flatIterable(objects).cursor();
					
					HashMap<Long,Long> objectIDtoVolumeMap = new HashMap();
					while(objectsCursor.hasNext()) {
						UnsignedLongType voxel = objectsCursor.next();
						long objectID = voxel.get();
						objectIDtoVolumeMap.put(objectID, objectIDtoVolumeMap.getOrDefault(objectID,0L)+1);
					}
					return objectIDtoVolumeMap;
				});
			
				HashMap<Long,Long> finalObjectIDtoVolumeMap = objectIDtoVolumeMaps.reduce((a,b) -> {
					for(Long objectID : b.keySet()) {
						a.put(objectID, a.getOrDefault(objectID,0L)+b.get(objectID));
					}
					return a; 
					});

				//rewrite it
				rdd = sc.parallelize(blockInformationList);
				rdd.foreach(currentBlockInformation -> {
					// Get information for reading in/writing current block
					long[][] gridBlock = currentBlockInformation.gridBlock;
					long[] offset = gridBlock[0];
					long[] dimension = gridBlock[1];
			
					// Read in source block
					final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
					final RandomAccessibleInterval<UnsignedLongType> objects = Views.offsetInterval(Views.extendZero((RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)),offset, dimension); 
					Cursor<UnsignedLongType> objectsCursor = Views.flatIterable(objects).cursor();
					
					while(objectsCursor.hasNext()) {
						UnsignedLongType voxel = objectsCursor.next();
						long objectID = voxel.get();
						if(finalObjectIDtoVolumeMap.get(objectID) <= minimumVolumeCutoffInVoxels) {
							voxel.set(0);
						}
					}
					// Write out output to temporary n5 stack
					final N5Writer n5WriterLocal = new N5FSWriter(inputN5Path);
					N5Utils.saveBlock(objects, n5WriterLocal, outputN5DatasetName, gridBlock[2]);
				});
		
	}*/

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
	public static final <T extends NativeType<T>> MapsForFillingHoles getMapsForFillingHoles(
			final JavaSparkContext sc, final String inputN5Path, final String inputN5DatasetName,
			List<BlockInformation> blockInformationList) throws IOException {

		// Get attributes of input data set
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();

		// Set up rdd to parallelize over blockInformation list and run RDD, which will
		// return updated block information containing list of components on the edge of
		// the corresponding block
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<MapsForFillingHoles> javaRDDFillingHoleResults = rdd.map(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			long[] paddedOffset = new long [] {offset[0]-1, offset[1]-1, offset[2]-1};//add one extra for distance transform
			long[] paddedDimension = new long[] {dimension[0]+2, dimension[1]+2, dimension[2]+2};
			
			// Read in source block
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			long[] fullDimensions = new long [] {0,0,0};
			((RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)).dimensions(fullDimensions);
			long maxValue = fullDimensions[0]*fullDimensions[1]*fullDimensions[2]*10;//no object should have a value larger than this
			final RandomAccessibleInterval<UnsignedLongType> objects = Views.offsetInterval(Views.extendValue((RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName), new UnsignedLongType(maxValue)),paddedOffset, paddedDimension); 
			final RandomAccessibleInterval<UnsignedLongType> holes = Views.offsetInterval(Views.extendZero((RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName+"_holes")),paddedOffset, paddedDimension);
			
			final RandomAccessibleInterval<BoolType> objectsBinarized = Converters.convert(objects,
					(a, b) -> b.set(a.getRealDouble() > 0), new BoolType());



			
			//Get distance transform
			ArrayImg<FloatType, FloatArray> distanceFromObjects = ArrayImgs.floats(paddedDimension);	
			DistanceTransform.binaryTransform(objectsBinarized, distanceFromObjects, DISTANCE_TYPE.EUCLIDIAN);
	
			//Reassign black values
			ArrayCursor<FloatType> distanceFromObjectCursor = distanceFromObjects.cursor();
			RandomAccess<UnsignedLongType> holeComponentsRandomAccess = holes.randomAccess();
			RandomAccess<UnsignedLongType> objectsRandomAccess = objects.randomAccess();
			
		/*	if(offset[0]==0 && offset[1]==360 && offset[2] ==0) {
				new ImageJ();
				ImageJFunctions.show(objects,"objects");
				ImageJFunctions.show(holes,"holes");
				ImageJFunctions.show(distanceFromObjects,"distance");
			}*/
			
			Map<Long,Long> holeIDtoObjectIDMap = new HashMap();
			Map<Long,Long> objectIDtoVolumeMap = new HashMap();
			Map<Long,Long> holeIDtoVolumeMap = new HashMap();
			
			while(distanceFromObjectCursor.hasNext()) {
				float distanceFromObjectSquared = distanceFromObjectCursor.next().get();
				int pos [] = new int[] {distanceFromObjectCursor.getIntPosition(0), distanceFromObjectCursor.getIntPosition(1), distanceFromObjectCursor.getIntPosition(2)};
				//objectsRandomAccess.setPosition(pos);
				//long objectID = objectsRandomAccess.get().get();
				/*if (objectID != maxValue && objectID>0) {
					objectIDtoVolumeMap.put(objectID, objectIDtoVolumeMap.getOrDefault(objectID, 0L) + 1);		
				}*/
				
				if (distanceFromObjectSquared>0 && distanceFromObjectSquared <= 3) { //3 for corners. If true, then is on edge of hole
					if(pos[0]>0 && pos[0]<=dimension[0] && pos[1]>0 && pos[1]<=dimension[1] && pos[2]>0 && pos[2]<=dimension[2]) {//Then in original block
						holeComponentsRandomAccess.setPosition(pos);
						long holeID = holeComponentsRandomAccess.get().get();
						//holeIDtoVolumeMap.put(holeID, holeIDtoVolumeMap.getOrDefault(holeID, 0L) + 1);	

						for(int dx=-1; dx<=1; dx++) {
							for(int dy=-1; dy<=1; dy++) {
								for(int dz=-1; dz<=1; dz++) {
									if((dx==0 && dy==0 && dz!=0) || (dx==0 && dz==0 && dy!=0) || (dy==0 && dz==0 && dx!=0)) {//diamond checking
										int newPos [] = new int[] {pos[0]+dx, pos[1]+dy, pos[2]+dz};
										objectsRandomAccess.setPosition(newPos);
										long objectID = objectsRandomAccess.get().get();
										if(objectID>0) {//can still be outside
											if ( objectID == maxValue || (holeIDtoObjectIDMap.containsKey(holeID) && objectID != holeIDtoObjectIDMap.get(holeID)) ) //is touching outside or then has already been assigned to an object and is not really a hole since it is touching multiple objects
												holeIDtoObjectIDMap.put(holeID,0L);
											else 
												holeIDtoObjectIDMap.put(holeID,objectID);										
										}
									}
								}
							}
						}
						
					}
				}
			}

			return new MapsForFillingHoles(objectIDtoVolumeMap, holeIDtoVolumeMap, holeIDtoObjectIDMap);
		});

		MapsForFillingHoles mapsForFillingHoles = javaRDDFillingHoleResults.reduce((a,b) -> {
			a.merge(b);
			return a;
		});
		
		//mapsForFillingHoles.filterObjectsByVolume(1000);
		
		/*
		for(Entry entry: mapsForFillingHoles.objectIDtoVolumeMap.entrySet())
			System.out.println("obj id, vol: "+entry.getKey()+" "+entry.getValue());
		
		for(Entry entry: mapsForFillingHoles.holeIDtoVolumeMap.entrySet())
			System.out.println("hole id, vol: "+entry.getKey()+" "+entry.getValue());
		
		for(Entry entry: mapsForFillingHoles.holeIDtoObjectIDMap.entrySet())
			System.out.println("id, id: "+entry.getKey()+" "+entry.getValue());
		
		mapsForFillingHoles.filterObjectsByVolume(1000);
		
		System.out.println(mapsForFillingHoles.objectIDsBelowVolumeFilter);
		
		for(Entry entry: mapsForFillingHoles.holeIDtoObjectIDMap.entrySet())
			System.out.println("id, id: "+entry.getKey()+" "+entry.getValue());
		*/
		
		return mapsForFillingHoles;
	}
	
	public static final <T extends NativeType<T>> void fillHoles(
			final JavaSparkContext sc, final String inputN5Path, final String inputN5DatasetName, final String outputN5DatasetName, MapsForFillingHoles mapsForFillingHoles,
			List<BlockInformation> blockInformationList) throws IOException {
				// Get attributes of input data set
				final N5Reader n5Reader = new N5FSReader(inputN5Path);
				final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
				final int[] blockSize = attributes.getBlockSize();

				// Create output dataset
				final N5Writer n5Writer = new N5FSWriter(inputN5Path);
				n5Writer.createGroup(outputN5DatasetName);
				n5Writer.createDataset(outputN5DatasetName, attributes.getDimensions(), blockSize,
						org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
				n5Writer.setAttribute(outputN5DatasetName, "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, inputN5DatasetName)));

				
				// Set up rdd to parallelize over blockInformation list and run RDD, which will
				// return updated block information containing list of components on the edge of
				// the corresponding block
				final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
				rdd.foreach(currentBlockInformation -> {
					// Get information for reading in/writing current block
					long[][] gridBlock = currentBlockInformation.gridBlock;
					long[] offset = gridBlock[0];
					long[] dimension = gridBlock[1];
			
					// Read in source block
					final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
					final RandomAccessibleInterval<UnsignedLongType> objects = Views.offsetInterval(Views.extendZero((RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)),offset, dimension); 
					final RandomAccessibleInterval<UnsignedLongType> holes = Views.offsetInterval(Views.extendZero((RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName+"_holes")),offset, dimension);
					ArrayImg<UnsignedLongType, LongArray> output = ArrayImgs.unsignedLongs(dimension);	

					ArrayCursor<UnsignedLongType> outputCursor = output.cursor();
					RandomAccess<UnsignedLongType> objectsRandomAccess = objects.randomAccess();
					RandomAccess<UnsignedLongType> holesRandomAccess = holes.randomAccess();
					while(outputCursor.hasNext()) {
						UnsignedLongType voxel = outputCursor.next();
						long pos [] = new long[] {outputCursor.getLongPosition(0), outputCursor.getLongPosition(1), outputCursor.getLongPosition(2)};
						holesRandomAccess.setPosition(pos);
						objectsRandomAccess.setPosition(pos);
						
						long holeID = holesRandomAccess.get().get();
						long objectID = objectsRandomAccess.get().get();
						
						
						long setValue = objectID;	
						/*if( objectID >0 ) {
							if(! mapsForFillingHoles.objectIDsBelowVolumeFilter.contains(objectID))
								setValue = objectID;
						}*/
						 
						if( holeID > 0) {
							setValue = mapsForFillingHoles.holeIDtoObjectIDMap.get(holeID);
						}
						
						voxel.set(setValue);
					}


					// Write out output to temporary n5 stack
					final N5Writer n5WriterLocal = new N5FSWriter(inputN5Path);
					N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);
					
					//Get distance transform
				});
			
		
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

		final SparkConf conf = new SparkConf().setAppName("SparkFillHolesInConnectedComponents");

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

		String datasetToHoleFill = null;
		String tempOutputN5DatasetName = null;
		String finalOutputN5DatasetName = null;
		List<String> directoriesToDelete = new ArrayList<String>();
		for (String currentOrganelle : organelles) {
			logMemory(currentOrganelle);

			
			// Create block information list
			List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			datasetToHoleFill = currentOrganelle;
			if(!options.getSkipVolumeFilter()) {
				String tempVolumeFilteredDatasetName = currentOrganelle + "_volumeFilteredTemp"+options.getOutputN5DatasetSuffix();
				SparkVolumeFilterConnectedComponents.volumeFilterConnectedComponents(sc, options.getInputN5Path(), currentOrganelle, tempVolumeFilteredDatasetName, options.getMinimumVolumeCutoff(), blockInformationList);
				directoriesToDelete.add(options.getInputN5Path() + "/" + tempVolumeFilteredDatasetName);
				datasetToHoleFill = tempVolumeFilteredDatasetName;
			}
			
			tempOutputN5DatasetName = datasetToHoleFill + "_holes" + "_blockwise_temp_to_delete";
			finalOutputN5DatasetName = datasetToHoleFill + "_holes";
			directoriesToDelete.add(options.getInputN5Path() + "/" + tempOutputN5DatasetName);
			directoriesToDelete.add(options.getInputN5Path() + "/" + finalOutputN5DatasetName);
			
			if(!options.getSkipCreatingHoleDataset()) {
				// Get connected components of holes in *_holes
				int minimumVolumeCutoff = 0;
				blockInformationList = SparkConnectedComponents.blockwiseConnectedComponents(sc, options.getInputN5Path(),
						datasetToHoleFill, options.getInputN5Path(), tempOutputN5DatasetName, null, 1, minimumVolumeCutoff,
						blockInformationList, true, false);
				logMemory("Stage 1 complete");
	
				blockInformationList = SparkConnectedComponents.unionFindConnectedComponents(sc, options.getInputN5Path(),
						tempOutputN5DatasetName, minimumVolumeCutoff, blockInformationList);
				logMemory("Stage 2 complete");
	
				SparkConnectedComponents.mergeConnectedComponents(sc, options.getInputN5Path(), tempOutputN5DatasetName,
						finalOutputN5DatasetName, blockInformationList);
				logMemory("Stage 3 complete");
			}
			

			MapsForFillingHoles mapsForFillingHoles = getMapsForFillingHoles(sc,  options.getInputN5Path(), datasetToHoleFill, blockInformationList);
			fillHoles(sc, options.getInputN5Path(), datasetToHoleFill, currentOrganelle+options.getOutputN5DatasetSuffix(), mapsForFillingHoles, blockInformationList);
			
			sc.close();
		}

		// Remove temporary files
		SparkDirectoryDelete.deleteDirectories(conf, directoriesToDelete);
		logMemory("Stage 4 complete");

	}
}

class MapsForFillingHoles implements Serializable{
	public Map<Long,Long> objectIDtoVolumeMap;
	public Map<Long,Long> holeIDtoVolumeMap;
	public Map<Long,Long> holeIDtoObjectIDMap;
	public Set<Long> objectIDsBelowVolumeFilter;

	public MapsForFillingHoles(Map<Long,Long> objectIDtoVolumeMap, Map<Long,Long> holeIDtoVolumeMap, Map<Long,Long> holeIDtoObjectIDMap){
		this.objectIDtoVolumeMap = objectIDtoVolumeMap;
		this.holeIDtoVolumeMap = holeIDtoVolumeMap;
		this.holeIDtoObjectIDMap = holeIDtoObjectIDMap;
		this.objectIDsBelowVolumeFilter = new HashSet();
	}
	
	public void merge(MapsForFillingHoles newMapsForFillingHoles) {
		//merge holeIDtoObjectIDMap
		for(Entry<Long,Long> entry : newMapsForFillingHoles.holeIDtoObjectIDMap.entrySet()) {
			long holeID = entry.getKey();
			long objectID = entry.getValue();
			if(	holeIDtoObjectIDMap.containsKey(holeID) && holeIDtoObjectIDMap.get(holeID)!=objectID) 
				holeIDtoObjectIDMap.put(holeID, 0L);
			else 
				holeIDtoObjectIDMap.put(holeID, objectID);
		}
		
		//merge holeIDtoVolumeMap
		for(Entry<Long,Long> entry : newMapsForFillingHoles.holeIDtoVolumeMap.entrySet())
			holeIDtoVolumeMap.put(entry.getKey(), holeIDtoVolumeMap.getOrDefault(entry.getKey(), 0L) + entry.getValue() );
		
		//merge objectIDtoVolumeMap
		for(Entry<Long,Long> entry : newMapsForFillingHoles.objectIDtoVolumeMap.entrySet())
			objectIDtoVolumeMap.put(entry.getKey(), objectIDtoVolumeMap.getOrDefault(entry.getKey(), 0L) + entry.getValue() );
	
	}
	
	public void fillHolesForVolume() {
		for(Entry<Long,Long> entry : holeIDtoObjectIDMap.entrySet()) {
			long holeID = entry.getKey();
			long objectID = entry.getValue();
			if(objectID != 0 ) {
				long holeVolume = holeIDtoVolumeMap.get(holeID);
				objectIDtoVolumeMap.put(objectID, objectIDtoVolumeMap.getOrDefault(objectID, 0L) + holeVolume );
			}
		}		
	}
	
	public void filterObjectsByVolume(int minimumVolumeCutoff) {
		fillHolesForVolume();
		for(Entry<Long,Long> entry : objectIDtoVolumeMap.entrySet()) {
			long objectID = entry.getKey();
			long volume = entry.getValue();
			if(volume<=minimumVolumeCutoff) 
				objectIDsBelowVolumeFilter.add(objectID);
		}
		for(Entry<Long,Long> entry : holeIDtoObjectIDMap.entrySet()) {
			long holeID = entry.getKey();
			long objectID = entry.getValue();
			if ( objectIDsBelowVolumeFilter.contains(objectID) ) //then surrounding object is too small
				holeIDtoObjectIDMap.put(holeID, 0L);
		}
		
	}
	
}
