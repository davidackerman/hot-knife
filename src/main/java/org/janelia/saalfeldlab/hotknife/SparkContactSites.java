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
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ij.ImageJ;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.algorithm.neighborhood.RectangleNeighborhood;
import net.imglib2.algorithm.neighborhood.RectangleShape;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * Get inter- and intra-contact sites between object types, using a predetermined cutoff distance.
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkContactSites {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /path/to/segmented/data.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /path/to/output/data.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. organelle")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5DatasetSuffix", required = false, usage = "N5 suffix, e.g. _cc so output would be organelle1_to_organelle2_cc")
		private String outputN5DatasetSuffix = "";

		@Option(name = "--contactDistance", required = false, usage = "Distance from orgnelle for contact site (nm)")
		private double contactDistance = 10;
		
		@Option(name = "--minimumVolumeCutoff", required = false, usage = "Minimum contact site cutoff (nm^3)")
		private double minimumVolumeCutoff = 35E3;
		
		@Option(name = "--doSelfContacts", required = false, usage = "Minimum contact site cutoff (nm^3)")
		private boolean doSelfContacts = false;
		
		@Option(name = "--skipGeneratingContactBoundaries", required = false, usage = "Minimum contact site cutoff (nm^3)")
		private boolean skipGeneratingContactBoundaries = false;

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

		public double getContactDistance() {
			return contactDistance;
		}
		
		public double getMinimumVolumeCutoff() {
			return minimumVolumeCutoff;
		}
		
		public boolean getDoSelfContacts() {
			return doSelfContacts;
		}

		public boolean getSkipGeneratingContactBoundaries() {
			return skipGeneratingContactBoundaries;
		}
	}
	
	/**
	 * Calculate contact boundaries around objects
	 *
	 * For a given segmented dataset, writes out an n5 file which contains halos around organelles within the contact distance. Each is labeled with the corresponding object ID from the segmentation.
	 *
	 * @param sc					Spark context
	 * @param inputN5Path			Input N5 path
	 * @param organelle				Organelle around which halo is calculated
	 * @param outputN5Path			Output N5 path
	 * @param contactDistance		Contact distance (in nm) 
	 * @param doSelfContactSites	Whether or not to do self contacts
	 * @param blockInformationList	List of block information
	 * @throws IOException
	 */
	public static final void calculateObjectContactBoundaries(
			final JavaSparkContext sc, final String inputN5Path, final String organelle,
			final String outputN5Path,
			final double contactDistance, final boolean doSelfContactSites, List<BlockInformation> blockInformationList) throws IOException {
		
			if(doSelfContactSites) calculateObjectContactBoundariesWithSelfContacts(sc, inputN5Path, organelle, outputN5Path, contactDistance, blockInformationList);	
			else calculateObjectContactBoundaries(sc, inputN5Path, organelle, outputN5Path, contactDistance, blockInformationList);
	}
	
	/**
	 * Calculate contact boundaries around objects when not doing self-contacts.
	 *
	 * For a given segmented dataset, writes out an n5 file which contains halos around organelles within the contact distance. Each is labeled with the corresponding object ID from the segmentation.
	 *
	 * @param sc					Spark context
	 * @param inputN5Path			Input N5 path
	 * @param organelle				Organelle around which halo is calculated
	 * @param outputN5Path			Output N5 path
	 * @param contactDistance		Contact distance (in nm) 
	 * @param blockInformationList	List of block information
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final void calculateObjectContactBoundaries(
			final JavaSparkContext sc, final String inputN5Path, final String organelle,
			final String outputN5Path,
			final double contactDistance, List<BlockInformation> blockInformationList) throws IOException {
		// Get attributes of input data set
				final N5Reader n5Reader = new N5FSReader(inputN5Path);
				final DatasetAttributes attributes = n5Reader.getDatasetAttributes(organelle);
				final int[] blockSize = attributes.getBlockSize();
				final long[] outputDimensions = attributes.getDimensions();

				// Create output dataset
				String outputN5DatasetNameOrganelle = organelle+"_contact_boundary_temp_to_delete";
				final N5Writer n5Writer = new N5FSWriter(outputN5Path);
				n5Writer.createGroup(outputN5DatasetNameOrganelle);
				n5Writer.createDataset(outputN5DatasetNameOrganelle, outputDimensions, blockSize,
						org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
				double [] pixelResolution = IOHelper.getResolution(n5Reader, organelle);
				n5Writer.setAttribute(outputN5DatasetNameOrganelle, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
				
				//Get contact distance in voxels
				final double contactDistanceInVoxels = contactDistance/pixelResolution[0]+1;//add 1 extra because are going to include surface voxels of other organelle so need an extra distance of 1 voxel
				double contactDistanceInVoxelsSquared = contactDistanceInVoxels*contactDistanceInVoxels;//contactDistanceInVoxelsCeiling*contactDistanceInVoxelsCeiling;
				int contactDistanceInVoxelsCeiling=(int)Math.ceil(contactDistanceInVoxels);
				
				final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
				rdd.foreach(currentBlockInformation -> {
					// Get information for reading in/writing current block. Need to extend offset/dimension so that can accomodate objects from different blocks
					long[][] gridBlock = currentBlockInformation.gridBlock;
					long[] dimension = gridBlock[1];
					long[] extendedOffset = gridBlock[0].clone();
					long[] extendedDimension = gridBlock[1].clone();
					Arrays.setAll(extendedOffset, i->extendedOffset[i]-contactDistanceInVoxelsCeiling);
					Arrays.setAll(extendedDimension, i->extendedDimension[i]+2*contactDistanceInVoxelsCeiling);

					// Read in source block
					final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);			
					final RandomAccessibleInterval<UnsignedLongType> segmentedOrganelle = Views.offsetInterval(Views.extendZero(
									(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, organelle)),
							extendedOffset, extendedDimension);
			
					
					//Get distance transform
					final RandomAccessibleInterval<BoolType> objectsBinarized = Converters.convert(segmentedOrganelle,
							(a, b) -> b.set(a.getLong() > 0), new BoolType());
					ArrayImg<FloatType, FloatArray> distanceFromObjects = ArrayImgs.floats(extendedDimension);	
					DistanceTransform.binaryTransform(objectsBinarized, distanceFromObjects, DISTANCE_TYPE.EUCLIDIAN);
					
					// Access input/output 
					final RandomAccess<UnsignedLongType> segmentedOrganelleRandomAccess = segmentedOrganelle.randomAccess();		
					final Img<UnsignedLongType> extendedOutput =  new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType()).create(extendedDimension);	

					final Cursor<UnsignedLongType> extendedOutputCursor = Views.flatIterable(extendedOutput).cursor();
					final Cursor<FloatType> distanceFromObjectsCursor = distanceFromObjects.cursor();
					// Loop over image and label voxel within halo according to object ID. Note: This will mean that each voxel only gets one object assignment
					while (extendedOutputCursor.hasNext()) {
						final UnsignedLongType voxelOutput = extendedOutputCursor.next();
						final float distanceFromObjectsSquared = distanceFromObjectsCursor.next().get();
						long[] position = {extendedOutputCursor.getLongPosition(0),  extendedOutputCursor.getLongPosition(1),  extendedOutputCursor.getLongPosition(2)};
						if( (distanceFromObjectsSquared>0 && distanceFromObjectsSquared<=contactDistanceInVoxelsSquared) || isSurfaceVoxel(segmentedOrganelleRandomAccess, position)) {//then voxel is within distance
							findAndSetValueToNearestOrganelleID(voxelOutput, distanceFromObjectsSquared, position, segmentedOrganelleRandomAccess);
						}
					}
					
					RandomAccessibleInterval<UnsignedLongType> output = Views.offsetInterval(extendedOutput,new long[] {contactDistanceInVoxelsCeiling,contactDistanceInVoxelsCeiling,contactDistanceInVoxelsCeiling},dimension);
					final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
					N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetNameOrganelle, gridBlock[2]);
					
				});
	}
	
	/**
	 * Calculate contact boundaries around objects when doing self-contacts. In this case, need to check if a given contact boundary for one organelle overlaps with another organelle's contact boundary.
	 *
	 * For a given segmented dataset, writes out an n5 file which contains halos around organelles within the contact distance. Each is labeled with the corresponding object ID from the segmentation.
	 *
	 * @param sc					Spark context
	 * @param inputN5Path			Input N5 path
	 * @param organelle				Organelle around which halo is calculated
	 * @param outputN5Path			Output N5 path
	 * @param contactDistance		Contact distance (in nm) 
	 * @param blockInformationList	List of block information
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> void calculateObjectContactBoundariesWithSelfContacts(
			final JavaSparkContext sc, final String inputN5Path, final String organelle,
			final String outputN5Path,
			final double contactDistance, List<BlockInformation> blockInformationList) throws IOException {
				
		// Get attributes of input data set
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(organelle);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();

		// Create output dataset
		String outputN5DatasetNameOrganelle = organelle+"_contact_boundary_temp_to_delete";
		String outputN5DatasetNameOrganellePairs = organelle+"_pairs_contact_boundary_temp_to_delete";

		final N5Writer n5Writer = new N5FSWriter(outputN5Path);
		n5Writer.createGroup(outputN5DatasetNameOrganelle);
		n5Writer.createDataset(outputN5DatasetNameOrganelle, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
		double [] pixelResolution = IOHelper.getResolution(n5Reader, organelle);
		n5Writer.setAttribute(outputN5DatasetNameOrganelle, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
		
		//Pairs necessary for classA - classA contact sites
		n5Writer.createGroup(outputN5DatasetNameOrganellePairs);
		n5Writer.createDataset(outputN5DatasetNameOrganellePairs, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
		n5Writer.setAttribute(outputN5DatasetNameOrganellePairs, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
		
		//Get contact distance in voxels
		final double contactDistanceInVoxels = contactDistance/pixelResolution[0]+1;//cuz will use surface voxels
		double contactDistanceInVoxelsSquared = contactDistanceInVoxels*contactDistanceInVoxels;//contactDistanceInVoxelsCeiling*contactDistanceInVoxelsCeiling;
		int contactDistanceInVoxelsCeiling=(int)Math.ceil(contactDistanceInVoxels);
		
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(currentBlockInformation -> {
			// Get information for reading in/writing current block. Need to extend offset/dimension so that can accomodate objects from different blocks
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] dimension = gridBlock[1];
			long[] extendedOffset = gridBlock[0].clone();
			long[] extendedDimension = gridBlock[1].clone();
			Arrays.setAll(extendedOffset, i->extendedOffset[i]-contactDistanceInVoxelsCeiling);
			Arrays.setAll(extendedDimension, i->extendedDimension[i]+2*contactDistanceInVoxelsCeiling);

			// Read in source block
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);			
			final RandomAccessibleInterval<UnsignedLongType> segmentedOrganelle = Views.offsetInterval(Views.extendZero(
							(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, organelle)),
					extendedOffset, extendedDimension);
			
			//Get distance transform
			final RandomAccessibleInterval<BoolType> objectsBinarized = Converters.convert(segmentedOrganelle,
					(a, b) -> b.set(a.getLong() > 0), new BoolType());
			ArrayImg<FloatType, FloatArray> distanceFromObjects = ArrayImgs.floats(extendedDimension);	
			DistanceTransform.binaryTransform(objectsBinarized, distanceFromObjects, DISTANCE_TYPE.EUCLIDIAN);
			
			// Access input/output 
			final RandomAccess<UnsignedLongType> segmentedOrganelleRandomAccess = segmentedOrganelle.randomAccess();		
			final Img<UnsignedLongType> extendedOutput =  new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType()).create(extendedDimension);	
			final Img<UnsignedLongType> extendedOutputPair =  new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType()).create(extendedDimension); //if a voxel is within contactDistance of two organelles of the same type, that is a contact site	

			final Cursor<UnsignedLongType> extendedOutputCursor = Views.flatIterable(extendedOutput).cursor();
			final Cursor<UnsignedLongType> extendedOutputPairCursor = Views.flatIterable(extendedOutputPair).cursor();
			final Cursor<FloatType> distanceFromObjectsCursor = distanceFromObjects.cursor();
			
			// Loop over image and label voxel within halo according to object ID. Note: This will mean that each voxel only gets one object assignment
			while (extendedOutputCursor.hasNext()) {
				final UnsignedLongType voxelOutput = extendedOutputCursor.next();
				final UnsignedLongType voxelOutputPair = extendedOutputPairCursor.next();
				final float distanceFromObjectsSquared = distanceFromObjectsCursor.next().get();
				long[] position = {extendedOutputCursor.getLongPosition(0),  extendedOutputCursor.getLongPosition(1),  extendedOutputCursor.getLongPosition(2)};
				if( (distanceFromObjectsSquared>0 && distanceFromObjectsSquared<=contactDistanceInVoxelsSquared) || isSurfaceVoxel(segmentedOrganelleRandomAccess, position)) {//then voxel is within distance
					long objectID = findAndSetValueToNearestOrganelleID(voxelOutput, distanceFromObjectsSquared, position, segmentedOrganelleRandomAccess);
					if(objectID>0) {//Check if another organelle has this voxel as part of its contact site
						findAndSetValueToOrganellePairID(objectID, distanceFromObjectsSquared, voxelOutputPair, contactDistanceInVoxelsSquared, position, segmentedOrganelleRandomAccess);	
					}
				}
			}
			
			//Crop and write out data
			RandomAccessibleInterval<UnsignedLongType> output = Views.offsetInterval(extendedOutput,new long[] {contactDistanceInVoxelsCeiling,contactDistanceInVoxelsCeiling,contactDistanceInVoxelsCeiling},dimension);
			RandomAccessibleInterval<UnsignedLongType> outputPair = Views.offsetInterval(extendedOutputPair,new long[] {contactDistanceInVoxelsCeiling,contactDistanceInVoxelsCeiling,contactDistanceInVoxelsCeiling},dimension);
			
			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetNameOrganelle, gridBlock[2]);
			N5Utils.saveBlock(outputPair, n5WriterLocal, outputN5DatasetNameOrganellePairs, gridBlock[2]);
			
		});
	}
	
	/**
	 * For a given voxel that is within the contact distance, set its id to the nearest organelle.
	 * 
	 * @param voxelOutput						Voxel to set
	 * @param distanceSquared					Squared distance from object
	 * @param position							Voxel position
	 * @param segmentedOrganelleRandomAccess	Random access for the segmented dataset
	 * @return									ID of nearest object
	 */
	private static long findAndSetValueToNearestOrganelleID(UnsignedLongType voxelOutput,float distanceSquared, long [] position, RandomAccess<UnsignedLongType>segmentedOrganelleRandomAccess) {
		Set<List<Integer>> voxelsToCheck = getVoxelsToCheckBasedOnDistance(distanceSquared);
		for(List<Integer> voxelToCheck : voxelsToCheck) {
			int dx = voxelToCheck.get(0);
			int dy = voxelToCheck.get(1);
			int dz = voxelToCheck.get(2);
			segmentedOrganelleRandomAccess.setPosition(new long[] {position[0]+dx,position[1]+dy,position[2]+dz});
			long currentObjectID = segmentedOrganelleRandomAccess.get().get();
			if(currentObjectID > 0) {
				voxelOutput.set(currentObjectID);
				return currentObjectID;
			}	
		}
		return 0;
	}
	
	/** 
	 * If a voxel is within a the contact distance of an organelle, check if it is also within contact distance of another organelle of the same type. This is necessary for checking for intra-type contacts.
	 *  
	 * @param objectID							ID of nearest organelle
	 * @param distanceFromObjectSquared			Squared distance from nearest organelle
	 * @param voxelOutputPair					Voxel in pair image
	 * @param contactDistanceInVoxelsSquared	Squared contact distance
	 * @param position							Position
	 * @param segmentedOrganelleRandomAccess	Random access for segmented dataset
	 */
	private static void findAndSetValueToOrganellePairID(long objectID, float distanceFromObjectSquared, UnsignedLongType voxelOutputPair, double contactDistanceInVoxelsSquared, long [] position, RandomAccess<UnsignedLongType> segmentedOrganelleRandomAccess){
		//For a given voxel outside an object, find the closest object to it and relabel the voxel with the corresponding object ID
		int distanceFromObject = (int) Math.floor(Math.sqrt(distanceFromObjectSquared));
		int distanceFromOrganellePairSquared = Integer.MAX_VALUE;
		long organellePairID=0;
		for(int radius = distanceFromObject; radius <= Math.sqrt(contactDistanceInVoxelsSquared); radius++) {//must be at least as far away as other organelle
			int radiusSquared = radius*radius;
			int radiusMinus1Squared = (radius-1)*(radius-1);
			for(int x=-radius; x<=radius; x++){
				for(int y=-radius; y<=radius; y++) {
					for(int z=-radius; z<=radius; z++) {
						int currentDistanceSquared = x*x+y*y+z*z;
						if(currentDistanceSquared>radiusMinus1Squared && currentDistanceSquared<=radiusSquared && currentDistanceSquared<=contactDistanceInVoxelsSquared) {
							segmentedOrganelleRandomAccess.setPosition(new long[] {position[0]+x,position[1]+y,position[2]+z});
							long currentObjectID = segmentedOrganelleRandomAccess.get().get();
							if(currentObjectID > 0 && currentObjectID != objectID) { //then is within contact distance of another organelle
								if(currentDistanceSquared < distanceFromOrganellePairSquared) { //ensure it is closest
									distanceFromOrganellePairSquared = currentDistanceSquared;
									organellePairID = currentObjectID;
								}
							
							}
						}
					}
				}
			}
			if (organellePairID>0) {
				voxelOutputPair.set(organellePairID);
				return;
			}
		}
		return;
	}

	
	/**
	 * Find connected components of contact sites for LM data on a block-by-block basis and write out to
	 * temporary n5. Since this is for LM data, a voxel is part of a contact site if it is in both objects, ie, there are no in-between voxels that are part of the contact site.
	 *
	 * 
	 * @param sc					Spark context
	 * @param inputN5Path			Input N5 path
	 * @param organelle1			First organelle name
	 * @param organelle2			Second organelle name
	 * @param outputN5Path			Output N5 path
	 * @param outputN5DatasetName	Output N5 dataset name
	 * @param minimumVolumeCutoff	Minimum volume cutoff (nm^3), above which objects will be kept
	 * @param blockInformationList	List of block information
	 * @return						List of block information
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final List<BlockInformation> blockwiseConnectedComponentsLM(
			final JavaSparkContext sc, final String inputN5Path, String organelle1, String organelle2,
			final String outputN5Path, final String outputN5DatasetName, final double minimumVolumeCutoff,
			List<BlockInformation> blockInformationList) throws IOException {
		boolean sameOrganelleClassTemp=false;
		if(organelle1.equals(organelle2)) {
			organelle1+="_contact_boundary_temp_to_delete";
			organelle2+="_pairs_contact_boundary_temp_to_delete";
			sameOrganelleClassTemp=true;
		}
		else {
			organelle1+="_contact_boundary_temp_to_delete";
			organelle2+="_contact_boundary_temp_to_delete";
		}
		final boolean sameOrganelleClass = sameOrganelleClassTemp;
		final String organelle1ContactBoundaryString = organelle1;
		final String organelle2ContactBoundaryString = organelle2;
		// Get attributes of input data set
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(organelle1);
		final int[] blockSize = attributes.getBlockSize();
		final long[] blockSizeL = new long[] { blockSize[0], blockSize[1], blockSize[2] };
		final long[] outputDimensions = attributes.getDimensions();
		final double [] pixelResolution = IOHelper.getResolution(n5Reader, organelle1);
		
		// Create output dataset
		final N5Writer n5Writer = new N5FSWriter(outputN5Path);
		n5Writer.createGroup(outputN5DatasetName);
		n5Writer.createDataset(outputN5DatasetName, outputDimensions, blockSize,
				DataType.UINT64, attributes.getCompression());
		n5Writer.setAttribute(outputN5DatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));

		// Set up rdd to parallelize over blockInformation list and run RDD, which will
		// return updated block information containing list of components on the edge of
		// the corresponding block
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<BlockInformation> javaRDDsets = rdd.map(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];

			// Read in source block
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			
			final RandomAccessibleInterval<UnsignedLongType>  organelle1Data = Views.offsetInterval(Views.extendZero(
						(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, organelle1ContactBoundaryString)
						),offset, dimension);
			final RandomAccessibleInterval<UnsignedLongType>  organelle2Data = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, organelle2ContactBoundaryString)
					),offset, dimension);
			
			RandomAccess<UnsignedLongType> organelle1DataRA = organelle1Data.randomAccess();
			RandomAccess<UnsignedLongType> organelle2DataRA = organelle2Data.randomAccess();
			
			Set<List<Long>> contactSiteOrganellePairsSet = new HashSet<>();
			for(int x=0; x<dimension[0]; x++) {
				for(int y=0; y<dimension[1]; y++){
					for(int z=0; z<dimension[2]; z++) {
						int [] pos = new int[] {x,y,z};
						organelle1DataRA.setPosition(pos);
						organelle2DataRA.setPosition(pos);
						
						Long organelle1ID = organelle1DataRA.get().get();
						Long organelle2ID = organelle2DataRA.get().get();

						if(organelle1ID>0 && organelle2ID>0) { //then is part of a contact site
							contactSiteOrganellePairsSet.add(Arrays.asList(organelle1ID,organelle2ID));
						}
					}
				}
			}
			
			long[] currentDimensions = { 0, 0, 0 };
			organelle1Data.dimensions(currentDimensions);
			
			// Create the output based on the current dimensions
			final Img<UnsignedLongType> output = new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType()).create(currentDimensions);
			
			final Img<UnsignedByteType> currentPairBinarized = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType()).create(currentDimensions);
			RandomAccess<UnsignedByteType> currentPairBinarizedRA = currentPairBinarized.randomAccess();
			currentBlockInformation.edgeComponentIDtoVolumeMap = new HashMap();
			currentBlockInformation.edgeComponentIDtoOrganelleIDs = new HashMap();
			for( List<Long> organellePairs : contactSiteOrganellePairsSet ) {
				for(int x=0; x<dimension[0]; x++) {
					for(int y=0; y<dimension[1]; y++){
						for(int z=0; z<dimension[2]; z++) {
							int [] pos = new int[] {x,y,z};
							organelle1DataRA.setPosition(pos);
							organelle2DataRA.setPosition(pos);
							currentPairBinarizedRA.setPosition(pos);
							Long organelle1ID = organelle1DataRA.get().get();
							Long organelle2ID = organelle2DataRA.get().get();
							if(organelle1ID.equals(organellePairs.get(0)) && organelle2ID.equals(organellePairs.get(1))) {
								currentPairBinarizedRA.get().set(1);
							}
							else if(sameOrganelleClass && (organelle1ID.equals(organellePairs.get(1)) && organelle2ID.equals(organellePairs.get(0))) ) {//if it is the same organelle class, then ids can be swapped since they still correspond to the same pair
								currentPairBinarizedRA.get().set(1);
							}
							else {
								currentPairBinarizedRA.get().set(0);
							}
						}
					}
				}
				
				// Compute the connected components which returns the components along the block
				// edges, and update the corresponding blockInformation object
				int minimumVolumeCutoffInVoxels = (int) Math.ceil(minimumVolumeCutoff/Math.pow(pixelResolution[0],3));
				
				Map<Long, Long> currentPairEdgeComponentIDtoVolumeMap = SparkConnectedComponents.computeConnectedComponents(currentPairBinarized, output, outputDimensions,
						blockSizeL, offset, 1, minimumVolumeCutoffInVoxels);
				currentBlockInformation.edgeComponentIDtoVolumeMap.putAll(currentPairEdgeComponentIDtoVolumeMap);
				for(Long edgeComponentID : currentPairEdgeComponentIDtoVolumeMap.keySet()) {
					if(sameOrganelleClass) {
						Long o1 = organellePairs.get(0);
						Long o2 = organellePairs.get(1);
						List<Long> sortedPair = o1<o2 ? Arrays.asList(o1,o2) : Arrays.asList(o2,o1);
						currentBlockInformation.edgeComponentIDtoOrganelleIDs.put(edgeComponentID, sortedPair);
					}
					else{
						currentBlockInformation.edgeComponentIDtoOrganelleIDs.put(edgeComponentID, Arrays.asList(organellePairs.get(0), organellePairs.get(1)));
					}
				}
				
			}

			// Write out output to temporary n5 stack
			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);

			return currentBlockInformation;
		});

		// Run, collect and return blockInformationList
		blockInformationList = javaRDDsets.collect();

		return blockInformationList;
	}
	
	/**
	 * Find connected components of contact sites for EM data on a block-by-block basis and write out to
	 * temporary n5. Since this is for LM data, a voxel is part of a contact site if it is in both objects, ie, there are no in-between voxels that are part of the contact site.
	 *
	 * 
	 * @param sc					Spark context
	 * @param inputN5Path			Input N5 path
	 * @param organelle1			First organelle name
	 * @param organelle2			Second organelle name
	 * @param outputN5Path			Output N5 path
	 * @param outputN5DatasetName	Output N5 dataset name
	 * @param minimumVolumeCutoff	Minimum volume cutoff (nm^3), above which objects will be kept
	 * @param blockInformationList	List of block information
	 * @param contactDistance		Contact distance
	 * @return						List of block information
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> List<BlockInformation> blockwiseConnectedComponentsEM(
			final JavaSparkContext sc, final String inputN5Path, String organelle1, String organelle2,
			final String outputN5Path, final String outputN5DatasetName, final double minimumVolumeCutoff, double contactDistance,
			List<BlockInformation> blockInformationList) throws IOException {
		
		boolean sameOrganelleClassTemp=false;
		String organelle1ContactBoundaryName, organelle2ContactBoundaryName;
		if(organelle1.equals(organelle2)) { //always look at pairs since know that organelle is within its own halo, basically want to see if there is another organelle in the same distance
			organelle1ContactBoundaryName = organelle1+"_pairs_contact_boundary_temp_to_delete"; 
			organelle2ContactBoundaryName = organelle2+"_pairs_contact_boundary_temp_to_delete";
			sameOrganelleClassTemp=true;
		}
		else {
			organelle1ContactBoundaryName = organelle1+"_contact_boundary_temp_to_delete";
			organelle2ContactBoundaryName = organelle2+"_contact_boundary_temp_to_delete";
		}		
		final boolean sameOrganelleClass = sameOrganelleClassTemp;
	
		// Get attributes of input data set
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(organelle1);
		final int[] blockSize = attributes.getBlockSize();
		final long[] blockSizeL = new long[] { blockSize[0], blockSize[1], blockSize[2] };
		final long[] outputDimensions = attributes.getDimensions();
		final double [] pixelResolution = IOHelper.getResolution(n5Reader, organelle1);
		
		// Create output dataset
		final N5Writer n5Writer = new N5FSWriter(outputN5Path);
		n5Writer.createGroup(outputN5DatasetName);
		n5Writer.createDataset(outputN5DatasetName, outputDimensions, blockSize,
				DataType.UINT64, attributes.getCompression());
		n5Writer.setAttribute(outputN5DatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));

		// Set up rdd to parallelize over blockInformation list and run RDD, which will
		// return updated block information containing list of components on the edge of
		// the corresponding block
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		
		//Get contact distance in voxels
		final double contactDistanceInVoxels = contactDistance/pixelResolution[0];
		double expandedContactDistanceSquared = (contactDistanceInVoxels+2)*(contactDistanceInVoxels+2);//expand by 2 since including surface voxels (1 for each voxel);
		int contactDistanceInVoxelsCeiling=(int)Math.ceil(contactDistanceInVoxels);
	
		int minimumVolumeCutoffInVoxels = (int) Math.ceil(minimumVolumeCutoff/Math.pow(pixelResolution[0],3));

		JavaRDD<BlockInformation> javaRDDsets = rdd.map(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			long padding = contactDistanceInVoxelsCeiling+1;
			long[] paddedOffset = {offset[0]-padding, offset[1]-padding, offset[2] - padding};
			long[] paddedDimension = {dimension[0]+2*padding, dimension[1]+2*padding, dimension[2]+2*padding};

			// Read in source block
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			
			final RandomAccessibleInterval<UnsignedLongType>  organelle1Data = Views.offsetInterval(Views.extendZero(
						(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, organelle1)
						),paddedOffset, paddedDimension);
			final RandomAccessibleInterval<UnsignedLongType>  organelle2Data = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, organelle2)
					),paddedOffset, paddedDimension);
			
			RandomAccessibleInterval<UnsignedLongType>  organelle1ContactBoundaryData = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, organelle1ContactBoundaryName)
					),paddedOffset, paddedDimension);
			RandomAccessibleInterval<UnsignedLongType>  organelle2ContactBoundaryData = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, organelle2ContactBoundaryName)
					),paddedOffset, paddedDimension);
			
			RandomAccess<UnsignedLongType> organelle1DataRA = organelle1Data.randomAccess();
			RandomAccess<UnsignedLongType> organelle2DataRA = organelle2Data.randomAccess();
			RandomAccess<UnsignedLongType> organelle1ContactBoundaryDataRA = organelle1ContactBoundaryData.randomAccess();
			RandomAccess<UnsignedLongType> organelle2ContactBoundaryDataRA = organelle2ContactBoundaryData.randomAccess();
			
			ContactSiteInformation csi = getContactSiteInformation(paddedDimension, organelle1DataRA, organelle2DataRA, organelle1ContactBoundaryDataRA, organelle2ContactBoundaryDataRA, sameOrganelleClass);
			
			// Create the output based on the current dimensions
			
			final Img<UnsignedLongType> output = new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType()).create(dimension);
			
			currentBlockInformation.edgeComponentIDtoVolumeMap = new HashMap<Long,Long>();
			currentBlockInformation.edgeComponentIDtoOrganelleIDs = new HashMap<Long,List<Long>>();
			
			organelle1ContactBoundaryData = null;
			organelle1ContactBoundaryDataRA = null;
			organelle2ContactBoundaryData = null;
			organelle2ContactBoundaryDataRA = null;
			
			for( List<Long> currentOrganellePair : csi.allOrganellePairs ) {
				long organelle1ID = currentOrganellePair.get(0);
				long organelle2ID = currentOrganellePair.get(1);
				if(sameOrganelleClass && organelle1ID==organelle2ID) break; //then it is an object to itself contact site
				
				Set<List<Long>> allContactSiteVoxels = getContactSiteVoxelsForOrganellePair(csi, currentOrganellePair, organelle1DataRA, organelle2DataRA, expandedContactDistanceSquared, padding, dimension, paddedDimension);
				
				if(!allContactSiteVoxels.isEmpty()) {
					doConnectedComponentsForOrganellePairContactSites(organelle1ID, organelle2ID, sameOrganelleClass, allContactSiteVoxels, output, minimumVolumeCutoffInVoxels, blockSizeL, offset, dimension, outputDimensions, currentBlockInformation);
				}			// Write out output to temporary n5 stack
			}
			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);

			return currentBlockInformation;
		});

		// Run, collect and return blockInformationList
		blockInformationList = javaRDDsets.collect();

		return blockInformationList;
	}
	
	/**
	 * 
	 * @param csi
	 * @param currentOrganellePair
	 * @param organelle1DataRA
	 * @param organelle2DataRA
	 * @param expandedContactDistanceSquared
	 * @param padding
	 * @param dimension
	 * @param paddedDimension
	 * @return
	 */
	public static Set<List<Long>> getContactSiteVoxelsForOrganellePair(ContactSiteInformation csi, List<Long> currentOrganellePair, RandomAccess<UnsignedLongType> organelle1DataRA,RandomAccess<UnsignedLongType> organelle2DataRA, double expandedContactDistanceSquared, long padding, long[] dimension, long [] paddedDimension){
		Set<List<Long>> allContactSiteVoxels = new HashSet<List<Long>>();
		
		List<long[]> organelle1SurfaceVoxels = csi.organelle1SurfaceVoxelsWithinOrganelle2ContactDistance.getOrDefault(currentOrganellePair, new ArrayList<long[]>());
		List<long[]> organelle2SurfaceVoxels = csi.organelle2SurfaceVoxelsWithinOrganelle1ContactDistance.getOrDefault(currentOrganellePair, new ArrayList<long[]>());
	
		for(long [] organelle1SurfaceVoxel : organelle1SurfaceVoxels) {
			for(long [] organelle2SurfaceVoxel : organelle2SurfaceVoxels) {
				if(Math.pow(organelle2SurfaceVoxel[0]-organelle1SurfaceVoxel[0],2) + Math.pow(organelle2SurfaceVoxel[1]-organelle1SurfaceVoxel[1],2) + Math.pow(organelle2SurfaceVoxel[2]-organelle1SurfaceVoxel[2],2)<=expandedContactDistanceSquared) { //then could be valid
					List<long[]> voxelsToCheck = Bressenham3D.getLine(organelle1SurfaceVoxel,organelle2SurfaceVoxel);
					if(!voxelPathEntersObject(organelle1DataRA, organelle2DataRA,voxelsToCheck,csi.allSurfaceVoxelIDs, paddedDimension)) {
						 for(long[] validVoxel : voxelsToCheck) {
							long [] correctedPosition = new long[] {validVoxel[0]-padding, validVoxel[1]-padding,validVoxel[2]-padding};
							if(correctedPosition[0]>=0 && correctedPosition[1]>=0 && correctedPosition[2]>=0 &&
									correctedPosition[0]<dimension[0] && correctedPosition[1]<dimension[1] && correctedPosition[2]<dimension[2]) {
								allContactSiteVoxels.add(Arrays.asList(correctedPosition[0],correctedPosition[1],correctedPosition[2]));
							}
						}
					}
				}
			}
		}		
		return allContactSiteVoxels;
	}

	public static void doConnectedComponentsForOrganellePairContactSites(long organelle1ID, long organelle2ID, boolean sameOrganelleClass, Set<List<Long>> allContactSiteVoxels, Img<UnsignedLongType> output, int minimumVolumeCutoffInVoxels, long [] blockSizeL, long [] offset, long [] dimension, long outputDimensions[], BlockInformation currentBlockInformation) {
		Img<UnsignedByteType> currentPairBinarized = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType()).create(dimension);
		RandomAccess<UnsignedByteType> currentPairBinarizedRA = currentPairBinarized.randomAccess();
		for(List<Long> contactSiteVoxel : allContactSiteVoxels) {
			currentPairBinarizedRA.setPosition(new long[] {contactSiteVoxel.get(0),contactSiteVoxel.get(1),contactSiteVoxel.get(2)});
			currentPairBinarizedRA.get().set(1);
		}
	
		// Compute the connected components which returns the components along the block
		// edges, and update the corresponding blockInformation object					
		Map<Long, Long> currentPairEdgeComponentIDtoVolumeMap = SparkConnectedComponents.computeConnectedComponents(currentPairBinarized, output, outputDimensions,
				blockSizeL, offset, 1, minimumVolumeCutoffInVoxels, new RectangleShape(1,false));
		currentBlockInformation.edgeComponentIDtoVolumeMap.putAll(currentPairEdgeComponentIDtoVolumeMap);
		for(Long edgeComponentID : currentPairEdgeComponentIDtoVolumeMap.keySet()) {
			if(sameOrganelleClass) {
				List<Long> sortedPair = organelle1ID<organelle2ID ? Arrays.asList(organelle1ID,organelle2ID) : Arrays.asList(organelle2ID,organelle1ID);
				currentBlockInformation.edgeComponentIDtoOrganelleIDs.put(edgeComponentID, sortedPair);
			}
			else{
				currentBlockInformation.edgeComponentIDtoOrganelleIDs.put(edgeComponentID, Arrays.asList(organelle1ID, organelle2ID));
			}
		}
	}
	
	public static ContactSiteInformation getContactSiteInformation(long [] paddedDimension, RandomAccess<UnsignedLongType> organelle1DataRA, RandomAccess<UnsignedLongType> organelle2DataRA, RandomAccess<UnsignedLongType> organelle1ContactBoundaryDataRA, RandomAccess<UnsignedLongType> organelle2ContactBoundaryDataRA, boolean sameOrganelleClass) {
		Map<List<Long>,List<long[]>> organelle1SurfaceVoxelsWithinOrganelle2ContactDistance = new HashMap<List<Long>,List<long[]>>();
		Map<List<Long>,List<long[]>> organelle2SurfaceVoxelsWithinOrganelle1ContactDistance = new HashMap<List<Long>,List<long[]>>();
		Set<List<Long>> allOrganellePairs = new HashSet<List<Long>>();
		Set<Long> allSurfaceVoxelIDs = new HashSet<Long>();
		
		boolean surfaceVoxel;
		for(long x=0; x<paddedDimension[0]; x++) {
			for(long y=0; y<paddedDimension[1]; y++){
				for(long z=0; z<paddedDimension[2]; z++) {
					long [] pos = new long[] {x,y,z};
					surfaceVoxel = false;
					organelle1ContactBoundaryDataRA.setPosition(pos);
					organelle2ContactBoundaryDataRA.setPosition(pos);
	
					long organelle1ID = 0,organelle2ID = 0;
					if(organelle2ContactBoundaryDataRA.get().get()>0 && isSurfaceVoxel(organelle1DataRA, pos)) {//if it is withincutoff of other class
						surfaceVoxel = true;
						organelle1ID = organelle1DataRA.get().get();
						organelle2ID = organelle2ContactBoundaryDataRA.get().get();
						List<long[]> surfaceVoxels = organelle1SurfaceVoxelsWithinOrganelle2ContactDistance.getOrDefault( Arrays.asList(organelle1ID, organelle2ID),new ArrayList<long[]>());
						surfaceVoxels.add(pos);
						organelle1SurfaceVoxelsWithinOrganelle2ContactDistance.put(Arrays.asList(organelle1ID,organelle2ID),surfaceVoxels);
					}
					if(organelle1ContactBoundaryDataRA.get().get()>0 && isSurfaceVoxel(organelle2DataRA,pos)) {
						organelle1ID = organelle1ContactBoundaryDataRA.get().get();
						organelle2ID = organelle2DataRA.get().get();
						surfaceVoxel = true;
						List<long[]> surfaceVoxels = organelle2SurfaceVoxelsWithinOrganelle1ContactDistance.getOrDefault( Arrays.asList(organelle1ID, organelle2ID),new ArrayList<long[]>());
						surfaceVoxels.add(pos);
						organelle2SurfaceVoxelsWithinOrganelle1ContactDistance.put(Arrays.asList(organelle1ID, organelle2ID),surfaceVoxels);
					}
					if(surfaceVoxel) {
						Long ID = SparkCosemHelper.convertPositionToGlobalID(pos, paddedDimension);
						allSurfaceVoxelIDs.add(ID);
						allOrganellePairs.add((sameOrganelleClass && organelle2ID<organelle1ID) ? Arrays.asList(organelle2ID, organelle1ID) : Arrays.asList(organelle1ID, organelle2ID));
					}
				}
			}
		}
		
		return new ContactSiteInformation(organelle1SurfaceVoxelsWithinOrganelle2ContactDistance, organelle2SurfaceVoxelsWithinOrganelle1ContactDistance, allOrganellePairs, allSurfaceVoxelIDs);
	}
	
	public static class ContactSiteInformation{
		public Map<List<Long>,List<long[]>> organelle1SurfaceVoxelsWithinOrganelle2ContactDistance;
		public Map<List<Long>,List<long[]>> organelle2SurfaceVoxelsWithinOrganelle1ContactDistance;
		public Set<List<Long>> allOrganellePairs;
		public Set<Long> allSurfaceVoxelIDs;
		
		ContactSiteInformation(Map<List<Long>,List<long[]>> organelle1SurfaceVoxelsWithinOrganelle2ContactDistance,
		Map<List<Long>,List<long[]>> organelle2SurfaceVoxelsWithinOrganelle1ContactDistance,
		Set<List<Long>> allOrganellePairs,
		Set<Long> allSurfaceVoxelIDs){
			this.organelle1SurfaceVoxelsWithinOrganelle2ContactDistance = organelle1SurfaceVoxelsWithinOrganelle2ContactDistance;
			this.organelle2SurfaceVoxelsWithinOrganelle1ContactDistance = organelle2SurfaceVoxelsWithinOrganelle1ContactDistance;
			this.allOrganellePairs = allOrganellePairs;
			this.allSurfaceVoxelIDs = allSurfaceVoxelIDs;
		}
		
	}
	
	public static void getAllNearestSurfaceVoxelPairs(List<long[]> organelleASurfaceVoxels, List<long[]> organelleBSurfaceVoxels, double contactDistanceInVoxelsSquared, long[] dimensions, Set<List<Long>> allNearestSurfaceVoxelPairs) {
		for(long [] organelleASurfaceVoxel : organelleASurfaceVoxels) {
			double minVoxelPairDistanceSquared = Double.MAX_VALUE;//to ensure it is greater
			Set<List<Long>> currentSurfaceVoxelPairs = new HashSet<List<Long>>(); //nearest neighbors (may be multiple)
			for(long [] organelleBSurfaceVoxel : organelleBSurfaceVoxels) {
				double voxelPairDistanceSquared = Math.pow(organelleBSurfaceVoxel[0]-organelleASurfaceVoxel[0],2) + Math.pow(organelleBSurfaceVoxel[1]-organelleASurfaceVoxel[1],2) + Math.pow(organelleBSurfaceVoxel[2]-organelleASurfaceVoxel[2],2);
				if(voxelPairDistanceSquared==minVoxelPairDistanceSquared) {
					currentSurfaceVoxelPairs.add(Arrays.asList(SparkCosemHelper.convertPositionToGlobalID(organelleASurfaceVoxel, dimensions),SparkCosemHelper.convertPositionToGlobalID(organelleBSurfaceVoxel, dimensions)));
				}
				else if(voxelPairDistanceSquared<minVoxelPairDistanceSquared) {//reset
					minVoxelPairDistanceSquared = voxelPairDistanceSquared;
					currentSurfaceVoxelPairs = new HashSet<List<Long>>();
					currentSurfaceVoxelPairs.add(Arrays.asList(SparkCosemHelper.convertPositionToGlobalID(organelleASurfaceVoxel, dimensions),SparkCosemHelper.convertPositionToGlobalID(organelleBSurfaceVoxel, dimensions)));
				}
			}
			if(minVoxelPairDistanceSquared<=contactDistanceInVoxelsSquared) {
				allNearestSurfaceVoxelPairs.addAll(currentSurfaceVoxelPairs);
			}
		}
	}
	
	/**
	 * Merge all necessary objects obtained from blockwise connected components.
	 *
	 * Determines which objects need to be fused based on which ones touch at the
	 * boundary between blocks. Then performs the corresponding union find so each
	 * complete object has a unique id. Parallelizes over block information list.
	 *
	 * @param sc
	 * @param inputN5Path
	 * @param inputN5DatasetName
	 * @param blockInformationList
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> List<BlockInformation> unionFindConnectedComponents(
			final JavaSparkContext sc, final String inputN5Path, final String inputN5DatasetName, double minimumVolumeCutoff, final HashMap<Long,List<Long>> edgeComponentIDtoOrganelleIDs,
			boolean diamondShape, List<BlockInformation> blockInformationList) throws IOException {

		// Get attributes of input data set:
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final double [] pixelResolution = IOHelper.getResolution(n5Reader, inputN5DatasetName);

		// Set up and run RDD, which will return the set of pairs of object IDs that
		// need to be fused
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<Set<List<Long>>> javaRDDsets = rdd.map(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];

			// Get source
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			@SuppressWarnings("unchecked")
			final RandomAccessibleInterval<UnsignedLongType> source = (RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5ReaderLocal, inputN5DatasetName);
			long[] sourceDimensions = { 0, 0, 0 };
			source.dimensions(sourceDimensions);

			// Get hyperplanes of current block (x-most edge, y-most edge, z-most edge), and
			// the corresponding hyperplanes for the x+1 block, y+1 block and z+1 block.
			RandomAccessibleInterval<UnsignedLongType> xPlane1, yPlane1, zPlane1, xPlane2, yPlane2, zPlane2;
			xPlane1 = yPlane1 = zPlane1 = xPlane2 = yPlane2 = zPlane2 = null;

			long xOffset = offset[0] + blockSize[0];
			long yOffset = offset[1] + blockSize[1];
			long zOffset = offset[2] + blockSize[2];
			
			long padding = diamondShape ? 0 : 1; //if using rectangle shape, pad extra of 1 for neighboring block 
			long[] paddedOffset = new long[]{offset[0]-padding, offset[1]-padding, offset[2]-padding};
			long[] xPlaneDims = new long[] { 1, dimension[1]+2*padding, dimension[2]+2*padding };
			long[] yPlaneDims = new long[] { dimension[0]+2*padding, 1, dimension[2]+2*padding };
			long[] zPlaneDims = new long[] { dimension[0]+2*padding, dimension[1]+2*padding, 1 };
			xPlane1 = Views.offsetInterval(Views.extendZero(source), new long[] { xOffset - 1, paddedOffset[1], paddedOffset[2] },
					xPlaneDims);
			yPlane1 = Views.offsetInterval(Views.extendZero(source), new long[] { paddedOffset[0], yOffset - 1,paddedOffset[2] },
					yPlaneDims);
			zPlane1 = Views.offsetInterval(Views.extendZero(source), new long[] { paddedOffset[0], paddedOffset[1], zOffset - 1 },
					zPlaneDims);

			if (xOffset < sourceDimensions[0])
				xPlane2 = Views.offsetInterval(Views.extendZero(source), new long[] { xOffset, paddedOffset[1], paddedOffset[2] },
						xPlaneDims);
			if (yOffset < sourceDimensions[1])
				yPlane2 = Views.offsetInterval(Views.extendZero(source), new long[] { paddedOffset[0], yOffset, paddedOffset[2] },
						yPlaneDims);
			if (zOffset < sourceDimensions[2])
				zPlane2 = Views.offsetInterval(Views.extendZero(source), new long[] { paddedOffset[0], paddedOffset[1], zOffset },
						zPlaneDims);

			// Calculate the set of object IDs that are touching and need to be merged
			Set<List<Long>> globalIDtoGlobalIDSet = new HashSet<>();
			getGlobalIDsToMerge(xPlane1, xPlane2, edgeComponentIDtoOrganelleIDs, globalIDtoGlobalIDSet, diamondShape);
			getGlobalIDsToMerge(yPlane1, yPlane2, edgeComponentIDtoOrganelleIDs, globalIDtoGlobalIDSet, diamondShape);
			getGlobalIDsToMerge(zPlane1, zPlane2, edgeComponentIDtoOrganelleIDs, globalIDtoGlobalIDSet, diamondShape);
			// Calculate the set of object IDs that are touching and need to be merged
		

			return globalIDtoGlobalIDSet;
		});

		// Collect and combine the sets of objects to merge
		long t0 = System.currentTimeMillis();
		Set<List<Long>> globalIDtoGlobalIDFinalSet = javaRDDsets.reduce((a,b) -> {a.addAll(b); return a; });
		long t1 = System.currentTimeMillis();
		System.out.println(globalIDtoGlobalIDFinalSet.size());
		System.out.println("Total unions = " + globalIDtoGlobalIDFinalSet.size());

		// Perform union find to merge all touching objects
		UnionFindDGA unionFind = new UnionFindDGA(globalIDtoGlobalIDFinalSet);
		unionFind.getFinalRoots();

		long t2 = System.currentTimeMillis();

		System.out.println("collect time: " + (t1 - t0));
		System.out.println("union find time: " + (t2 - t1));
		System.out.println("Total edge objects: " + unionFind.globalIDtoRootID.values().stream().distinct().count());

		// Add block-specific relabel map to the corresponding block information object
		Map<Long, Long> rootIDtoVolumeMap= new HashMap<Long, Long>();
		for (BlockInformation currentBlockInformation : blockInformationList) {
			Map<Long, Long> currentGlobalIDtoRootIDMap = new HashMap<Long, Long>();
			for (Long currentEdgeComponentID : currentBlockInformation.edgeComponentIDtoVolumeMap.keySet()) {
				Long rootID;
				rootID = unionFind.globalIDtoRootID.getOrDefault(currentEdgeComponentID, currentEdgeComponentID); // Need this check since not all edge objects will be connected to neighboring blocks
				currentGlobalIDtoRootIDMap.put(currentEdgeComponentID, rootID);
				rootIDtoVolumeMap.put(rootID, rootIDtoVolumeMap.getOrDefault(rootID, 0L) + currentBlockInformation.edgeComponentIDtoVolumeMap.get(currentEdgeComponentID));
				
			}
			currentBlockInformation.edgeComponentIDtoRootIDmap = currentGlobalIDtoRootIDMap;
		}
		
		int minimumVolumeCutoffInVoxels = (int) Math.ceil(minimumVolumeCutoff/Math.pow(pixelResolution[0],3));
		for (BlockInformation currentBlockInformation : blockInformationList) {
			for (Entry <Long,Long> e : currentBlockInformation.edgeComponentIDtoRootIDmap.entrySet()) {
				Long key = e.getKey();
				Long value = e.getValue();
				currentBlockInformation.edgeComponentIDtoRootIDmap.put(key, 
						rootIDtoVolumeMap.get(value) <= minimumVolumeCutoffInVoxels ? 0L : value);
			}
		}
		
		

		return blockInformationList;
	}
	public static final void getGlobalIDsToMerge(RandomAccessibleInterval<UnsignedLongType> hyperSlice1,
			RandomAccessibleInterval<UnsignedLongType> hyperSlice2, final HashMap<Long,List<Long>> edgeComponentIDtoOrganelleIDs, Set<List<Long>> globalIDtoGlobalIDSet, boolean diamondShape) {
		if(diamondShape) getGlobalIDsToMergeDiamondShape(hyperSlice1, hyperSlice2, edgeComponentIDtoOrganelleIDs, globalIDtoGlobalIDSet);
		else { getGlobalIDsToMergeRectangleShape(hyperSlice1, hyperSlice2, edgeComponentIDtoOrganelleIDs, globalIDtoGlobalIDSet);}
	}
	
	public static final void getGlobalIDsToMergeDiamondShape(RandomAccessibleInterval<UnsignedLongType> hyperSlice1,
			RandomAccessibleInterval<UnsignedLongType> hyperSlice2, final HashMap<Long,List<Long>> edgeComponentIDtoOrganelleIDs, Set<List<Long>> globalIDtoGlobalIDSet) {
		// The global IDS that need to be merged are those that are touching along the
		// hyperplane borders between adjacent blocks
		if (hyperSlice1 != null && hyperSlice2 != null) {
			Cursor<UnsignedLongType> hs1Cursor = Views.flatIterable(hyperSlice1).cursor();
			Cursor<UnsignedLongType> hs2Cursor = Views.flatIterable(hyperSlice2).cursor();
			while (hs1Cursor.hasNext()) {
				long hs1Value = hs1Cursor.next().getLong();
				long hs2Value = hs2Cursor.next().getLong();
				
				List<Long> organelleIDs1 = edgeComponentIDtoOrganelleIDs.get(hs1Value);
				List<Long> organelleIDs2 = edgeComponentIDtoOrganelleIDs.get(hs2Value);
				if (hs1Value > 0 && hs2Value > 0 && organelleIDs1.equals(organelleIDs2)) {
					globalIDtoGlobalIDSet.add(Arrays.asList(hs1Value, hs2Value));// hs1->hs2 pair should always be
																					// distinct since hs1 is unique to
																					// first block
				}
			}

		}
	}
	
	public static final void getGlobalIDsToMergeRectangleShape(RandomAccessibleInterval<UnsignedLongType> hyperSlice1,
			RandomAccessibleInterval<UnsignedLongType> hyperSlice2, final HashMap<Long,List<Long>> edgeComponentIDtoOrganelleIDs, Set<List<Long>> globalIDtoGlobalIDSet) {
		// The global IDS that need to be merged are those that are touching along the
		// hyperplane borders between adjacent blocks
		if (hyperSlice1 != null && hyperSlice2 != null) {
			RandomAccess<UnsignedLongType> hs1RA = hyperSlice1.randomAccess();
			RandomAccess<UnsignedLongType> hs2RA = hyperSlice2.randomAccess();
			
			long [] dimensions = new long [] {hyperSlice1.dimension(0),hyperSlice1.dimension(1),hyperSlice1.dimension(2)};
			
			List<Long> xPositions = new ArrayList<Long>();
			List<Long> yPositions = new ArrayList<Long>();
			List<Long> zPositions = new ArrayList<Long>();
			
			List<Long> deltaX= new ArrayList<Long>();
			List<Long> deltaY= new ArrayList<Long>();
			List<Long> deltaZ= new ArrayList<Long>();
			
			//initialize before we know which dimension the plane is along
			xPositions = Arrays.asList(1L,dimensions[0]-2);
			yPositions = Arrays.asList(1L,dimensions[1]-2);
			zPositions = Arrays.asList(1L,dimensions[2]-2);

			deltaX = Arrays.asList(-1L,0L,1L) ;
			deltaY = Arrays.asList(-1L,0L,1L) ;
			deltaZ = Arrays.asList(-1L,0L,1L) ;
			
			
			//determine plane we are working on
			if(dimensions[0]==1) {
				deltaX = Arrays.asList(0L);
				xPositions = Arrays.asList(0L,0L);
			}
			else if(dimensions[1]==1){
				deltaY = Arrays.asList(0L);
				yPositions = Arrays.asList(0L,0L);
			}
			else {
				deltaZ = Arrays.asList(0L);
				zPositions = Arrays.asList(0L,0L);	
			}

			long hs1Value, hs2Value;
			for(long x=xPositions.get(0); x<=xPositions.get(1); x++) {
				for(long y=yPositions.get(0); y<=yPositions.get(1); y++) {
					for(long z=zPositions.get(0); z<=zPositions.get(1); z++) {
						long [] pos = new long[] {x,y,z};
						hs1RA.setPosition(pos);
						hs1Value = hs1RA.get().get();
						if(hs1Value>0) {
							for(long dx : deltaX) {
								for(long dy : deltaY) {
									for(long dz : deltaZ) {
										long [] newPos = new long[] {pos[0]+dx, pos[1]+dy, pos[2]+dz};
										hs2RA.setPosition(newPos);
										hs2Value = hs2RA.get().get();
										
										if(hs2Value>0) {
											List<Long> organelleIDs1 = edgeComponentIDtoOrganelleIDs.get(hs1Value);
											List<Long> organelleIDs2 = edgeComponentIDtoOrganelleIDs.get(hs2Value);
											if(organelleIDs1.equals(organelleIDs2)) globalIDtoGlobalIDSet.add(Arrays.asList(hs1Value, hs2Value));// hs1->hs2 pair should always be
											// distinct since hs1 is unique to
											// first block
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	
	public static final Set<List<Integer>> getVoxelsToCheckBasedOnDistance(float distanceSquared) {
		Set<List<Integer>> voxelsToCheck= new HashSet<>();
		double distance = Math.sqrt(distanceSquared);
		int [] posNeg = new int [] {1,-1};
		for(int i=0; i<=distance;i++) {
			for(int j=0; j<=distance; j++) {
				for(int k=0; k<=distance; k++) {
					if (i*i+j*j+k*k == distanceSquared) {
						for(int ip : posNeg) {
							for(int jp : posNeg) {
								for(int kp : posNeg) {
									voxelsToCheck.add(Arrays.asList(i*ip,j*jp,k*kp));
								}
							}
						}
					}
				}
			}
		}	
		return voxelsToCheck;
	}

	public static boolean voxelPathEntersObject(RandomAccess<UnsignedLongType> organelle1RA,RandomAccess<UnsignedLongType> organelle2RA, List<long[]> voxelsToCheck, Set<Long> allSurfaceVoxelsToCheck, long[] paddedDimension) {
		
		for(int i=1; i<voxelsToCheck.size()-1; i++) {//don't check start and end since those are defined already to be surface voxels.
			long[] currentVoxel = voxelsToCheck.get(i);
			organelle1RA.setPosition(currentVoxel);
			if(organelle1RA.get().get()>0 ) return true;//&& !allSurfaceVoxelsToCheck.contains(convertPositionToID(currentVoxel, paddedDimension))) return true;			//if it crosses, but is not a surface voxel
			else {
				organelle2RA.setPosition(currentVoxel);
				if(organelle2RA.get().get() >0 ) return true; //&& !allSurfaceVoxelsToCheck.contains(convertPositionToID(currentVoxel, paddedDimension))) return true;
			}	
		}
		return false;
	}
	
	
	public static final void calculateContactSites(final SparkConf conf, final String [] organelles, final boolean doSelfContacts, final double minimumVolumeCutoff, final double cutoffDistance, final String inputN5Path, final String outputN5Path ) throws IOException {
		List<String> directoriesToDelete = new ArrayList<String>();
		for (int i = 0; i<organelles.length; i++) {
			final String organelle1 =organelles[i];
			for(int j= doSelfContacts ? i : i+1; j< organelles.length; j++) {
				String organelle2 = organelles[j];
				List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(inputN5Path, organelle1);
				JavaSparkContext sc = new JavaSparkContext(conf);
				
				final String organelleContactString = organelle1 + "_to_" + organelle2;
				final String tempOutputN5ConnectedComponents = organelleContactString + "_cc_blockwise_temp_to_delete";
				final String finalOutputN5DatasetName = organelleContactString + "_cc";
				
				if(cutoffDistance==0) {
					blockInformationList = blockwiseConnectedComponentsLM(
							sc, outputN5Path,
							organelle1, organelle2, 
							outputN5Path,
							tempOutputN5ConnectedComponents,
							minimumVolumeCutoff,
							blockInformationList);
				}
				else {
					blockInformationList = blockwiseConnectedComponentsEM(
							sc, outputN5Path,
							organelle1, organelle2, 
							outputN5Path,
							tempOutputN5ConnectedComponents,
							minimumVolumeCutoff, cutoffDistance,
							blockInformationList);
				}	
				
				
				HashMap<Long, List<Long>> edgeComponentIDtoOrganelleIDs = new HashMap<Long,List<Long>>();
				for(BlockInformation currentBlockInformation : blockInformationList) {
					edgeComponentIDtoOrganelleIDs.putAll(currentBlockInformation.edgeComponentIDtoOrganelleIDs);
				}
				boolean diamondShape = false; //using rectangle
				blockInformationList = unionFindConnectedComponents(sc, outputN5Path, tempOutputN5ConnectedComponents, minimumVolumeCutoff,edgeComponentIDtoOrganelleIDs, diamondShape, blockInformationList);
				
				SparkConnectedComponents.mergeConnectedComponents(sc, outputN5Path, tempOutputN5ConnectedComponents, finalOutputN5DatasetName, blockInformationList);				
				directoriesToDelete.add(outputN5Path + "/" + tempOutputN5ConnectedComponents);
				sc.close();
			}
		}
		
		SparkDirectoryDelete.deleteDirectories(conf, directoriesToDelete);
	}
	
	public static boolean isSurfaceVoxel(final RandomAccess<UnsignedLongType> sourceRandomAccess, long [] position ) {
		sourceRandomAccess.setPosition(position);
		long referenceVoxelValue = sourceRandomAccess.get().get();
		if(referenceVoxelValue==0) {//needs to be inside organelle
			return false;
		}
		else{
			for(int dx=-1; dx<=1; dx++) {
				for(int dy=-1; dy<=1; dy++) {
					for(int dz=-1; dz<=1; dz++) {
						if(!(dx==0 && dy==0 && dz==0)) {
							final long testPosition[] = {position[0]+dx, position[1]+dy, position[2]+dz};
							sourceRandomAccess.setPosition(testPosition);
							if(sourceRandomAccess.get().get() != referenceVoxelValue) {
								sourceRandomAccess.setPosition(position);
								return true;
							}
						}
					}
				}
			}
			sourceRandomAccess.setPosition(position);
			return false;	
		}
	
	}
	
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {
		final Options options = new Options(args);
		if (!options.parsedSuccessfully)
			return;
		final SparkConf conf = new SparkConf().setAppName("SparkContactSites");

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
		
		//First calculate the object contact boundaries
		if(!options.getSkipGeneratingContactBoundaries()) {
			for (String organelle : organelles) {
				JavaSparkContext sc = new JavaSparkContext(conf);
				List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), organelle);
				calculateObjectContactBoundaries(sc, options.getInputN5Path(), organelle,
						options.getOutputN5Path(),
						options.getContactDistance(), options.getDoSelfContacts(), blockInformationList);
				sc.close();
			}
		}
		System.out.println("finished boundaries");
		
		calculateContactSites(conf, organelles,  options.getDoSelfContacts(), options.getMinimumVolumeCutoff(), options.getContactDistance(), options.getInputN5Path(), options.getOutputN5Path());
		System.out.println("finished contact sites");
		
		//limitContactSitesToBetweenRegion(conf, organelles,  options.getInputN5Path());
	}
}
