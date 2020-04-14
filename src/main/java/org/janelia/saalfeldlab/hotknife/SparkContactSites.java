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
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;

/**
 * Connected components for an entire n5 volume
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

		@Option(name = "--maskN5Path", required = true, usage = "mask N5 path, e.g. /path/to/input/mask.n5")
		private String maskN5Path = null;

		@Option(name = "--contactDistance", required = false, usage = "Distance from orgnelle for contact site (nm)")
		private double contactDistance = 10;

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

		public String getMaskN5Path() {
			return maskN5Path;
		}

		public double getContactDistance() {
			return contactDistance;
		}
	}
	
	/**
	 * Calculate contact boundaries around objects
	 *
	 * For a given segmented dataset, writes out an n5 file which contains halos around organelles within contactDistance. Each is labeled with the corresponding object ID from the segmentation.
	 *
	 * @param sc
	 * @param inputN5Path
	 * @param organelle
	 * @param outputN5Path
	 * @param outputN5DatasetName
	 * @param contactDistance
	 * @param blockInformationList
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> void caclulateObjectContactBoundaries(
			final JavaSparkContext sc, final String inputN5Path, final String organelle,
			final String outputN5Path, final String outputN5DatasetName,
			final double contactDistance, List<BlockInformation> blockInformationList) throws IOException {
				
		// Get attributes of input data set
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(organelle);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();

		// Create output dataset
		final N5Writer n5Writer = new N5FSWriter(outputN5Path);
		n5Writer.createGroup(outputN5DatasetName);
		n5Writer.createDataset(outputN5DatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
		double [] pixelResolution = IOHelper.getResolution(n5Reader, organelle);
		n5Writer.setAttribute(outputN5DatasetName, "pixelResolution", pixelResolution);
		
		//Get contact distance in voxels
		final double contactDistanceInVoxels = contactDistance/pixelResolution[0];
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
	
			// Access input/output 
			final RandomAccess<UnsignedLongType> segmentedOrganelleRandomAccess = segmentedOrganelle.randomAccess();		
			final Img<UnsignedLongType> extendedOutput =  new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType()).create(extendedDimension);	
			final Cursor<UnsignedLongType> segmentedOrganelleCursor = Views.flatIterable(segmentedOrganelle).cursor();
			final Cursor<UnsignedLongType> extendedOutputCursor = Views.flatIterable(extendedOutput).cursor();

			// Loop over image and label voxel within halo according to object ID. Note: This will mean that each voxel only gets one object assignment
			while (segmentedOrganelleCursor.hasNext()) {
				final UnsignedLongType voxelOrganelle = segmentedOrganelleCursor.next();
				final UnsignedLongType voxelOutput = extendedOutputCursor.next();
				long[] position = {segmentedOrganelleCursor.getLongPosition(0),  segmentedOrganelleCursor.getLongPosition(1),  segmentedOrganelleCursor.getLongPosition(2)};
				if (voxelOrganelle.get()==0) {//Then it is outside
					checkIfVoxelIsWithinDistance(voxelOrganelle, voxelOutput, contactDistanceInVoxels, position, segmentedOrganelleRandomAccess);
				}
			}
			
			RandomAccessibleInterval<UnsignedLongType> output = Views.offsetInterval(extendedOutput,new long[] {contactDistanceInVoxelsCeiling,contactDistanceInVoxelsCeiling,contactDistanceInVoxelsCeiling},dimension);
			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);
			
		});
	}
	
	/**
	 * Calculate contact sites between object classes
	 *
	 * Gets contact sites from the contact halos around two different objects classes. The contact sites are taken to be voxels within the halos of two different object types
	 *
	 * @param sc
	 * @param inputN5Path
	 * @param organelle1
	 * @param organelle2
	 * @param outputN5Path
	 * @param outputN5DatasetName
	 * @param blockInformationList
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> void calculateContactSitesUsingObjectContactBoundaries(
			final JavaSparkContext sc, final String inputN5Path, final String organelle1, final String organelle2,
			final String outputN5Path, final String outputN5DatasetName, List<BlockInformation> blockInformationList) throws IOException {
				
		// Get attributes of input data set
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(organelle1);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();

		// Create output dataset
		final N5Writer n5Writer = new N5FSWriter(outputN5Path);
		n5Writer.createGroup(outputN5DatasetName);
		n5Writer.createDataset(outputN5DatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT8, attributes.getCompression());
		n5Writer.setAttribute(outputN5DatasetName, "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, organelle1)));

		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];

			// Read in source blocks and create cursors
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);			
			final RandomAccessibleInterval<UnsignedLongType> contactBoundaryOrganelle1 = Views.offsetInterval(Views.extendZero(
							(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, organelle1)),
							offset, dimension);
			final RandomAccessibleInterval<UnsignedLongType> contactBoundaryOrganelle2 = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, organelle2)),
					offset, dimension);
			final Cursor<UnsignedLongType> contactBoundaryOrganelle1Cursor = Views.flatIterable(contactBoundaryOrganelle1).cursor();
			final Cursor<UnsignedLongType> contactBoundaryOrganelle2Cursor = Views.flatIterable(contactBoundaryOrganelle2).cursor();
			
			// Create the output based on the current dimensions
			long[] currentDimensions = { 0, 0, 0 };
			contactBoundaryOrganelle1.dimensions(currentDimensions);
			final Img<UnsignedByteType> output = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType()).create(currentDimensions);
			final Cursor<UnsignedByteType> outputCursor = Views.flatIterable(output).cursor();
			
			//Loop over both object classes and consider an object part of a contact site if it is within the contact halo of an object in both classes.
			while (contactBoundaryOrganelle1Cursor.hasNext()) {
				final UnsignedLongType voxelContactBoundaryOrganelle1 = contactBoundaryOrganelle1Cursor.next();
				final UnsignedLongType voxelContactBoundaryOrganelle2 = contactBoundaryOrganelle2Cursor.next();
				final UnsignedByteType voxelOutput = outputCursor.next();
				if ((voxelContactBoundaryOrganelle1.get() > 0 && voxelContactBoundaryOrganelle2.get() > 0 )) {
						voxelOutput.set(1);
				}
			}

			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);		
		});
	}
	
	private static void checkIfVoxelIsWithinDistance(UnsignedLongType voxelOrganelle, UnsignedLongType voxelOutput, double contactDistanceInVoxels, long [] position, RandomAccess<UnsignedLongType>segmentedOrganelleRandomAccess) {
		//For a given voxel outside an object, find the closest object to it and relabel the voxel with the corresponding object ID
		for(int radius = 1; radius <= Math.ceil(contactDistanceInVoxels); radius++) {
			int radiusSquared = radius*radius;
			int radiusMinus1Squared = (radius-1)*(radius-1);
			for(int x=-radius; x<=radius; x++){
				for(int y=-radius; y<=radius; y++) {
					for(int z=-radius; z<=radius; z++) {
						double currentDistanceSquared = x*x+y*y+z*z;
						if(currentDistanceSquared>radiusMinus1Squared && currentDistanceSquared<=radiusSquared && currentDistanceSquared<=contactDistanceInVoxels) {
							segmentedOrganelleRandomAccess.setPosition(new long[] {position[0]+x,position[1]+y,position[2]+z});
							long currentObjectID = segmentedOrganelleRandomAccess.get().get();
							if(currentObjectID > 0) {
								voxelOutput.set(currentObjectID);
								return;
							}
						}
					}
				}
			}
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
		for (String organelle : organelles) {
			JavaSparkContext sc = new JavaSparkContext(conf);
			List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), organelle);
			final String organelleBoundaryString = organelle+"_contact_boundary_temp_to_delete";
			caclulateObjectContactBoundaries(sc, options.getInputN5Path(), organelle,
					options.getOutputN5Path(), organelleBoundaryString,
					options.getContactDistance(), blockInformationList);
			sc.close();
		}
		
		//For each pair of object classes, calculate the contact sites and get the connected component information
		List<String> directoriesToDelete = new ArrayList<String>();
		for (int i = 0; i<organelles.length; i++) {
			final String organelle1 =organelles[i];
			for(int j= i+1; j< organelles.length; j++) {
				final String organelle2 = organelles[j];

				List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), organelle1);
				JavaSparkContext sc = new JavaSparkContext(conf);
				
				final String organelleContactString = organelle1 + "_to_" + organelle2 + options.getOutputN5DatasetSuffix();
				final String tempOutputN5ContactSites= organelleContactString+"_temp_to_delete";
				final String tempOutputN5ConnectedComponents = organelleContactString + "_cc_blockwise_temp_to_delete";
				final String finalOutputN5DatasetName = organelleContactString + "_cc";
				
				System.out.println(organelleContactString + " " + new SimpleDateFormat("HH:mm:ss").format(new Date()));
				
				calculateContactSitesUsingObjectContactBoundaries(
						sc, options.getInputN5Path(), organelle1+"_contact_boundary_temp_to_delete", organelle2+"_contact_boundary_temp_to_delete",
						options.getOutputN5Path(), tempOutputN5ContactSites, blockInformationList); 
				System.out.println("Stage 1 Complete: " + new SimpleDateFormat("HH:mm:ss").format(new Date()));
				
				int minimumVolumeCutoff = 100;
				int intensityThreshold = 1;
				blockInformationList = SparkConnectedComponents.blockwiseConnectedComponents(sc, options.getOutputN5Path(), tempOutputN5ContactSites,
						options.getOutputN5Path(), tempOutputN5ConnectedComponents, options.getMaskN5Path(),
						intensityThreshold, minimumVolumeCutoff, blockInformationList);				
				
				System.out.println("Stage 2 Complete: " + new SimpleDateFormat("HH:mm:ss").format(new Date()));

				blockInformationList = SparkConnectedComponents.unionFindConnectedComponents(sc, options.getOutputN5Path(), tempOutputN5ConnectedComponents, minimumVolumeCutoff,
						blockInformationList);
				
				System.out.println("Stage 3 Complete: " + new SimpleDateFormat("HH:mm:ss").format(new Date()));
				
				SparkConnectedComponents.mergeConnectedComponents(sc, options.getOutputN5Path(), tempOutputN5ConnectedComponents, finalOutputN5DatasetName,
						blockInformationList);
				
				System.out.println("Stage 4 Complete: " + new SimpleDateFormat("HH:mm:ss").format(new Date()));
				
				directoriesToDelete.add(options.getOutputN5Path() + "/" + tempOutputN5ContactSites);
				directoriesToDelete.add(options.getOutputN5Path() + "/" + tempOutputN5ConnectedComponents);
				sc.close();
			}
		}
		
		SparkDirectoryDelete.deleteDirectories(conf, directoriesToDelete);
		System.out.println("Stage 5 Complete: " + new SimpleDateFormat("HH:mm:ss").format(new Date()));
		
	}
}
