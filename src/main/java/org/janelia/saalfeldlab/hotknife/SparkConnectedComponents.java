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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.hotknife.util.Grid;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.spark_project.guava.collect.Sets;

import ij.ImageJ;
import net.imagej.ops.Ops.Filter.Gauss;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.*;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

import org.janelia.saalfeldlab.hotknife.IOHelper;
import org.janelia.saalfeldlab.hotknife.ops.*;

/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkConnectedComponents {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /path/to/input/predictions.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /path/to/output/connected_components.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. organelle")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5DatasetSuffix", required = false, usage = "N5 suffix, e.g. _cc so output would be organelle_cc")
		private String outputN5DatasetSuffix = "_cc";

		@Option(name = "--maskN5Path", required = true, usage = "mask N5 path, e.g. /path/to/input/mask.n5")
		private String maskN5Path = null;

		@Option(name = "--thresholdDistance", required = false, usage = "Distance for thresholding (positive inside, negative outside) (nm)")
		private double thresholdDistance = 0;
		
		@Option(name = "--minimumVolumeCutoff", required = false, usage = "Volume above which objects will be kept (nm^3)")
		private double minimumVolumeCutoff = 20E6;
		
		@Option(name = "--onlyKeepLargestComponent", required = false, usage = "Keep only the largest connected component")
		private boolean onlyKeepLargestComponent = false;

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

		public double getThresholdDistance() {
			return thresholdDistance;
		}
		
		public double getMinimumVolumeCutoff() {
			return minimumVolumeCutoff;
		}
		
		public boolean getOnlyKeepLargestComponent() {
			return onlyKeepLargestComponent;
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
	public static final <T extends NativeType<T>> List<BlockInformation> blockwiseConnectedComponents(
			final JavaSparkContext sc, final String inputN5Path, final String inputN5DatasetName,
			final String outputN5Path, final String outputN5DatasetName, final String maskN5PathName,
			final double thresholdIntensityCutoff, double minimumVolumeCutoff, List<BlockInformation> blockInformationList) throws IOException {
		
		//Do not find holes unless explicitly called for
		return blockwiseConnectedComponents(
				sc, inputN5Path, inputN5DatasetName,
				outputN5Path,  outputN5DatasetName,maskN5PathName,
				thresholdIntensityCutoff, minimumVolumeCutoff, blockInformationList, false, true);	
	}
	
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> List<BlockInformation> blockwiseConnectedComponents(
			final JavaSparkContext sc, final String inputN5Path, final String inputN5DatasetName,
			final String outputN5Path, final String outputN5DatasetName, final String maskN5PathName,
			final double thresholdIntensityCutoff, double minimumVolumeCutoff, List<BlockInformation> blockInformationList, boolean findHoles, boolean smooth) throws IOException {

		// Get attributes of input data set
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] blockSizeL = new long[] { blockSize[0], blockSize[1], blockSize[2] };
		final long[] outputDimensions = attributes.getDimensions();
		final double [] pixelResolution = IOHelper.getResolution(n5Reader, inputN5DatasetName);
				
		// Create output dataset
		final N5Writer n5Writer = new N5FSWriter(outputN5Path);
		n5Writer.createGroup(outputN5DatasetName);
		n5Writer.createDataset(outputN5DatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
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
			RandomAccessibleInterval<UnsignedByteType> sourceInterval = null;
			if(findHoles) {
				
				RandomAccessibleInterval<UnsignedLongType> connectedComponents = Views.offsetInterval(Views.extendZero(
						(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)
						),offset, dimension);
				
				sourceInterval = Converters.convert(connectedComponents,
						(a, b) -> b.set(a.getLong()==0 ? 255 : 0 ), new UnsignedByteType());
			/*	if(offset[0]==0 && offset[1]==360 && offset[2] ==0) {
					new ImageJ();
				ImageJFunctions.show(sourceInterval);
				}*/
				
			}
			else {
				if(smooth) {
					double [] sigma = new double[] {3,3,3};
					int[] sizes = Gauss3.halfkernelsizes( sigma );
					long padding = sizes[0];
					long [] paddedOffset = new long [] {offset[0]-padding,offset[1]-padding,offset[2]-padding};
					long [] paddedDimension = new long [] {dimension[0]+2*padding,dimension[1]+2*padding,dimension[2]+2*padding};
					RandomAccessibleInterval<UnsignedByteType> rawPredictions = Views.offsetInterval(Views.extendMirrorSingle(
							(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)
							),paddedOffset, paddedDimension);
					
					final Img<UnsignedByteType> smoothedPredictions =  new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType()).create(paddedDimension);	
					SimpleGaussRA<UnsignedByteType> gauss = new SimpleGaussRA<UnsignedByteType>(sigma);
					gauss.compute(rawPredictions, smoothedPredictions);
					//Gauss3.gauss(sigma, rawPredictions, smoothedPredictions);
					/*ImageJ ij = new ImageJ();
					ImageJFunctions.show(rawPredictions);
					ImageJFunctions.show(smoothedPredictions);*/
					sourceInterval = Views.offsetInterval(smoothedPredictions,new long[] {padding,padding,padding},dimension);
					
				}else {
					sourceInterval = Views.offsetInterval(Views.extendZero(
							(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)
							),offset, dimension);
				}

				if(maskN5PathName != null) {
					// Read in mask block
					final N5Reader n5MaskReaderLocal = new N5FSReader(maskN5PathName);
					final RandomAccessibleInterval<UnsignedByteType> mask = N5Utils.open(n5MaskReaderLocal,
							"/volumes/masks/foreground");
					final RandomAccessibleInterval<UnsignedByteType> maskInterval = Views.offsetInterval(Views.extendZero(mask),
							new long[] { offset[0] / 2, offset[1] / 2, offset[2] / 2 },
							new long[] { dimension[0] / 2, dimension[1] / 2, dimension[2] / 2 });
		
					// Mask out appropriate region in source block; need to do it this way rather
					// than converter since mask is half the size of source
					Cursor<UnsignedByteType> sourceCursor = Views.flatIterable(sourceInterval).cursor();
					RandomAccess<UnsignedByteType> maskRandomAccess = maskInterval.randomAccess();
					while (sourceCursor.hasNext()) {
						final UnsignedByteType voxel = sourceCursor.next();
						final long[] positionInMask = { (long) Math.floor(sourceCursor.getDoublePosition(0) / 2),
								(long) Math.floor(sourceCursor.getDoublePosition(1) / 2),
								(long) Math.floor(sourceCursor.getDoublePosition(2) / 2) };
						maskRandomAccess.setPosition(positionInMask);
						if (maskRandomAccess.get().getRealDouble() == 0) {
							voxel.setInteger(0);
						}
					}
				}
			}

			// Create the output based on the current dimensions
			long[] currentDimensions = { 0, 0, 0 };
			sourceInterval.dimensions(currentDimensions);
			final Img<UnsignedLongType> output = new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType())
					.create(currentDimensions);

			// Compute the connected components which returns the components along the block
			// edges, and update the corresponding blockInformation object
			int minimumVolumeCutoffInVoxels = (int) Math.ceil(minimumVolumeCutoff/Math.pow(pixelResolution[0],3));
			currentBlockInformation = computeConnectedComponents(currentBlockInformation, sourceInterval, output, outputDimensions,
					blockSizeL, offset, thresholdIntensityCutoff, minimumVolumeCutoffInVoxels);

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
			final JavaSparkContext sc, final String inputN5Path, final String inputN5DatasetName, double minimumVolumeCutoff,
			List<BlockInformation> blockInformationList) throws IOException {

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
			xPlane1 = Views.offsetInterval(Views.extendZero(source), new long[] { xOffset - 1, offset[1], offset[2] },
					new long[] { 1, dimension[1], dimension[2] });
			yPlane1 = Views.offsetInterval(Views.extendZero(source), new long[] { offset[0], yOffset - 1, offset[2] },
					new long[] { dimension[0], 1, dimension[2] });
			zPlane1 = Views.offsetInterval(Views.extendZero(source), new long[] { offset[0], offset[1], zOffset - 1 },
					new long[] { dimension[0], dimension[1], 1 });

			if (xOffset < sourceDimensions[0])
				xPlane2 = Views.offsetInterval(Views.extendZero(source), new long[] { xOffset, offset[1], offset[2] },
						new long[] { 1, dimension[1], dimension[2] });
			if (yOffset < sourceDimensions[1])
				yPlane2 = Views.offsetInterval(Views.extendZero(source), new long[] { offset[0], yOffset, offset[2] },
						new long[] { dimension[0], 1, dimension[2] });
			if (zOffset < sourceDimensions[2])
				zPlane2 = Views.offsetInterval(Views.extendZero(source), new long[] { offset[0], offset[1], zOffset },
						new long[] { dimension[0], dimension[1], 1 });

			// Calculate the set of object IDs that are touching and need to be merged
			Set<List<Long>> globalIDtoGlobalIDSet = new HashSet<>();
			getGlobalIDsToMerge(xPlane1, xPlane2, globalIDtoGlobalIDSet);
			getGlobalIDsToMerge(yPlane1, yPlane2, globalIDtoGlobalIDSet);
			getGlobalIDsToMerge(zPlane1, zPlane2, globalIDtoGlobalIDSet);

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
		long maxVolume = 0;
		Set<Long> maxVolumeObjectIDs = new HashSet<Long>();
		
		for (BlockInformation currentBlockInformation : blockInformationList) {
			Map<Long, Long> currentGlobalIDtoRootIDMap = new HashMap<Long, Long>();
			for (Long currentEdgeComponentID : currentBlockInformation.edgeComponentIDtoVolumeMap.keySet()) {
				Long rootID;
				rootID = unionFind.globalIDtoRootID.getOrDefault(currentEdgeComponentID, currentEdgeComponentID); // Need this check since not all edge objects will be connected to neighboring blocks
				currentGlobalIDtoRootIDMap.put(currentEdgeComponentID, rootID);
				rootIDtoVolumeMap.put(rootID, rootIDtoVolumeMap.getOrDefault(rootID, 0L) + currentBlockInformation.edgeComponentIDtoVolumeMap.get(currentEdgeComponentID));
				
			}
			currentBlockInformation.edgeComponentIDtoRootIDmap = currentGlobalIDtoRootIDMap;
			
			if(currentBlockInformation.selfContainedMaxVolume == maxVolume) {
				maxVolumeObjectIDs.addAll(currentBlockInformation.selfContainedMaxVolumeOrganelles);
			}
			else if(currentBlockInformation.selfContainedMaxVolume > maxVolume) {
				maxVolume = currentBlockInformation.selfContainedMaxVolume;
				maxVolumeObjectIDs.clear();
				maxVolumeObjectIDs.addAll(currentBlockInformation.selfContainedMaxVolumeOrganelles);
			}
			
		}
		
		for (Entry <Long,Long> e : rootIDtoVolumeMap.entrySet()) {
			Long rootID = e.getKey();
			Long volume = e.getValue();
			if(volume == maxVolume) {
				maxVolumeObjectIDs.add(rootID);
			}
			else if(volume > maxVolume) {
				maxVolume = volume;
				maxVolumeObjectIDs.clear();
				maxVolumeObjectIDs.add(rootID);
			}
		}
		
		
		
		int minimumVolumeCutoffInVoxels = (int) Math.ceil(minimumVolumeCutoff/Math.pow(pixelResolution[0],3));
		for (BlockInformation currentBlockInformation : blockInformationList) {
			for (Entry <Long,Long> e : currentBlockInformation.edgeComponentIDtoRootIDmap.entrySet()) {
				Long key = e.getKey();
				Long value = e.getValue();
				currentBlockInformation.edgeComponentIDtoRootIDmap.put(key, 
						rootIDtoVolumeMap.get(value) <= minimumVolumeCutoffInVoxels ? 0L : value);
			}
			currentBlockInformation.maxVolumeObjectIDs = maxVolumeObjectIDs;
		}
		
		

		return blockInformationList;
	}

	
	/**
	 * Merge touching objects by relabeling them to common root
	 *
	 * Reads in blockwise-connected component data and relabels touching objects
	 * using the block information map, writing the data to the final output n5
	 *
	 * @param sc
	 * @param inputN5Path
	 * @param datasetName
	 * @param inputN5DatasetName
	 * @param outputN5DatasetName
	 * @param blockInformationList
	 * @return
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> void mergeConnectedComponents(final JavaSparkContext sc,
			final String inputN5Path, final String inputN5DatasetName, final String outputN5DatasetName,
			final List<BlockInformation> blockInformationList) throws IOException {
			mergeConnectedComponents(sc,
				inputN5Path,inputN5DatasetName, outputN5DatasetName, false,
				blockInformationList);
	}
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> void mergeConnectedComponents(final JavaSparkContext sc,
			final String inputN5Path, final String inputN5DatasetName, final String outputN5DatasetName, boolean onlyKeepLargestComponent,
			final List<BlockInformation> blockInformationList) throws IOException {

		// Set up reader to get n5 attributes
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();

		// Set up writer for output
		final N5Writer n5Writer = new N5FSWriter(inputN5Path);
		n5Writer.createGroup(outputN5DatasetName);
		n5Writer.createDataset(outputN5DatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
		n5Writer.setAttribute(outputN5DatasetName, "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, inputN5DatasetName)));

		// Set up and run rdd to relabel objects and write out blocks
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(currentBlockInformation -> {
			// Get block-specific information
			final long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			final Map<Long, Long> edgeComponentIDtoRootIDmap = currentBlockInformation.edgeComponentIDtoRootIDmap;

			// Read in source data
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			final RandomAccessibleInterval<UnsignedLongType> sourceInterval = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)),
					offset, dimension);

			//Relabel objects
			Cursor<UnsignedLongType> sourceCursor = Views.flatIterable(sourceInterval).cursor();
			while (sourceCursor.hasNext()) {
				final UnsignedLongType voxel = sourceCursor.next();
				long currentValue = voxel.getLong();
				if(onlyKeepLargestComponent) {
					if(currentValue>0) {
						Long rootID = edgeComponentIDtoRootIDmap.getOrDefault(currentValue, currentValue); //either on edge, or contained internally
						if (currentBlockInformation.maxVolumeObjectIDs.contains(rootID)) {//then it is on edge
							voxel.setLong(rootID);	
						}
						else {
							voxel.setLong(0);
						}
					}
				}
				else {
					if (currentValue > 0 && edgeComponentIDtoRootIDmap.containsKey(currentValue)) {
						Long currentRoot = edgeComponentIDtoRootIDmap.get(currentValue);
						voxel.setLong(currentRoot);
					}
				}
			}

			//Write out block
			final N5Writer n5WriterLocal = new N5FSWriter(inputN5Path);
			N5Utils.saveBlock(sourceInterval, n5WriterLocal, outputN5DatasetName, gridBlock[2]);
		});

	}
	

	public static List<BlockInformation> buildBlockInformationList(final String inputN5Path,
			final String inputN5DatasetName) throws IOException {
		//Get block attributes
		N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();
		//final long[] outputDimensions = new long[] {501,501,501};

		//Build list
		List<long[][]> gridBlockList = Grid.create(outputDimensions, blockSize);
		List<BlockInformation> blockInformationList = new ArrayList<BlockInformation>();
		for (int i = 0; i < gridBlockList.size(); i++) {
			long[][] currentGridBlock = gridBlockList.get(i);
			blockInformationList.add(new BlockInformation(currentGridBlock, null, null));
		}
		return blockInformationList;
	}

	public static BlockInformation computeConnectedComponents(BlockInformation blockInformation, RandomAccessibleInterval<UnsignedByteType> sourceInterval,
			RandomAccessibleInterval<UnsignedLongType> output, long[] sourceDimensions, long[] outputDimensions,
			long[] offset, double thresholdIntensityCutoff, int minimumVolumeCutoff) {

		// Threshold sourceInterval using thresholdIntensityCutoff
		final RandomAccessibleInterval<BoolType> thresholded = Converters.convert(sourceInterval,
				(a, b) -> b.set(a.getRealDouble() >= thresholdIntensityCutoff), new BoolType());

		// Run connected component analysis, storing results in components
		final ArrayImg<UnsignedLongType, LongArray> components = ArrayImgs
				.unsignedLongs(Intervals.dimensionsAsLongArray(thresholded));
		ConnectedComponentAnalysis.connectedComponents(thresholded, components);

		// Cursors over output and components
		Cursor<UnsignedLongType> o = Views.flatIterable(output).cursor();
		final Cursor<UnsignedLongType> c = Views.flatIterable(components).cursor();

		// Relabel object ids with unique ids corresponding to a global voxel index
		// within the object, and store/return object ids on edge
		long[] defaultIDtoGlobalID = new long[(int) (outputDimensions[0] * outputDimensions[1] * outputDimensions[2])];
		Set<Long> edgeComponentIDs = new HashSet<>();
		Map<Long,Long> allComponentIDtoVolumeMap = new HashMap<>();
		while (o.hasNext()) {

			final UnsignedLongType tO = o.next();
			final UnsignedLongType tC = c.next();

			int defaultID = tC.getInteger();
			if (defaultID > 0) {
				// If the voxel is part of an object, set the corresponding output voxel to a
				// unique global ID

				if (defaultIDtoGlobalID[defaultID] == 0) {
					// Get global ID

					long[] currentVoxelPosition = { o.getIntPosition(0), o.getIntPosition(1), o.getIntPosition(2) };
					currentVoxelPosition[0] += offset[0];
					currentVoxelPosition[1] += offset[1];
					currentVoxelPosition[2] += offset[2];

					// Unique global ID is based on pixel index, +1 to differentiate it from
					// background
					long globalID = sourceDimensions[0] * sourceDimensions[1] * currentVoxelPosition[2]
							+ sourceDimensions[0] * currentVoxelPosition[1] + currentVoxelPosition[0] + 1;

					defaultIDtoGlobalID[defaultID] = globalID;
				}

				tO.setLong(defaultIDtoGlobalID[defaultID]);

				// Store ids of objects on the edge of a block
				if (o.getIntPosition(0) == 0 || o.getIntPosition(0) == outputDimensions[0] - 1
						|| o.getIntPosition(1) == 0 || o.getIntPosition(1) == outputDimensions[1] - 1
						|| o.getIntPosition(2) == 0 || o.getIntPosition(2) == outputDimensions[2] - 1) {
					edgeComponentIDs.add(defaultIDtoGlobalID[defaultID]);
				}
				allComponentIDtoVolumeMap.put(defaultIDtoGlobalID[defaultID], allComponentIDtoVolumeMap.getOrDefault(defaultIDtoGlobalID[defaultID],0L)+1);	
			}
		}
		Set<Long> allComponentIDs = allComponentIDtoVolumeMap.keySet();
		Set<Long> selfContainedObjectIDs = Sets.difference(allComponentIDs, edgeComponentIDs);
		
		final Map<Long,Long> edgeComponentIDtoVolumeMap =  edgeComponentIDs.stream()
		        .filter(allComponentIDtoVolumeMap::containsKey)
		        .collect(Collectors.toMap(Function.identity(), allComponentIDtoVolumeMap::get));
		
		final Map<Long,Long> selfContainedComponentIDtoVolumeMap =  selfContainedObjectIDs.stream()
		        .filter(allComponentIDtoVolumeMap::containsKey)
		        .collect(Collectors.toMap(Function.identity(), allComponentIDtoVolumeMap::get));
		
		o = Views.flatIterable(output).cursor();
		blockInformation.selfContainedMaxVolume = 0L;
		blockInformation.selfContainedMaxVolumeOrganelles = new HashSet<Long>();
		while (o.hasNext()) {
			final UnsignedLongType tO = o.next();
			if (selfContainedComponentIDtoVolumeMap.getOrDefault(tO.get(), Long.MAX_VALUE) <= minimumVolumeCutoff){
				tO.set(0);
			}
			else { //large enough to keep or on edge
				Long objectID = tO.get();
				if(objectID>0) {
					long objectVolume = selfContainedComponentIDtoVolumeMap.getOrDefault(objectID,0L);
					if(objectVolume > blockInformation.selfContainedMaxVolume) {
						blockInformation.selfContainedMaxVolume = objectVolume;
						blockInformation.selfContainedMaxVolumeOrganelles.clear();
						blockInformation.selfContainedMaxVolumeOrganelles.add(objectID);
					}
					else if(objectVolume == blockInformation.selfContainedMaxVolume){
						blockInformation.selfContainedMaxVolumeOrganelles.add(objectID);
					}
				}
			}
		}

		blockInformation.edgeComponentIDtoVolumeMap = edgeComponentIDtoVolumeMap;
		
		return blockInformation;
	}
	
	
	public static Map<Long,Long> computeConnectedComponents(RandomAccessibleInterval<UnsignedByteType> sourceInterval,
			RandomAccessibleInterval<UnsignedLongType> output, long[] sourceDimensions, long[] outputDimensions,
			long[] offset, double thresholdIntensityCutoff, int minimumVolumeCutoff) {

		BlockInformation blockInformation = new BlockInformation();
		blockInformation = computeConnectedComponents(blockInformation, sourceInterval,
				output,  sourceDimensions,  outputDimensions,
				offset, thresholdIntensityCutoff, minimumVolumeCutoff);

		return blockInformation.edgeComponentIDtoVolumeMap;
	}
	
	public static final void getGlobalIDsToMerge(RandomAccessibleInterval<UnsignedLongType> hyperSlice1,
			RandomAccessibleInterval<UnsignedLongType> hyperSlice2, Set<List<Long>> globalIDtoGlobalIDSet) {
		// The global IDS that need to be merged are those that are touching along the
		// hyperplane borders between adjacent blocks
		if (hyperSlice1 != null && hyperSlice2 != null) {
			Cursor<UnsignedLongType> hs1Cursor = Views.flatIterable(hyperSlice1).cursor();
			Cursor<UnsignedLongType> hs2Cursor = Views.flatIterable(hyperSlice2).cursor();
			while (hs1Cursor.hasNext()) {
				long hs1Value = hs1Cursor.next().getLong();
				long hs2Value = hs2Cursor.next().getLong();
				if (hs1Value > 0 && hs2Value > 0) {
					globalIDtoGlobalIDSet.add(Arrays.asList(hs1Value, hs2Value));// hs1->hs2 pair should always be
																					// distinct since hs1 is unique to
																					// first block
				}
			}

		}
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
	
	public static void standardConnectedComponentAnalysisWorkflow(SparkConf conf, String inputN5DatasetName, String inputN5Path, String maskN5Path, String outputN5Path, String outputN5DatasetSuffix, double thresholdDistance, double minimumVolumeCutoff, boolean onlyKeepLargestComponent ) throws IOException {
		// Get all organelles
		String[] organelles = { "" };
		double thresholdIntensityCutoff = 	128 * Math.tanh(thresholdDistance / 50) + 127;

		if (inputN5DatasetName!= null) {
			organelles = inputN5DatasetName.split(",");
		} else {
			File file = new File(inputN5Path);
			organelles = file.list(new FilenameFilter() {
				@Override
				public boolean accept(File current, String name) {
					return new File(current, name).isDirectory();
				}
			});
		}

		System.out.println(Arrays.toString(organelles));

		String tempOutputN5DatasetName = null;
		String finalOutputN5DatasetName = null;
		List<String> directoriesToDelete = new ArrayList<String>();
		for (String currentOrganelle : organelles) {
			logMemory(currentOrganelle);	
			tempOutputN5DatasetName = currentOrganelle + outputN5DatasetSuffix + "_blockwise_temp_to_delete";
			finalOutputN5DatasetName = currentOrganelle + outputN5DatasetSuffix;
			
			//Create block information list
			List<BlockInformation> blockInformationList = buildBlockInformationList(inputN5Path,
				currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
		
			if(currentOrganelle.equals("ribosomes") || currentOrganelle.equals("microtubules")) {
				minimumVolumeCutoff = 0;
			}
			blockInformationList = blockwiseConnectedComponents(sc, inputN5Path, currentOrganelle,
					outputN5Path, tempOutputN5DatasetName, maskN5Path,
					thresholdIntensityCutoff, minimumVolumeCutoff, blockInformationList);
			logMemory("Stage 1 complete");
			
			blockInformationList = unionFindConnectedComponents(sc, outputN5Path, tempOutputN5DatasetName, minimumVolumeCutoff,
					blockInformationList);
			logMemory("Stage 2 complete");
			
			mergeConnectedComponents(sc, outputN5Path, tempOutputN5DatasetName, finalOutputN5DatasetName, onlyKeepLargestComponent,blockInformationList);
			logMemory("Stage 3 complete");

			directoriesToDelete.add(outputN5Path + "/" + tempOutputN5DatasetName);
			
			sc.close();
		}

		//Remove temporary files
		SparkDirectoryDelete.deleteDirectories(conf, directoriesToDelete);
		logMemory("Stage 4 complete");

	}
	
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkConnectedComponents");
		
		standardConnectedComponentAnalysisWorkflow(conf, options.getInputN5DatasetName(), options.getInputN5Path(), options.getMaskN5Path(), options.getOutputN5Path(), options.getOutputN5DatasetSuffix(), options.getThresholdDistance(), options.getMinimumVolumeCutoff(), options.getOnlyKeepLargestComponent());

	}
}

