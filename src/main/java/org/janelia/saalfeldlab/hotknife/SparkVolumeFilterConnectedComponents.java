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
public class SparkVolumeFilterConnectedComponents {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;
		
		@Option(name = "--minimumVolumeCutoff", required = false, usage = "Volume above which objects will be kept")
		private double minimumVolumeCutoff = 20E6;
		
		@Option(name = "--idsToKeep", required = false, usage = "Volume above which objects will be kept")
		private String idsToKeep = "";

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
		
		public double getMinimumVolumeCutoff() {
			return minimumVolumeCutoff;
		}
	
		public String getIDsToKeep() {
			return idsToKeep;
		}

	}
	public static final <T extends NativeType<T>> void volumeFilterConnectedComponents(
			final JavaSparkContext sc, final String inputN5Path, final String inputN5DatasetName, final String outputN5DatasetName, double minimumVolumeCutoff,
			List<BlockInformation> blockInformationList) throws IOException {
		volumeFilterConnectedComponents(sc, inputN5Path, inputN5DatasetName, outputN5DatasetName, minimumVolumeCutoff, new HashSet<Long>(), blockInformationList);
	}
	
	public static final <T extends NativeType<T>> void volumeFilterConnectedComponents(
			final JavaSparkContext sc, final String inputN5Path, final String inputN5DatasetName, final String outputN5DatasetName, double minimumVolumeCutoff, Set<Long> idsToKeep,
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
						if(finalObjectIDtoVolumeMap.get(objectID) <= minimumVolumeCutoffInVoxels && !idsToKeep.contains(objectID)) {
							voxel.set(0);
						}
					}
					// Write out output to temporary n5 stack
					final N5Writer n5WriterLocal = new N5FSWriter(inputN5Path);
					N5Utils.saveBlock(objects, n5WriterLocal, outputN5DatasetName, gridBlock[2]);
				});
		
	}


	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkVolumeFilterConnectedComponents");

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

		for (String currentOrganelle : organelles) {
			// Create block information list
			List<BlockInformation> blockInformationList = SparkConnectedComponents
								.buildBlockInformationList(options.getInputN5Path(), currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			Set<Long> idsToKeep = new HashSet<Long>();
			if(!options.getIDsToKeep().isEmpty())
				for(String s :  Arrays.asList(options.getIDsToKeep().split(","))) idsToKeep.add(Long.valueOf(s));
			
			volumeFilterConnectedComponents(sc, options.getInputN5Path(), currentOrganelle, currentOrganelle + "_volumeFiltered", options.getMinimumVolumeCutoff(), idsToKeep, blockInformationList);
			sc.close();
		}
	}
}
