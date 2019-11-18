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
import java.util.concurrent.ExecutionException;

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

import bdv.labels.labelset.Label;
import ij.ImageJ.*;
import ij.IJ;
import ij.ImagePlus;
import net.imagej.ImageJ;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.NativeImg;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.BooleanArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.label.Test;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.SubsampleIntervalView;
import net.imglib2.view.Views;
import net.imagej.ops.morphology.thin.*;
import net.imglib2.type.logic.BitType;

import ij.plugin.*;

/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkSkeletonization {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5DatasetSuffix", required = false, usage = "N5 suffix, e.g. _cc so output would be /mito_cc")
		private String outputN5DatasetSuffix = "_cc";

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

	}

	

	public static final void tempPerformThinning(final JavaSparkContext sc, final String n5Path,
			final String datasetName, final String n5OutputPath, final String outputDatasetName,
			final long[] initialPadding, final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final int n = dimensions.length;

		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		n5Writer.createDataset(outputDatasetName, dimensions, blockSize, DataType.UINT8, new GzipCompression());

		/*
		 * grid block size for parallelization to minimize double loading of blocks
		 */
		boolean show = true;
		if (show)
			new ij.ImageJ();
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(blockInformation -> {
			final long[][] gridBlock = blockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			
			long [] paddedOffset = {offset[0]-1, offset[1]-1, offset[2]-1};
			long [] paddedDimension = {dimension[0]+1, dimension[1]+1, dimension[2]+1};
			
			
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			final RandomAccessibleInterval<UnsignedLongType> source = (RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, datasetName);
			final IntervalView<UnsignedLongType> inputForSkeletonization = Views.offsetInterval(
					Views.extendValue(source, new UnsignedLongType(1)),
					paddedOffset, paddedDimension);
			
			ImagePlusImg< UnsignedByteType, ? > intermediateOutputImage = new ImagePlusImgFactory<>( new UnsignedByteType() ).create( paddedDimension  );
			final Cursor<UnsignedByteType> intermediateOutputImageCursor = intermediateOutputImage.cursor();
			
			final Cursor<UnsignedLongType> inputForSkeletonizationCursor = inputForSkeletonization.cursor();

			int objectVoxelCount = 0;
			while(inputForSkeletonizationCursor.hasNext()) {
				UnsignedLongType v1 = inputForSkeletonizationCursor.next();
				UnsignedByteType v2 = intermediateOutputImageCursor.next();
				if(v1.getLong()>0) {
					v2.set(255);
					objectVoxelCount++;
				}
			}
			if (show)
				intermediateOutputImage.getImagePlus().show();
			
			boolean needToThinAgain = false;
			if (objectVoxelCount == 0 || objectVoxelCount == dimensions[0]*dimensions[1]*dimensions[2]) {
				//cant thin yet;
				needToThinAgain = true;
			}
			else {
				Skeletonize3D_ skeletonize3D = new Skeletonize3D_();
				needToThinAgain = skeletonize3D.thinningForParallelization(intermediateOutputImage.getImagePlus());
			}
			if (show)
				intermediateOutputImage.getImagePlus().show();
			
			final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);

			Views.offsetInterval((RandomAccessible<UnsignedByteType>) intermediateOutputImage.getImagePlus(), new long[]{1,1,1},dimension);
			N5Utils.saveBlock(intermediateOutputImage, n5BlockWriter, outputDatasetName, gridBlock[2]);
		});
	}

	
	public static List<BlockInformation> buildBlockInformationList(final String inputN5Path,
			final String inputN5DatasetName) throws IOException {
		// Get block attributes
		N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();

		// Build list
		List<long[][]> gridBlockList = Grid.create(outputDimensions, blockSize);
		List<BlockInformation> blockInformationList = new ArrayList<BlockInformation>();
		for (int i = 0; i < gridBlockList.size(); i++) {
			long[][] currentGridBlock = gridBlockList.get(i);
			blockInformationList.add(new BlockInformation(currentGridBlock, null, null));
		}
		return blockInformationList;
	}

	

	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {
		System.setProperty("plugins.dir", "/groups/scicompsoft/home/ackermand/Fiji.app/plugins/");

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkConnectedComponents");

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

		String tempOutputN5DatasetName = null;
		String finalOutputN5DatasetName = null;
		for (String currentOrganelle : organelles) {
			tempOutputN5DatasetName = currentOrganelle + options.getOutputN5DatasetSuffix()
					+ "_blockwise_temp_to_delete";
			finalOutputN5DatasetName = currentOrganelle + options.getOutputN5DatasetSuffix();

			// Create block information list
			List<BlockInformation> blockInformationList = buildBlockInformationList(options.getInputN5Path(),
					currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);

			tempPerformThinning(sc, options.getInputN5Path(), currentOrganelle, options.getOutputN5Path(),
					"skeletonize", new long[] { 16, 16, 16 }, blockInformationList);

			sc.close();
		}

		// Remove temporary files
		for (String currentOrganelle : organelles) {
			tempOutputN5DatasetName = currentOrganelle + options.getOutputN5DatasetSuffix()
					+ "_blockwise_temp_to_delete";
			FileUtils.deleteDirectory(new File(options.getOutputN5Path() + "/" + tempOutputN5DatasetName));
		}

	}
}