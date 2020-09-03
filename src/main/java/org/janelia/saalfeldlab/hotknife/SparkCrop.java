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
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.type.numeric.real.DoubleType;
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
public class SparkCrop {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;
		
		@Option(name = "--dimensions", required = false, usage = "N5 dataset, e.g. /mito")
		private String dimensions = null;
		
		@Option(name = "--offsets", required = false, usage = "N5 dataset, e.g. /mito")
		private String offsets = null;

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

		public String getOutputN5Path() {
			return outputN5Path;
		}
		
		public String getOffsets() {
			return offsets;
		}
		
		public String getDimensions() {
			return dimensions;
		}

	}

	public static final void crop(
			final JavaSparkContext sc,
			final String n5PathToCropTo,
			final String datasetNameToCropTo,
			final String n5Path,
			final String datasetName,
			final String n5OutputPath,
			final boolean convertTo8Bit,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5PathToCropTo);
		final DatasetAttributes attributesToCropTo = n5Reader.getDatasetAttributes(datasetNameToCropTo);
		final long[] dimensions = attributesToCropTo.getDimensions();
		final int[] blockSize = attributesToCropTo.getBlockSize();		
		final int[] offsetsToCropTo = IOHelper.getOffset(n5Reader, datasetNameToCropTo);		
		final int[] pixelResolutionToCropTo = IOHelper.getOffset(n5Reader,datasetNameToCropTo);
		int[] offsetsToCropToInVoxels = new int[] {offsetsToCropTo[0]/pixelResolutionToCropTo[0],offsetsToCropTo[1]/pixelResolutionToCropTo[1],offsetsToCropTo[2]/pixelResolutionToCropTo[2]};

		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);

		n5Writer.createDataset(
					datasetName,
					dimensions,
					blockSize,
					convertTo8Bit ? DataType.UINT8 : DataType.UINT64,
					new GzipCompression());
		
		n5Writer.setAttribute(datasetName, "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, datasetName)));
		n5Writer.setAttribute(datasetName, "offset", offsetsToCropTo);

		/*
		 * grid block size for parallelization to minimize double loading of
		 * blocks
		 */
		List<BlockInformation> blockInformationListRefinedPredictions = BlockInformation.buildBlockInformationList(dimensions, blockSize);
		JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationListRefinedPredictions);			
		rdd.foreach(blockInformation -> {
			final long [] dimension = blockInformation.gridBlock[1];
			final long [] offset= new long [] {blockInformation.gridBlock[0][0]+offsetsToCropToInVoxels[0],
					blockInformation.gridBlock[0][1]+offsetsToCropToInVoxels[1],
					blockInformation.gridBlock[0][2]+offsetsToCropToInVoxels[2]
			};
			
			final RandomAccessibleInterval<UnsignedLongType> source = Views.offsetInterval((RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5Reader, datasetName), offset, dimension);
			final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
			
			if(convertTo8Bit) {
				final RandomAccessibleInterval<UnsignedByteType> sourceConverted = Converters.convert(
						source,
						(a, b) -> {
							b.set( a.getIntegerLong()>0 ? 255 : 0);
						},
						new UnsignedByteType());
				N5Utils.saveBlock(sourceConverted, n5BlockWriter, datasetName, blockInformation.gridBlock[2]);
			}
			else {
				N5Utils.saveBlock(source, n5BlockWriter, datasetName, blockInformation.gridBlock[2]);
			}
		});
	}

	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkBinarize");

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
			
			//Create block information list
			List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(),
				currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			crop(
					sc,
					options.getInputN5Path(),
					currentOrganelle,
					options.getOutputN5Path(),
					blockInformationList);
			
			sc.close();
		}

	}
}
