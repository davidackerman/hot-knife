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

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.view.Views;

/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkCrop {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--n5PathToCropTo", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String n5PathToCropTo = null;
		
		@Option(name = "--datasetNameToCropTo", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String datasetNameToCropTo = null;
		
		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;
		
		@Option(name = "--outputN5DatasetSuffix", required = true, usage = "N5 dataset, e.g. /mito")
		private String outputN5DatasetSuffix = "";
		
		@Option(name = "--convertTo8Bit", required = false, usage = "whether to convert to 8 bit")
		private boolean convertTo8Bit = false;
		
		@Option(name = "--zyxTOxyz", required = false, usage = "switch from zyx order to xyz order")
		private boolean zyxTOxyz = false;

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

		public String getN5PathToCropTo() {
			return n5PathToCropTo;
		}
		
		public String getDatasetNameToCropTo() {
			return datasetNameToCropTo;
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
		
		public boolean getConvertTo8Bit() {
			return convertTo8Bit;
		}
		
		public boolean getZyxTOxyz() {
			return zyxTOxyz;
		}
		
		public String getOutputN5DatasetSuffix() {
			return outputN5DatasetSuffix;
		}
		
		
		

	}

	public static final void crop(
			final JavaSparkContext sc,
			final String n5PathToCropTo,
			final String datasetNameToCropTo,
			final String inputN5Path,
			final String inputN5DatasetName,
			final String outputN5DatasetName,
			final String outputN5Path,
			final boolean convertTo8Bit,
			final boolean zyxTOxyz) throws IOException {

		final N5Reader n5ToCropToReader = new N5FSReader(n5PathToCropTo);
		final DatasetAttributes attributesToCropTo = n5ToCropToReader.getDatasetAttributes(datasetNameToCropTo);
		final long[] dimensions = attributesToCropTo.getDimensions();
		final int[] blockSize = attributesToCropTo.getBlockSize();		
		final int[] offsetsToCropTo = IOHelper.getOffset(n5ToCropToReader, datasetNameToCropTo);		
		if(zyxTOxyz) {
			int z = offsetsToCropTo[0];
			int x = offsetsToCropTo[2];
			offsetsToCropTo[0]=x;
			offsetsToCropTo[2]=z;
		}
		
		final double[] pixelResolutionToCropTo = IOHelper.getResolution(n5ToCropToReader,datasetNameToCropTo);
		int[] offsetsToCropToInVoxels = new int[] {(int) (offsetsToCropTo[0]/pixelResolutionToCropTo[0]),(int) (offsetsToCropTo[1]/pixelResolutionToCropTo[1]),(int) (offsetsToCropTo[2]/pixelResolutionToCropTo[2])};
		
		

		final N5Writer n5Writer = new N5FSWriter(outputN5Path);

		n5Writer.createDataset(
					outputN5DatasetName,
					dimensions,
					blockSize,
					convertTo8Bit ? DataType.UINT8 : DataType.UINT64,
					new GzipCompression());
		
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		n5Writer.setAttribute(outputN5DatasetName, "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, inputN5DatasetName)));
		n5Writer.setAttribute(outputN5DatasetName, "offset", offsetsToCropTo);
		
		List<BlockInformation> blockInformationListRefinedPredictions = BlockInformation.buildBlockInformationList(dimensions, blockSize);
		JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationListRefinedPredictions);			
		rdd.foreach(blockInformation -> {
			final long [] offset= new long [] {
					blockInformation.gridBlock[0][0]+offsetsToCropToInVoxels[0],
					blockInformation.gridBlock[0][1]+offsetsToCropToInVoxels[1],
					blockInformation.gridBlock[0][2]+offsetsToCropToInVoxels[2]
			};

			final long [] dimension = blockInformation.gridBlock[1];
			
			
			final N5Reader n5BlockReader = new N5FSReader(inputN5Path);
			final RandomAccessibleInterval<UnsignedLongType> source = Views.offsetInterval((RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, inputN5DatasetName), offset, dimension);
			
			final N5FSWriter n5BlockWriter = new N5FSWriter(outputN5Path);

			if(convertTo8Bit) {
				final RandomAccessibleInterval<UnsignedByteType> sourceConverted = Converters.convert(
						source,
						(a, b) -> {
							b.set( a.getIntegerLong()>0 ? 255 : 0);
						},
						new UnsignedByteType());
				N5Utils.saveBlock(sourceConverted, n5BlockWriter, outputN5DatasetName, blockInformation.gridBlock[2]);
			}
			else {
				N5Utils.saveBlock(source, n5BlockWriter, outputN5DatasetName, blockInformation.gridBlock[2]);
			}
		});
	}

	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkCrop");
		String [] inputN5DatasetNames = options.getInputN5DatasetName().split(",");
		
		for (String currentDatasetName : inputN5DatasetNames) {
			
			//Create block information list
			
			JavaSparkContext sc = new JavaSparkContext(conf);
			crop(sc, 
				options.getN5PathToCropTo(),
				options.getDatasetNameToCropTo(),
				options.getInputN5Path(),
				currentDatasetName,
				currentDatasetName+options.getOutputN5DatasetSuffix(),
				options.getOutputN5Path(),
				options.getConvertTo8Bit(),
				options.getZyxTOxyz());
			
			sc.close();
		}

	}
}
