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
 * Binarize a segmented volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkBinarize {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;

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

	}

	/**
	 * 	Binarize segmented N5 volume
	 * 
	 * @param sc
	 * @param n5Path
	 * @param datasetName
	 * @param n5OutputPath
	 * @param blockInformationList
	 * @throws IOException
	 */
	public static final void binarize(
			final JavaSparkContext sc,
			final String n5Path,
			final String datasetName,
			final String n5OutputPath,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();		

		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		n5Writer.createDataset(
				datasetName + "_binarized",
				dimensions,
				blockSize,
				DataType.UINT8,
				new GzipCompression());
		n5Writer.setAttribute(datasetName + "_binarized", "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, datasetName)));

		//Blockwise binarize
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(blockInformation -> {
			final long [][] gridBlock = blockInformation.gridBlock;
			final N5Reader n5BlockReader = new N5FSReader(n5Path);

			@SuppressWarnings("unchecked")
			final RandomAccessibleInterval<UnsignedByteType> sourceBinarized = Converters.convert(
					(RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, datasetName),
					(a, b) -> {
						if(a.getIntegerLong()>0) {
							b.set(1);
						}
						else {
							b.set(0);
						}
					},
					new UnsignedByteType());
			
			final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
			N5Utils.saveBlock(Views.offsetInterval(Views.extendZero(sourceBinarized), gridBlock[0], gridBlock[1]), n5BlockWriter, datasetName + "_binarized", gridBlock[2]);
			
		
		
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
			
			binarize(
					sc,
					options.getInputN5Path(),
					options.getInputN5DatasetName(),
					options.getOutputN5Path(),
					blockInformationList);
			
			sc.close();
		}

	}
}
