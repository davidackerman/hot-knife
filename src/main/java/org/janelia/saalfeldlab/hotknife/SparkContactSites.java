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

import org.apache.commons.io.FileUtils;
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

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5DatasetSuffix", required = false, usage = "N5 suffix, e.g. _cc so output would be /mito_cc")
		private String outputN5DatasetSuffix = "";

		@Option(name = "--maskN5Path", required = true, usage = "mask N5 path, e.g. /groups/cosem/cosem/data/HeLa_Cell3_4x4x4nm/HeLa_Cell3_4x4x4nm.n5")
		private String maskN5Path = null;

		@Option(name = "--contactDistance", required = false, usage = "Distance for contact site")
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
	public static final <T extends NativeType<T>> void calculateContactSites(
			final JavaSparkContext sc, final String inputN5Path, final String organelle1, final String organelle2,
			final String outputN5Path, final String outputN5DatasetName, final String maskN5PathName,
			final double contactDistance, List<BlockInformation> blockInformationList) throws IOException {
				
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
			try{
			final RandomAccessibleInterval<FloatType> sourceIntervalOrganelle1 = Converters.convert(
					(RandomAccessibleInterval<UnsignedByteType>)Views.offsetInterval(Views.extendZero(
							(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(n5ReaderLocal, organelle1)),
							offset, dimension),
					(a, b) -> {
						final double x = (a.getInteger() - 127) / 128.0;
						final double d = 25 * Math.log((1 + x) / (1 - x));
						b.set((float)d);
					},
					new FloatType());
			
			final RandomAccessibleInterval<FloatType> sourceIntervalOrganelle2 = Converters.convert(
					(RandomAccessibleInterval<UnsignedByteType>)Views.offsetInterval(Views.extendZero(
							(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(n5ReaderLocal, organelle2)),
							offset, dimension),
					(a, b) -> {
						final double x = (a.getInteger() - 127) / 128.0;
						final double d = 25 * Math.log((1 + x) / (1 - x));
						b.set((float)d);
					},
					new FloatType());

			// Read in mask block
			final N5Reader n5MaskReaderLocal = new N5FSReader(maskN5PathName);
			final RandomAccessibleInterval<UnsignedByteType> mask = N5Utils.open(n5MaskReaderLocal,
					"/volumes/masks/foreground");
			final RandomAccessibleInterval<UnsignedByteType> maskInterval = Views.offsetInterval(Views.extendZero(mask),
					new long[] { offset[0] / 2, offset[1] / 2, offset[2] / 2 },
					new long[] { dimension[0] / 2, dimension[1] / 2, dimension[2] / 2 });

			// Mask out appropriate region in source block; need to do it this way rather
			// than converter since mask is half the size of source
			final Cursor<FloatType> sourceCursorOrganelle1 = Views.flatIterable(sourceIntervalOrganelle1).cursor();
			final Cursor<FloatType> sourceCursorOrganelle2 = Views.flatIterable(sourceIntervalOrganelle2).cursor();
			final RandomAccess<UnsignedByteType> maskRandomAccess = maskInterval.randomAccess();
			
			// Create the output based on the current dimensions
			long[] currentDimensions = { 0, 0, 0 };
			sourceIntervalOrganelle1.dimensions(currentDimensions);
			final Img<UnsignedByteType> output = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType())
					.create(currentDimensions);
			final Cursor<UnsignedByteType> outputCursor = Views.flatIterable(output).cursor();
			/*new ImageJ();
			ImageJFunctions.show((RandomAccessibleInterval<UnsignedByteType>)Views.offsetInterval(
					(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(n5ReaderLocal, organelle2),
					offset, dimension));
			ImageJFunctions.show(sourceIntervalOrganelle2);*/
			while (sourceCursorOrganelle1.hasNext()) {
				final FloatType voxelOrganelle1 = sourceCursorOrganelle1.next();
				final FloatType voxelOrganelle2 = sourceCursorOrganelle2.next();
				final UnsignedByteType voxelOutput = outputCursor.next();
				final long [] positionInMask = new long[3];
				Arrays.setAll(positionInMask, i -> (long) Math.floor(sourceCursorOrganelle1.getDoublePosition(i) / 2));
				maskRandomAccess.setPosition(positionInMask);
				if (maskRandomAccess.get().get() > 0){
					if ((voxelOrganelle1.get() < 0 && voxelOrganelle2.get() < 0 ) && Math.abs(voxelOrganelle1.get())<=contactDistance && Math.abs(voxelOrganelle2.get()) <= contactDistance) {
						voxelOutput.set(1);
					}
				}
			}

			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);
			
		}
		catch(Exception e){
			System.out.println(Arrays.toString(gridBlock));
			throw e;
		}
		});
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

		List<String> directoriesToDelete = new ArrayList<String>();
		for (int i = 0; i<organelles.length; i++) {
			final String organelle1 =organelles[i];
			for(int j= i+1; j< organelles.length; j++) {
				System.out.println(Arrays.toString(organelles));
				final String organelle2 = organelles[j];

				List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), organelle1);
				JavaSparkContext sc = new JavaSparkContext(conf);
				
				final String organelleContactString = organelle1 + "_to_" + organelle2 + options.getOutputN5DatasetSuffix();
				final String tempOutputN5ContactSites = organelleContactString + "_cc_blockwise_temp_to_delete";
				final String tempOutputN5ConnectedComponents = organelleContactString+"_temp_to_delete";
				final String finalOutputN5DatasetName = organelleContactString + "_cc";
				
				System.out.println(organelleContactString + " " + new SimpleDateFormat("HH:mm:ss").format(new Date()));
				
				calculateContactSites(sc, options.getInputN5Path(), organelle1, organelle2, options.getOutputN5Path(), tempOutputN5ConnectedComponents, options.getMaskN5Path(),
							options.getContactDistance(), blockInformationList);
				
				System.out.println("Stage 1 Complete: " + new SimpleDateFormat("HH:mm:ss").format(new Date()));
				int minimumVolumeCutoff = 100;
				blockInformationList = SparkConnectedComponents.blockwiseConnectedComponents(sc, options.getOutputN5Path(), tempOutputN5ConnectedComponents,
						options.getOutputN5Path(), tempOutputN5ContactSites, options.getMaskN5Path(),
						1, minimumVolumeCutoff, blockInformationList);				
				
				System.out.println("Stage 2 Complete: " + new SimpleDateFormat("HH:mm:ss").format(new Date()));

				blockInformationList = SparkConnectedComponents.unionFindConnectedComponents(sc, options.getOutputN5Path(), tempOutputN5ContactSites, minimumVolumeCutoff,
						blockInformationList);
				
				System.out.println("Stage 3 Complete: " + new SimpleDateFormat("HH:mm:ss").format(new Date()));
				
				SparkConnectedComponents.mergeConnectedComponents(sc, options.getOutputN5Path(), tempOutputN5ContactSites, finalOutputN5DatasetName,
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
