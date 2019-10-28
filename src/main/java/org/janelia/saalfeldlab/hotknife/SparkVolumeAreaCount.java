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
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import net.imglib2.RandomAccessible;
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
public class SparkVolumeAreaCount {
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
	public static final <T extends NativeType<T>> void calculateVolumeAreaCount(
			final JavaSparkContext sc, final String inputN5Path, final String inputN5DatasetName,
			final double contactDistance, List<BlockInformation> blockInformationList) throws IOException {
		
		
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final long[] outputDimensions = attributes.getDimensions();
		final long outOfBoundsValue = outputDimensions[0]*outputDimensions[1]*outputDimensions[2]+1;
		// Set up rdd to parallelize over blockInformation list and run RDD, which will
		// return updated block information containing list of components on the edge of
		// the corresponding block
		// Set up reader to get n5 attributes
				
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			
			//extend by 1 in each dimension, necessary for checking 
			Arrays.setAll(offset, i->offset[i]-1);
			Arrays.setAll(dimension, i->dimension[i]+2);
			
			// Read in source block
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);	
			final RandomAccessibleInterval<UnsignedLongType> sourceInterval = Views.offsetInterval(Views.extendValue(
					(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName),new UnsignedLongType(outOfBoundsValue)), offset, dimension);
			final RandomAccess<UnsignedLongType> sourceRandomAccess = sourceInterval.randomAccess();
			
			Set<Long> allObjectIDs = new HashSet<>();
			Set<Long> edgeObjectIDs = new HashSet<>();
			long volumeCount=0;
			long surfaceAreaCount=0;
			for(long x=offset[0]; x<offset[0]+dimension[0]; x++) {
				for(long y=offset[1]; y<offset[1]+dimension[1]; y++) {
					for(long z=offset[2]; z<offset[2]+dimension[2]; z++) {
						sourceRandomAccess.setPosition(new long[] {x,y,z});
						long currentVoxelValue=sourceRandomAccess.get().get();
						if (currentVoxelValue >0  && currentVoxelValue != outOfBoundsValue ) {
							allObjectIDs.add(currentVoxelValue);
							volumeCount++;
							if((x==offset[0] || x==offset[0]+dimension[0]-1) || (y==offset[1] || y==offset[1]+dimension[1]-1) || (z==offset[2] || z==offset[2]+dimension[2]-1) ) {
								edgeObjectIDs.add(currentVoxelValue);
							}
						
							if(isSurfaceVoxel(sourceRandomAccess, outOfBoundsValue){
								surfaceAreaCount++;
							}
						}
					}
				}
			}
			
			
//			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
//			N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);
		});
	}
	
	public static boolean isSurfaceVoxel(final RandomAccess<UnsignedLongType> sourceRandomAccess, long outOfBoundsValue ) {
		long referenceVoxelValue = sourceRandomAccess.get().get();
		final long sourceRandomAccessPosition[] = {sourceRandomAccess.getLongPosition(0), sourceRandomAccess.getLongPosition(1), sourceRandomAccess.getLongPosition(2)};
				
		for(int x=-1; x<=1; x++) {
			for(int y=-1; y<=1; y++) {
				for(int z=-1; z<=1; z++) {
					if(!(x==0 && y==0 && z==0)) {
						final long currentPosition[] = {sourceRandomAccessPosition[0]+x, sourceRandomAccessPosition[1]+y, sourceRandomAccessPosition[2]+z};
						sourceRandomAccess.setPosition(currentPosition);
						if(sourceRandomAccess.get().get() != referenceVoxelValue && sourceRandomAccess.get().get() !=outOfBoundsValue) {
							return true;
						}
					}
				}
			}
		}
		
		return false;	
	
	}
	
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

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

		for (int i = 0; i<organelles.length; i++) {
			final String organelle1 =organelles[i];
			for(int j= i+1; j< organelles.length; j++) {
				System.out.println(Arrays.toString(organelles));
				final String organelle2 = organelles[j];

				List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), organelle1);
				JavaSparkContext sc = new JavaSparkContext(conf);
				
				final String organelleContactString = organelle1 + "_to_" + organelle2 + options.getOutputN5DatasetSuffix();
				final String tempOutputN5DatasetName = organelleContactString + "_cc_blockwise_temp_to_delete";
				final String finalOutputN5DatasetName = organelleContactString + "_cc";
				
				System.out.println(organelleContactString + " " + new SimpleDateFormat("HH:mm:ss").format(new Date()));
				

				sc.close();
			}
		}
		
		File file = new File(options.getOutputN5Path());
		final String [] directoriesToDelete = file.list(new FilenameFilter() {
			@Override
			public boolean accept(File current, String name) {
				return new File(current, name).isDirectory();
			}
		});
		
		for (String currentDirectoryToDelete:directoriesToDelete){
			if (currentDirectoryToDelete.contains("_temp_to_delete")){
				FileUtils.deleteDirectory(new File(options.getOutputN5Path() + "/" + currentDirectoryToDelete));
			}
		}
	}
}
