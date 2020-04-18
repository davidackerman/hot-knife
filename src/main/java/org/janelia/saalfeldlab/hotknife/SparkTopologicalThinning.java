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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
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

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * Class to do topological thinning, ie, skeletonization and medial surface finding. Default is skeletonization unless --doMedialSurface is true
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkTopologicalThinning {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /path/to/input/data.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /path/to/output/skeletonization.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. organelle")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5DatasetSuffix", required = false, usage = "N5 suffix, e.g. _skeleton so output would be organelle_skeleton")
		private String outputN5DatasetSuffix = "";
		
		@Option(name = "--doMedialSurface", required = false, usage = "Whether to do do medial surface, by default is set to false and skeletonization is performed")
		private Boolean doMedialSurface = false;

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
		
		public Boolean getDoMedialSurface() {
			return doMedialSurface;
		}

	}
	
	/**
	 * Perform a topological thinning operation.
	 *
	 * Takes in an input dataset path and name, an output dataset path and name, whether or not to do medial surface thinning and the current thinning iteration
	 *
	 * @param sc
	 * @param n5Path
	 * @param originalInputdatsetName
	 * @param n5OutputPath
	 * @param originalOutputDatasetName
	 * @param doMedialSurface
	 * @param blockInformationList
	 * @param iteration
	 * @throws IOException
	 */
	public static List<BlockInformation> performTopologicalThinningIteration(final JavaSparkContext sc, final String n5Path,
			final String originalInputDatasetName, final String n5OutputPath, String originalOutputDatasetName, boolean doMedialSurface,
			List<BlockInformation> blockInformationList, final int iteration) throws IOException {

		//For each iteration, create the correspondingly correct input/output datasets. The reason for this is that we do not want to overwrite data in one block that will be read in by another block that has yet to be processed
		final String inputDatasetName = originalOutputDatasetName+(iteration%2==0 ? "_odd" : "_even");
		final String outputDatasetName = originalOutputDatasetName+(iteration%2==0 ? "_even" : "_odd");
		
		N5Reader n5Reader = null;
		DatasetAttributes attributes = null;
		if(iteration == 0) {
			n5Reader = new N5FSReader(n5Path);
			attributes = n5Reader.getDatasetAttributes(originalInputDatasetName);
		}
		else {
			n5Reader = new N5FSReader(n5OutputPath);
			attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		}
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		
		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		n5Writer.createDataset(outputDatasetName, dimensions, blockSize, DataType.UINT8, new GzipCompression());
		n5Writer.setAttribute(outputDatasetName, "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, originalInputDatasetName)));

		
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<BlockInformation> updatedBlockInformation = rdd.map(blockInformation -> {
			
			//Get relevant block informtation
			final long[][] gridBlock = blockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			long [] paddedOffset = blockInformation.paddedGridBlock[0];
			long [] paddedDimension = blockInformation.paddedGridBlock[1];
			long [] padding = blockInformation.padding;
			
			//Read in input and create output
			N5Reader n5BlockReader = null;	
			IntervalView<UnsignedByteType> outputImage = null;
			IntervalView<UnsignedLongType> sourceCropped = null;
			if(iteration==0 ) {//Then this is the first thinning			
				//Get input from source, padded so that any thinning that will occur in the current block will have necessary context.
				//Create output image
				n5BlockReader = new N5FSReader(n5Path);
				RandomAccessibleInterval<UnsignedLongType> source = (RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, originalInputDatasetName);
				sourceCropped = Views.offsetInterval(Views.extendValue(source, new UnsignedLongType(0)), paddedOffset, paddedDimension);
				outputImage = Views.offsetInterval(ArrayImgs.unsignedBytes(paddedDimension),new long[]{0,0,0}, paddedDimension);

				//Initialize output image as binarized input
				final Cursor<UnsignedLongType> sourceCroppedCursor = sourceCropped.cursor();
				final Cursor<UnsignedByteType> outputImageCursor = outputImage.cursor();
				while(sourceCroppedCursor.hasNext()) {
					UnsignedLongType v1 = sourceCroppedCursor.next();
					UnsignedByteType v2 = outputImageCursor.next();
					if(v1.get()>0) {
						v2.set(1);
					}
				}
				
			}
			else {//Then has already done a thinning		
				//Input source is now the previously completed iteration image, and output is initialized to that
				n5BlockReader = new N5FSReader(n5OutputPath);
				final RandomAccessibleInterval<UnsignedByteType> source = (RandomAccessibleInterval<UnsignedByteType>)N5Utils.open(n5BlockReader, inputDatasetName);
				outputImage = Views.offsetInterval(Views.extendValue(source, new UnsignedByteType(0)), paddedOffset, paddedDimension);
			}
			
			
			//Create instance of Skeletonize3D_
			Skeletonize3D_ skeletonize3D = new Skeletonize3D_(outputImage, new int[]{(int) padding[0], (int) padding[1], (int) padding[2]}, new int[] {(int) paddedOffset[0],(int) paddedOffset[1], (int)paddedOffset[2]});
	
			//Assume we don't need to thin again and that the output image to write is the appropriately cropped version of outputImage
			boolean needToThinAgain = false;
			IntervalView<UnsignedByteType> croppedOutputImage = Views.offsetInterval(outputImage, padding, dimension);
			
			//For skeletonization and medial surface:
			//All blocks start off dependent, so it will only be independent after it made it through one iteration, ensuring all blocks are checked.
			//Perform thinning, then check if block is independent. If so, complete the block.
			if (doMedialSurface) {//Then do medial surface thinning
				if(!blockInformation.isIndependent) { 
					needToThinAgain = skeletonize3D.computeMedialSurfaceIteration();
					blockInformation.isIndependent = skeletonize3D.isMedialSurfaceBlockIndependent();
					if(blockInformation.isIndependent) {//then can finish it
						skeletonize3D = new Skeletonize3D_(croppedOutputImage, new int[]{0, 0, 0}, new int[] {(int) offset[0],(int) offset[1], (int)offset[2]});
						while(needToThinAgain) 
							needToThinAgain = skeletonize3D.computeMedialSurfaceIteration();
					}
				}
			}
			else {
				if(!blockInformation.isIndependent) {
					if(iteration==0)		//TODO: The below fix for touching objects by thinning them independently, therebey producing indpependent skeletons will fail in some extreme cases like if the objects are already thin/touching or still touching after an iteration because on the next iteration, they will be treated as one object
						needToThinAgain = thinEachObjectIndependently(sourceCropped, outputImage, padding, paddedOffset, paddedDimension, needToThinAgain ); //to prevent one skeleton being created for two distinct objects that are touching
					else 
						needToThinAgain = skeletonize3D.computeSkeletonIteration(); //just do normal thinning
					blockInformation.isIndependent = skeletonize3D.isSkeletonBlockIndependent();
					if(blockInformation.isIndependent) {//then can finish it
						skeletonize3D = new Skeletonize3D_(croppedOutputImage, new int[]{0, 0, 0}, new int[] {(int) offset[0],(int) offset[1], (int)offset[2]});
						while(needToThinAgain) 
							needToThinAgain = skeletonize3D.computeSkeletonIteration();
					}
				}
			}

			//Write out current thinned block and return block information updated with whether it needs to be thinned again
			final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
			N5Utils.saveBlock(croppedOutputImage, n5BlockWriter, outputDatasetName, gridBlock[2]);
			
			blockInformation.needToThinAgainCurrent = needToThinAgain;
			return blockInformation;
			
		});
		
		//Figure out whether another thinning iteration is needed. Update blockInformationList
		boolean needToThinAgain = false;
		blockInformationList  = new LinkedList<BlockInformation>(updatedBlockInformation.collect());
		for(int i=blockInformationList.size()-1; i>=0; i--) {
			BlockInformation currentBlockInformation = blockInformationList.get(i);
			
			//If a block is completed it needs to appear in both the _even and _odd outputs, otherwise update it and process again
			if(currentBlockInformation.isIndependent && (!currentBlockInformation.needToThinAgainPrevious && !currentBlockInformation.needToThinAgainCurrent)) {// if current block is independent and had no need to thin over two iterations, then can stop processing it since it will be identical in even/odd outputs
				blockInformationList.remove(i);
			}
			else {
				needToThinAgain |= currentBlockInformation.needToThinAgainCurrent;
				currentBlockInformation.needToThinAgainPrevious = currentBlockInformation.needToThinAgainCurrent;
				blockInformationList.set(i,currentBlockInformation);
			}
			
		}
		
			if(!needToThinAgain)
			blockInformationList = new LinkedList<BlockInformation>();
		
		return blockInformationList;
	}

	private static boolean thinEachObjectIndependently(IntervalView<UnsignedLongType> sourceCropped, IntervalView<UnsignedByteType> outputImage, long [] padding, long [] paddedOffset, long [] paddedDimension, boolean needToThinAgain ){
		Cursor<UnsignedLongType> sourceCroppedCursor = sourceCropped.cursor();
		RandomAccess<UnsignedByteType> outputImageRandomAccess = outputImage.randomAccess();
		
		Set<Long> objectIDsInBlock = new HashSet<Long>();
		while(sourceCroppedCursor.hasNext()) {
			long objectID = sourceCroppedCursor.next().get();
			int [] pos = new int [] {sourceCroppedCursor.getIntPosition(0), sourceCroppedCursor.getIntPosition(1), sourceCroppedCursor.getIntPosition(2)};
			outputImageRandomAccess.setPosition(pos);
			outputImageRandomAccess.get().set(0);
			if (objectID >0)
				objectIDsInBlock.add(objectID);
		}
		
		IntervalView<UnsignedByteType> current = null;
		Cursor<UnsignedByteType> currentCursor = null;
		for(long objectID : objectIDsInBlock) {
			sourceCroppedCursor.reset();
			current = Views.offsetInterval(ArrayImgs.unsignedBytes(paddedDimension),new long[]{0,0,0}, paddedDimension);
			currentCursor = current.cursor();
			while(sourceCroppedCursor.hasNext()) { //initialize to only look at current object
				sourceCroppedCursor.next();
				currentCursor.next();
				if(sourceCroppedCursor.get().get() == objectID)
					currentCursor.get().set(1);
			}
			
			Skeletonize3D_ skeletonize3D = new Skeletonize3D_(current, new int[]{(int) padding[0], (int) padding[1], (int) padding[2]}, new int[] {(int) paddedOffset[0],(int) paddedOffset[1], (int)paddedOffset[2]});
			needToThinAgain |= skeletonize3D.computeSkeletonIteration();
			
			//update output
			currentCursor.reset();
			while(currentCursor.hasNext()) {
				currentCursor.next();
				if(currentCursor.get().get() >0) {
					int [] pos = new int [] {currentCursor.getIntPosition(0), currentCursor.getIntPosition(1), currentCursor.getIntPosition(2)};
					outputImageRandomAccess.setPosition(pos);
					outputImageRandomAccess.get().set(1);
				}
			}
		}
		
		return needToThinAgain;
		
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
		List<BlockInformation> blockInformationList = new LinkedList<BlockInformation>();
		for (int i = 0; i < gridBlockList.size(); i++) {
			long pad = 50;//I think for doing 6 borders (N,S,E,W,U,B) where we do the 8 indpendent iterations, the furthest a voxel in a block can be affected is from something 48 away, so add 2 more just as extra border
			long[][] currentGridBlock = gridBlockList.get(i);
			long[][] paddedGridBlock = { {currentGridBlock[0][0]-pad, currentGridBlock[0][1]-pad, currentGridBlock[0][2]-pad}, //initialize padding
										{currentGridBlock[1][0]+2*pad, currentGridBlock[1][1]+2*pad, currentGridBlock[1][2]+2*pad}};
			long [] padding = {pad, pad, pad};
			blockInformationList.add(new BlockInformation(currentGridBlock, paddedGridBlock, padding, null, null));
		}
		return blockInformationList;
	}

	

	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkTopologicalThinning");

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
			finalOutputN5DatasetName = currentOrganelle + options.getOutputN5DatasetSuffix();

			// Create block information list
			List<BlockInformation> blockInformationList = buildBlockInformationList(options.getInputN5Path(), currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
			int fullIterations = 0;
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
						
			while(blockInformationList.size()>0){ //Thin until block information list is empty
				blockInformationList = performTopologicalThinningIteration(sc, options.getInputN5Path(), currentOrganelle, options.getOutputN5Path(),
							finalOutputN5DatasetName, options.getDoMedialSurface(), blockInformationList, fullIterations);
				fullIterations++;
				Date date = new Date();
				System.out.println(dateFormat.format(date)+" Number of Remaining Blocks: "+blockInformationList.size()+", Full iteration complete: "+fullIterations);
			}
			
			String finalFileName = finalOutputN5DatasetName + '_'+ ((fullIterations-1)%2==0 ? "even" : "odd");
			FileUtils.deleteDirectory(new File(options.getOutputN5Path() + "/" + finalOutputN5DatasetName));
			FileUtils.moveDirectory(new File(options.getOutputN5Path() + "/" + finalFileName), new File(options.getOutputN5Path() + "/" + finalOutputN5DatasetName));
			sc.close();
		}

		// Remove temporary files
		for (String currentOrganelle : organelles) {
			tempOutputN5DatasetName = currentOrganelle + options.getOutputN5DatasetSuffix()+ "_even";
			FileUtils.deleteDirectory(new File(options.getOutputN5Path() + "/" + tempOutputN5DatasetName));
			tempOutputN5DatasetName = currentOrganelle + options.getOutputN5DatasetSuffix()+ "_odd";
			FileUtils.deleteDirectory(new File(options.getOutputN5Path() + "/" + tempOutputN5DatasetName));
		}

	}
}
