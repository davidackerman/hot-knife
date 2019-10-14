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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.hotknife.ops.ConnectedComponentsOp;
import org.janelia.saalfeldlab.hotknife.util.Grid;
import org.janelia.saalfeldlab.hotknife.BlockInformationPair;
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
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.view.Views;

/**
 * Connected components on entire stack
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkConnectedComponents {

	final static public String ownerFormat = "%s/owner/%s";
	final static public String stackListFormat = ownerFormat + "/stacks";
	final static public String stackFormat = ownerFormat + "/project/%s/stack/%s";
	final static public String stackBoundsFormat = stackFormat  + "/bounds";
	final static public String boundingBoxFormat = stackFormat + "/z/%d/box/%d,%d,%d,%d,%f";
	final static public String renderParametersFormat = boundingBoxFormat + "/render-parameters";

	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5Group", required = false, usage = "N5 dataset, e.g. /Sec26")
		private String inputDatasetName = null;

		@Option(name = "--maskN5Path", required = true, usage = "N5 dataset, e.g. /Sec26")
		private String maskN5Path = null;
		
		public Options(final String[] args) {

			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);

				if (outputN5Path == null) outputN5Path = inputN5Path;

				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				System.err.println(e.getMessage());
				parser.printUsage(System.err);
			}
		}


		public String getInputN5Path() {
			return inputN5Path;
		}

		public String getInputDatasetName() {
			return inputDatasetName;
		}

		public String getOutputN5Path() {
			return outputN5Path;
		}
		
		public String getMaskN5Path() {
			return maskN5Path;
		}
	}


	/**
	 * Copy an existing N5 dataset into another with a different blockSize.
	 *
	 * Parallelizes over blocks of [max(input, output)] to reduce redundant
	 * loading.  If blockSizes are integer multiples of each other, no
	 * redundant loading will happen.
	 *
	 * @param sc
	 * @param n5Path
	 * @param datasetName
	 * @param outDatasetName
	 * @param outBlockSize
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> List<Set<List<Long>>>  blockwiseConnectedComponents(
			final JavaSparkContext sc,
			final String inputN5Path,
			final String inputDatasetName,
			final String outputN5Path,
			final String outputDatasetName,
			final String maskN5PathName) throws IOException {

		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final N5Writer n5Writer = new N5FSWriter(outputN5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		final int n = attributes.getNumDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final long[] blockSizeL = new long[] {blockSize[0], blockSize[1], blockSize[2]};
		final long[] outputDimensions = attributes.getDimensions();
		n5Writer.createGroup(outputDatasetName);
			n5Writer.createDataset(
					outputDatasetName,
					outputDimensions,
					blockSize,
					org.janelia.saalfeldlab.n5.DataType.UINT64,
					attributes.getCompression());

		final JavaRDD<long[][]> rdd =
				sc.parallelize(
						Grid.create(
								outputDimensions,
								blockSize));

		JavaRDD<Set<List<Long>>> javaRDDsets = rdd.map(
				gridBlock -> {
					long [] offset = gridBlock[0];
					long [] dimension = gridBlock[1];
					
					final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
					
					final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
					final RandomAccessibleInterval<UnsignedByteType> source = N5Utils.open(n5ReaderLocal, inputDatasetName);
					final RandomAccessibleInterval<UnsignedByteType> sourceInterval = Views.offsetInterval(source, offset, dimension);
					
					final N5Reader n5MaskReaderLocal = new N5FSReader(maskN5PathName);
					final RandomAccessibleInterval<UnsignedByteType> mask = N5Utils.open(n5MaskReaderLocal, "/volumes/masks/foreground");
					final RandomAccessibleInterval<UnsignedByteType> maskInterval = Views.offsetInterval(mask, new long [] {offset[0]/2, offset[1]/2, offset[2]/2}, new long[] {dimension[0]/2, dimension[1]/2, dimension[2]/2});
					
					Cursor<UnsignedByteType> sourceCursor = Views.flatIterable(sourceInterval).cursor();
					RandomAccess<UnsignedByteType> maskRandomAccess = maskInterval.randomAccess();
					while (sourceCursor.hasNext()) {
						final UnsignedByteType voxel = sourceCursor.next();
						final long [] positionInMask = {(long) Math.floor(sourceCursor.getDoublePosition(0)/2), (long) Math.floor(sourceCursor.getDoublePosition(1)/2), (long) Math.floor(sourceCursor.getDoublePosition(2)/2)};
						maskRandomAccess.setPosition(positionInMask);
						if (maskRandomAccess.get().getRealDouble()==0){
							voxel.setInteger(0);
						}
					}

					long [] sourceDimensions = {0,0,0};
					source.dimensions(sourceDimensions);
					
					final ConnectedComponentsOp<UnsignedByteType> connectedComponentsOp = new ConnectedComponentsOp<>(sourceInterval, sourceDimensions, false);
					long [] currentDimensions = {0,0,0};
					sourceInterval.dimensions(currentDimensions);
					final Img< UnsignedLongType> output = new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType())
				            .create( currentDimensions);
					Set<List<Long>> uniqueIDSet = connectedComponentsOp.computeConnectedComponents(sourceInterval, output, blockSizeL, offset);
					N5Utils.saveBlock(output, n5WriterLocal, outputDatasetName, gridBlock[2]);
					
					return uniqueIDSet;
				});
		
		List<Set<List<Long>>> uniqueIDCollectedSets=javaRDDsets.collect();
		return uniqueIDCollectedSets;
		
	}


	/**
	 * Copy an existing N5 dataset into another with a different blockSize.
	 *
	 * Parallelizes over blocks of [max(input, output)] to reduce redundant
	 * loading.  If blockSizes are integer multiples of each other, no
	 * redundant loading will happen.
	 *
	 * @param sc
	 * @param n5Path
	 * @param datasetName
	 * @param outDatasetName
	 * @param outBlockSize
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> Map<Long,Long> unionFindConnectedComponents(
			final JavaSparkContext sc,
			final String inputN5Path,
			final String inputDatasetName,
			final List<Set<List<Long>>> uniqueIDCollectedSets) throws IOException {
		
		Set<List<Long>> globalIDtoGlobalIDFinalSet = new HashSet<List<Long>>();
		for(Set<List<Long>>  currentUniqueIDSet:uniqueIDCollectedSets){
			globalIDtoGlobalIDFinalSet.addAll(currentUniqueIDSet);
        }
				
		final N5Reader n5Reader = new N5FSReader(inputN5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();

		final JavaRDD<long[][]> rdd =
				sc.parallelize(
						Grid.create(
								outputDimensions,
								blockSize));
		
		JavaRDD<Set<List<Long>>> javaRDDsets=
		rdd.map(
				gridBlock -> {
					final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
					final RandomAccessibleInterval<UnsignedLongType> source = N5Utils.open(n5ReaderLocal, inputDatasetName);
					long [] sourceDimensions = {0,0,0};
					source.dimensions(sourceDimensions);
					long [] offset = gridBlock[0];
					long [] dimension = gridBlock[1];
					
					RandomAccessibleInterval<UnsignedLongType> xPlane1, yPlane1, zPlane1, xPlane2, yPlane2, zPlane2;
					xPlane1 = yPlane1 = zPlane1 = xPlane2 = yPlane2 = zPlane2 = null;
					
					long xOffset = offset[0]+blockSize[0];
					long yOffset = offset[1]+blockSize[1];
					long zOffset = offset[2]+blockSize[2];
					xPlane1 = Views.offsetInterval(source, new long []{xOffset-1, offset[1], offset[2]}, new long[]{1, dimension[1], dimension[2]});
					yPlane1 = Views.offsetInterval(source, new long []{offset[0], yOffset-1, offset[2]}, new long[]{dimension[0], 1, dimension[2]});
					zPlane1 = Views.offsetInterval(source, new long []{offset[0], offset[1], zOffset-1}, new long[]{dimension[0], dimension[1], 1});

					if(xOffset < sourceDimensions[0]) xPlane2 = Views.offsetInterval(source, new long []{xOffset, offset[1], offset[2]}, new long[]{1, dimension[1], dimension[2]});
					if(yOffset < sourceDimensions[1]) yPlane2 = Views.offsetInterval(source, new long []{offset[0], yOffset, offset[2]}, new long[]{dimension[0], 1, dimension[2]});
					if(zOffset < sourceDimensions[2]) zPlane2 = Views.offsetInterval(source, new long []{offset[0], offset[1], zOffset}, new long[]{dimension[0], dimension[1], 1});

					Set<List<Long>> globalIDtoGlobalIDSet = new HashSet<List<Long>>();
					getGlobalIDsToMerge(xPlane1, xPlane2, globalIDtoGlobalIDSet);
					getGlobalIDsToMerge(yPlane1, yPlane2, globalIDtoGlobalIDSet);
					getGlobalIDsToMerge(zPlane1, zPlane2, globalIDtoGlobalIDSet);
					
					return globalIDtoGlobalIDSet;
				});
		
		// collect RDD for printing
		long t0 = System.currentTimeMillis();
		List<Set<List<Long>>> globalIDtoGlobalIDCollectedSets=javaRDDsets.collect();
		long t1 = System.currentTimeMillis();
	
        for(Set<List<Long>>  currentGlobalIDtoGlobalIDSet:globalIDtoGlobalIDCollectedSets){
        	globalIDtoGlobalIDFinalSet.addAll(currentGlobalIDtoGlobalIDSet);
        }
        System.out.println(globalIDtoGlobalIDFinalSet.size());
        long [][] arrayOfUnions = new long[globalIDtoGlobalIDFinalSet.size()][2];
        int count = 0;
        for( List<Long> currentPair : globalIDtoGlobalIDFinalSet) {
        	arrayOfUnions[count][0] = currentPair.get(0);
        	arrayOfUnions[count][1] = currentPair.get(1);
        	count++;
        }

        System.out.println("Total unions = "+arrayOfUnions.length);
		long t2 = System.currentTimeMillis();
        
        UnionFindDGA unionFind = new UnionFindDGA(arrayOfUnions);
		unionFind.renumberRoots();
		
		long t3 = System.currentTimeMillis();
	
		System.out.println("collect time: "+(t1-t0));
		System.out.println("build array time: "+(t2-t1));
		System.out.println("union find time: "+(t3-t2));
		return unionFind.globalIDtoRootID;
	}
	
	/**
	 * Copy an existing N5 dataset into another with a different blockSize.
	 *
	 * Parallelizes over blocks of [max(input, output)] to reduce redundant
	 * loading.  If blockSizes are integer multiples of each other, no
	 * redundant loading will happen.
	 *
	 * @param sc
	 * @param n5Path
	 * @param datasetName
	 * @param outDatasetName
	 * @param outBlockSize
	 * @return 
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> void mergeConnectedComponents(
			final JavaSparkContext sc,
			final String inputN5Path,
			final String inputDatasetName,
			final String outputDatasetName,
			final List<Set<List<Long>>> uniqueIDCollectedSets,
			final Map<Long, Long> globalIDtoRootID ) throws IOException {
		
		final PrintWriter driverOut = new PrintWriter("/groups/cosem/cosem/ackermand/tmp/logDriver.txt");

		logMsg("A", driverOut);
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final N5Writer n5Writer = new N5FSWriter(inputN5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();
		logMsg("B", driverOut);
		n5Writer.createGroup(outputDatasetName);
		logMsg("C", driverOut);
		n5Writer.createDataset(
					outputDatasetName,
					outputDimensions,
					blockSize,
					org.janelia.saalfeldlab.n5.DataType.UINT64,
					attributes.getCompression());
		
		logMsg("D", driverOut);
		final List< BlockInformation>blockInformation = mapAndGridForMergeConnectedComponents(uniqueIDCollectedSets, globalIDtoRootID, outputDimensions, blockSize); 
		/*final List<long[][]> chunks = Grid.create(
				outputDimensions,
				blockSize);
		
		logMsg("E", driverOut);
		logMsg("E1", driverOut);
        final JavaRDD<long[][]> rdd =
				sc.parallelize(chunks);
        */
        final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformation);
		logMsg("E2", driverOut);
		
		rdd.foreach(
				currentBlockInformation -> {
					final Map<Long,Long> currentBlockGlobalIDtoRootID = currentBlockInformation.map;
					final long[][] gridBlock = currentBlockInformation.gridBlock;
					long [] offset = gridBlock[0];
					long [] dimension = gridBlock[1];
					final PrintWriter out = null; //new PrintWriter(String.format("/groups/cosem/cosem/ackermand/tmp/logMemory-%d_%d_%d.txt", offset[0],offset[1],offset[2]));
					logMemory("BEFORE", out);
					final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
					logMemory("N5READER CREATED", out);
					final RandomAccessibleInterval<UnsignedLongType> source = N5Utils.open(n5ReaderLocal, inputDatasetName);
					logMemory("SOURCE CREATED", out);
					//final Map<Long,Long> globalIDtoRootID = broadcastGlobalIDtoRootID.value();
					logMemory("MAP LOADED", out);
					final RandomAccessibleInterval<UnsignedLongType> sourceInterval = Views.offsetInterval(source, offset, dimension);
					logMemory("SOURCE INTERVAL CREATED", out);
					Cursor<UnsignedLongType> sourceCursor = Views.flatIterable(sourceInterval).cursor();
					logMemory("CURSOR CREATED", out);
					while (sourceCursor.hasNext()) {
						final UnsignedLongType voxel = sourceCursor.next();
						long currentValue = voxel.getLong();
						if(currentValue>0) {
							try {
								Long currentRoot = currentBlockGlobalIDtoRootID.get(currentValue);
								voxel.setLong( currentRoot);
							}
							catch(Exception e){
								throw new java.lang.Error("Corrupt map from global ID to renumbered ID");
							}
						}
						
					}
					logMemory("UPDATED SOURCE", out);

					final N5Writer n5WriterLocal = new N5FSWriter(inputN5Path);
					logMemory("N5WRITER CREATED", out);
					N5Utils.saveBlock(sourceInterval, n5WriterLocal, outputDatasetName, gridBlock[2]);
					logMemory("N5 SAVED", out);
					//out.close();
				});	
		logMsg("E3", driverOut);


	    driverOut.close();

	}
	
	public static void logMemory(final String context,
                                 final PrintWriter out) {
         final long freeMem = Runtime.getRuntime().freeMemory() / 1000000L;
         final long totalMem = Runtime.getRuntime().totalMemory() / 1000000L;
         logMsg(context + ", Total: "+ totalMem + " MB, Free: " + freeMem + " MB, Delta: "+ (totalMem-freeMem)+" MB", out);
    }

	public static void logMsg(final String msg,
			                  final PrintWriter out) {
		final String ts = new SimpleDateFormat("HH:mm:ss").format(new Date()) + " ";
		System.out.println(ts+" "+msg);
		//out.println(ts + " " + msg);
		//out.flush();
		
	}
	
	public static List< BlockInformation> mapAndGridForMergeConnectedComponents(final List<Set<List<Long>>>  setOfUniqueIDs, final Map<Long, Long> globalIDtoRootID, final long[] outputDimensions, final int[] blockSize) {
		final List<long[][]> chunks = Grid.create(
				outputDimensions,
				blockSize);
		
		//List< BlockInformationPair<Map<Long,Long>, long[][] >> blockInformation = new ArrayList< BlockInformationPair< Map<Long,Long>, long[][] >>();
		
		List< BlockInformation> blockInformation = new ArrayList< BlockInformation>();
		for (int i=0; i<chunks.size(); i++) {
			long[][] currentGridBlock = chunks.get(i);
			
			final Set<List<Long>> currentBlockUniqueIDs = setOfUniqueIDs.get(i);
			final Map<Long, Long> currentBlockGlobalIDtoRootID = new HashMap<>();
			for  (List<Long> currentUniqueID : currentBlockUniqueIDs) {
				final Long key = currentUniqueID.get(0);
				currentBlockGlobalIDtoRootID.put(key, globalIDtoRootID.get(key));
			}
			final BlockInformation currentBlockInformation = new BlockInformation(currentBlockGlobalIDtoRootID, currentGridBlock);
			blockInformation.add(currentBlockInformation);
		}
		return blockInformation;
	}

	public static final void getGlobalIDsToMerge(RandomAccessibleInterval<UnsignedLongType> hyperSlice1, RandomAccessibleInterval<UnsignedLongType> hyperSlice2, Set<List<Long>> globalIDtoGlobalIDSet) {
		if (hyperSlice1!=null && hyperSlice2!=null) {
			Cursor<UnsignedLongType> hs1Cursor = Views.flatIterable(hyperSlice1).cursor();
			Cursor<UnsignedLongType> hs2Cursor = Views.flatIterable(hyperSlice2).cursor();
			while (hs1Cursor.hasNext()) {
				long hs1Value = hs1Cursor.next().getLong();
				long hs2Value = hs2Cursor.next().getLong();
				if (hs1Value >0 && hs2Value > 0 ) {
					globalIDtoGlobalIDSet.add(Arrays.asList(Math.min(hs1Value,hs2Value), Math.max(hs1Value,hs2Value)));
				}
			}

		}
	}
	
	
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkRandomSubsampleN5");
		
		//Get all organelles
		String[] organelles = {""};
		if(options.getInputDatasetName()!=null) {
			organelles = options.getInputDatasetName().split(",");
		}
		else {
			File file = new File(options.getInputN5Path());
			organelles = file.list(new FilenameFilter() {
			  @Override
			  public boolean accept(File current, String name) {
			    return new File(current, name).isDirectory();
			  }
			});
		}
		System.out.println(Arrays.toString(organelles));

		for(String currentOrganelle : organelles) {
			JavaSparkContext sc = new JavaSparkContext(conf);
			logMsg(currentOrganelle,null);
			final String tempOutputDatasetName = currentOrganelle+"_blockwise_cc_temp_to_delete";
			final String finalOutputDatasetName = currentOrganelle+"_cc";
			final List<Set<List<Long>>> uniqueIDCollectedSets = blockwiseConnectedComponents(sc, options.getInputN5Path(), currentOrganelle, options.getOutputN5Path(), tempOutputDatasetName, options.getMaskN5Path());	
			logMsg("Stage 1 complete",null);
			final Map<Long, Long> globalIDtoRootID = unionFindConnectedComponents(sc, options.getOutputN5Path(), tempOutputDatasetName, uniqueIDCollectedSets);
			logMsg("Stage 2 complete",null);
			mergeConnectedComponents(sc, options.getOutputN5Path(), tempOutputDatasetName, finalOutputDatasetName, uniqueIDCollectedSets, globalIDtoRootID);
			logMsg("Stage 3 complete",null);
			sc.close();
		}
		
		
		for(String currentOrganelle : organelles) {
			final String tempOutputDatasetName = currentOrganelle+"_blockwise_cc_temp_to_delete";
			FileUtils.deleteDirectory(new File(options.getOutputN5Path()+"/"+tempOutputDatasetName));
		}
		
	}
}
