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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.hotknife.ops.ConnectedComponentsOp;
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

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * Export a render stack to N5.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public class SparkUnionFindConnectedComponents {

	final static public String ownerFormat = "%s/owner/%s";
	final static public String stackListFormat = ownerFormat + "/stacks";
	final static public String stackFormat = ownerFormat + "/project/%s/stack/%s";
	final static public String stackBoundsFormat = stackFormat + "/bounds";
	final static public String boundingBoxFormat = stackFormat + "/z/%d/box/%d,%d,%d,%d,%f";
	final static public String renderParametersFormat = boundingBoxFormat + "/render-parameters";

	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5Group", required = true, usage = "N5 dataset, e.g. /Sec26")
		private String inputDatasetName = null;

		@Option(name = "--outputN5Group", required = true, usage = "N5 dataset, e.g. /Sec26")
		private String outputDatasetName = null;

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

		public String getInputDatasetName() {
			return inputDatasetName;
		}

		public String getOutputN5Path() {
			return outputN5Path;
		}

		public String getOutputDatasetName() {
			return outputDatasetName;
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
	public static final <T extends NativeType<T>> void connectedComponents(
			final JavaSparkContext sc,
			final String inputN5Path,
			final String outputN5Path,
			final String inputDatasetName,
			final String outputDatasetName) throws IOException {

		final N5Writer n5Reader = new N5FSWriter(inputN5Path);
		final N5Writer n5Writer = new N5FSWriter(outputN5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		final int n = attributes.getNumDimensions();
		final int[] blockSize = attributes.getBlockSize();
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
		
		JavaRDD<Map<Long,Long>> javaRDDmaps=
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
					
					/*final RandomAccessibleInterval<UnsignedLongType> referenceBlock = Views.offsetInterval(source, offset, dimension);
					xPlane1 = Views.hyperSlice(referenceBlock, 0, dimension[0]-1);
					yPlane1= Views.hyperSlice(referenceBlock, 1, dimension[1]-1);
					zPlane1 = Views.hyperSlice(referenceBlock, 2, dimension[2]-1);
					
					final RandomAccessibleInterval<UnsignedLongType> blockRight = Views.offsetInterval(source, new long []{offset[0]+blockSize[0], offset[1], offset[2]}, dimension);
					if(offset[0]+blockSize[0] < sourceDimensions[0]) xPlane2 = Views.hyperSlice(blockRight, 0, 0);
					
					final RandomAccessibleInterval<UnsignedLongType> blockBottom = Views.offsetInterval(source, new long []{offset[0], offset[1]+blockSize[1], offset[2]}, dimension);
					if(offset[1]+blockSize[1] < sourceDimensions[1]) yPlane2 = Views.hyperSlice(blockBottom, 1, 0);
					
					final RandomAccessibleInterval<UnsignedLongType> blockAbove = Views.offsetInterval(source, new long []{offset[0], offset[1], offset[2]+blockSize[2]}, dimension);
					if(offset[2]+blockSize[2] < sourceDimensions[2]) zPlane2 = Views.hyperSlice(blockAbove, 2, 0);
					*/
					long xOffset = offset[0]+blockSize[0];
					long yOffset = offset[1]+blockSize[1];
					long zOffset = offset[2]+blockSize[2];
					xPlane1 = Views.offsetInterval(source, new long []{xOffset-1, offset[1], offset[2]}, new long[]{1, dimension[1], dimension[2]});
					yPlane1 = Views.offsetInterval(source, new long []{offset[0], yOffset-1, offset[2]}, new long[]{dimension[0], 1, dimension[2]});
					zPlane1 = Views.offsetInterval(source, new long []{offset[0], offset[1], zOffset-1}, new long[]{dimension[0], dimension[1], 1});

					
					if(xOffset < sourceDimensions[0]) xPlane2 = Views.offsetInterval(source, new long []{xOffset, offset[1], offset[2]}, new long[]{1, dimension[1], dimension[2]});
					if(yOffset < sourceDimensions[1]) yPlane2 = Views.offsetInterval(source, new long []{offset[0], yOffset, offset[2]}, new long[]{dimension[0], 1, dimension[2]});
					if(zOffset < sourceDimensions[2]) zPlane2 = Views.offsetInterval(source, new long []{offset[0], offset[1], zOffset}, new long[]{dimension[0], dimension[1], 1});

					Map<Long, Long> globalIDtoGlobalIDMap = new HashMap<Long, Long>();
					
					getGlobalIDsToMerge(xPlane1, xPlane2, globalIDtoGlobalIDMap);
					getGlobalIDsToMerge(yPlane1, yPlane2, globalIDtoGlobalIDMap);
					getGlobalIDsToMerge(zPlane1, zPlane2, globalIDtoGlobalIDMap);
					
					return globalIDtoGlobalIDMap;
				});
		
		// collect RDD for printing
		long t0 = System.currentTimeMillis();
		int totalUnions = 0;
		List<Map<Long,Long>> globalIDtoGlobalIDmaps=javaRDDmaps.collect();
        for(Map<Long, Long> value:globalIDtoGlobalIDmaps){
        	totalUnions+=value.size();
        }
		long t1 = System.currentTimeMillis();
		
        long [][] arrayOfUnions = new long[totalUnions][2];
        int count = 0;
        for(Map<Long, Long> currentMap:globalIDtoGlobalIDmaps){
        	if (!currentMap.isEmpty()) {
        		for (Map.Entry<Long, Long> entry : currentMap.entrySet()) {
        			arrayOfUnions[count][0] = entry.getKey(); 
        			arrayOfUnions[count][1] = entry.getValue();
        			count++;
        		}
        	}
        }
		long t2 = System.currentTimeMillis();

        System.out.println("Total unions = "+totalUnions);
        
        UnionFindDGA unionFind = new UnionFindDGA(arrayOfUnions);
		long t3 = System.currentTimeMillis();
		for (Map.Entry<Long, Long> entry : unionFind.globalIDtoRootID.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue().toString());
		}   
		System.out.println("collect time: "+(t1-t0));
		System.out.println("build array time: "+(t2-t1));
		System.out.println("union find time: "+(t3-t2));

	}
	
	public static final void getGlobalIDsToMerge(RandomAccessibleInterval<UnsignedLongType> hyperSlice1, RandomAccessibleInterval<UnsignedLongType> hyperSlice2, Map<Long, Long> globalIDtoGlobalID) {
		if (hyperSlice1!=null && hyperSlice2!=null) {
			Cursor<UnsignedLongType> hs1Cursor = Views.flatIterable(hyperSlice1).cursor();
			Cursor<UnsignedLongType> hs2Cursor = Views.flatIterable(hyperSlice2).cursor();
			while (hs1Cursor.hasNext()) {
				long hs1Value = hs1Cursor.next().getLong();
				long hs2Value = hs2Cursor.next().getLong();
				if (hs1Value >0 && hs2Value > 0 ) {
					globalIDtoGlobalID.put(Math.min(hs1Value,hs2Value), Math.max(hs1Value,hs2Value));
				}
			}

		}
	}

	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkRandomSubsampleN5");
		final JavaSparkContext sc = new JavaSparkContext(conf);

		connectedComponents(sc, options.getInputN5Path(), options.getOutputN5Path(), options.getInputDatasetName(),
				options.getOutputDatasetName());

		sc.close();

	}
}
