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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.ejml.data.DMatrixRMaj;
import org.ejml.dense.row.decomposition.eig.SymmetricQRAlgorithmDecomposition_DDRM;
import org.janelia.saalfeldlab.hotknife.ops.GradientCenter;
import org.janelia.saalfeldlab.hotknife.ops.SimpleGaussRA;
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

import com.google.common.io.Files;

import ij.IJ;
import ij.ImageJ;
import ij.ImagePlus;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;
import net.imglib2.Cursor;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.array.ArrayRandomAccess;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.algorithm.neighborhood.Neighborhood;
import net.imglib2.algorithm.neighborhood.RectangleShape;
import net.imglib2.algorithm.neighborhood.Shape;


/**
 * Expand skeleton for visualization purposes when making meshes
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkSeparateRibosomes {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;
		
		@Option(name = "--inputN5DatasetName", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--sheetnessCSV", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String sheetnessCSV = null;
		
		@Option(name = "--sheetnessMaskedCSV", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String sheetnessMaskedCSV = null;
		
		@Option(name = "--sheetnessCutoff", required = false, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private double sheetnessCutoff = 0.9;
		
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
			if(outputN5Path == null) {
				return inputN5Path;
			}
			else {
				return outputN5Path;
			}
		}
		
		public String getSheetnessCSV() {
			return sheetnessCSV;
		}
		
		public String getSheetnessMaskedCSV() {
			return sheetnessMaskedCSV;
		}
		
		public double getSheetnessCutoff() {
			return sheetnessCutoff;
		}

	}

	/**
	 * Expand skeletonization (1-voxel thin data) by a set radius to make meshes cleaner and more visible.
	 * 
	 * @param sc					Spark context
	 * @param n5Path				Input N5 path
	 * @param inputDatasetName		Skeletonization dataset name
	 * @param n5OutputPath			Output N5 path
	 * @param outputDatasetName		Output N5 dataset name
	 * @param expansionInVoxels		Expansion in voxels
	 * @param blockInformationList	List of block information
	 * @throws IOException
	 */
	public static final void separateRibosomes(final JavaSparkContext sc, final String n5Path,
			final String inputDatasetName, final String n5OutputPath, Broadcast<Map<Long, Integer>> broadcastedRibosomeIDtoInformationMap,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();

		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		
		String cytosolicString = inputDatasetName+"_cytosolic";
		String nuclearString = inputDatasetName+"_nuclear";
		String sheetString = inputDatasetName+"_sheet";
		String tubeString = inputDatasetName+"_tube";

		for(String currentString: new String[] {cytosolicString, nuclearString, sheetString, tubeString}) {
			n5Writer.createDataset(currentString, dimensions, blockSize, DataType.UINT64, new GzipCompression());
			n5Writer.setAttribute(currentString, "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, inputDatasetName)));
		}
		
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		
	
		rdd.foreach(blockInformation -> {
			final long[][] gridBlock = blockInformation.gridBlock;
			long[] offset = gridBlock[0];//new long[] {64,64,64};//gridBlock[0];////
			long[] dimension = gridBlock[1];
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			
			Map<Long, Integer> ribosomeIDtoInformationMap = broadcastedRibosomeIDtoInformationMap.value();

			RandomAccessibleInterval<UnsignedLongType> ribosomes = Views.offsetInterval(Views.extendZero((RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, inputDatasetName)),offset, dimension);	
			IntervalView<UnsignedLongType> cytosolic = Views.offsetInterval(ArrayImgs.unsignedLongs(dimension),new long[]{0,0,0}, dimension);
			IntervalView<UnsignedLongType> nuclear = Views.offsetInterval(ArrayImgs.unsignedLongs(dimension),new long[]{0,0,0}, dimension);
			IntervalView<UnsignedLongType> sheet = Views.offsetInterval(ArrayImgs.unsignedLongs(dimension),new long[]{0,0,0}, dimension);
			IntervalView<UnsignedLongType> tube = Views.offsetInterval(ArrayImgs.unsignedLongs(dimension),new long[]{0,0,0}, dimension);

			RandomAccess<UnsignedLongType> ribosomesRA = ribosomes.randomAccess();
			RandomAccess<UnsignedLongType> cytosolicRA = cytosolic.randomAccess();
			RandomAccess<UnsignedLongType> nuclearRA = nuclear.randomAccess();
			RandomAccess<UnsignedLongType> sheetRA = sheet.randomAccess();
			RandomAccess<UnsignedLongType> tubeRA = tube.randomAccess();

			for(int x=0; x<dimension[0]; x++) {
				for(int y=0; y<dimension[1]; y++) {
					for(int z=0; z<dimension[2]; z++) {
						int pos[] = new int[] {x,y,z};
						ribosomesRA.setPosition(pos);
						long ID = ribosomesRA.get().get();

						if(ID>0) {//then it is on microtubule center axis
							
							int currentInformation = ribosomeIDtoInformationMap.getOrDefault(ID, 0);
							
							if(currentInformation==0) {
								cytosolicRA.setPosition(pos);
								cytosolicRA.get().set(ID);
							}
							else if(currentInformation==1){
								nuclearRA.setPosition(pos);
								nuclearRA.get().set(ID);
							}
							else if(currentInformation==2) {
								sheetRA.setPosition(pos);
								sheetRA.get().set(ID);
							}
							else {
								tubeRA.setPosition(pos);
								tubeRA.get().set(ID);
							}
							
						}
					}
				}
			}
			
			
			final N5Writer n5BlockWriter = new N5FSWriter(n5OutputPath);
			
			N5Utils.saveBlock(cytosolic, n5BlockWriter, cytosolicString, gridBlock[2]);
			N5Utils.saveBlock(nuclear, n5BlockWriter, nuclearString, gridBlock[2]);
			N5Utils.saveBlock(sheet, n5BlockWriter, sheetString, gridBlock[2]);
			N5Utils.saveBlock(tube, n5BlockWriter, tubeString, gridBlock[2]);

		});

	}
	
	//read in csv
	public static Map<Long,Integer> readInData(String sheetnessCSV, String sheetnessMaskedCSV, double sheetnessCutoff) throws NumberFormatException, IOException{
		String row;
		//id --> 1: nucleus bound, 2: sheet, 3:tube. if none of these, then is cytosolic
		Map<Long,Integer> ribosomeIDtoInformationMap = new HashMap<Long,Integer>();

		
		//sheetnessCSV
		//treat them all as nuclear bound, will update in the next step
		int count=0;
		BufferedReader csvReader = new BufferedReader(new FileReader(sheetnessCSV));		
		while ((row = csvReader.readLine()) != null) {
		    String[] data = row.split(",");
		    if(count>0) {
		    	long ID = Long.parseLong(data[0]);
		    	ribosomeIDtoInformationMap.put(ID, 1);    	
		    }
		    count++;
		 }
		csvReader.close();
		
		//sheetnessMaskedCSV
		count=0;
		csvReader = new BufferedReader(new FileReader(sheetnessMaskedCSV));		
		while ((row = csvReader.readLine()) != null) {
		    String[] data = row.split(",");
		    if(count>0) {
		    	long ID = Long.parseLong(data[0]);
		    	double sheetness = Double.parseDouble(data[1]);
		    	int information = sheetness>=sheetnessCutoff ? 2 : 3 ;
		    	ribosomeIDtoInformationMap.put(ID, information);    	
		    }
		    count++;
		 }
		csvReader.close();
		
		//all unlabeled ones are cytosolic
		return ribosomeIDtoInformationMap;
	}
	/**
	 * Expand skeleton for more visible meshes
	 * 
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkSeparateRibosomes");
		
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		Map<Long,Integer> ribosomeIDtoInformationMap = readInData(options.getSheetnessCSV(), options.getSheetnessMaskedCSV(), options.getSheetnessCutoff());
		Broadcast<Map<Long, Integer>> broadcastedRibosomeIDtoInformationMap = sc.broadcast(ribosomeIDtoInformationMap);

		// Create block information list
		List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), options.getInputN5DatasetName());
		separateRibosomes(sc, options.getInputN5Path(),
				options.getInputN5DatasetName(), options.getOutputN5Path(), broadcastedRibosomeIDtoInformationMap, blockInformationList);
		sc.close();
		

	}
}
