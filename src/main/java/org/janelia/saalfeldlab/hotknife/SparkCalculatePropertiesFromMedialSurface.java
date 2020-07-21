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
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

import org.janelia.saalfeldlab.hotknife.CorrectlyPaddedDistanceTransform;
/**
 * Get surface area, volume and thickness of volume based on medial surface.
 * Thickness is calculated from medial surface, and surface area and thickness are calculated from a volume recreated from the medial surface. Produces files with histogram data.
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkCalculatePropertiesFromMedialSurface {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "Input N5 path")
		private String inputN5Path = null;

		@Option(name = "--outputDirectory", required = false, usage = "Output directory")
		private String outputDirectory = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "Onput N5 dataset")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5Path", required = false, usage = "Output N5 path")
		private String outputN5Path = null;

		public Options(final String[] args) {
			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);
				if (outputN5Path == null)
					outputN5Path = inputN5Path;
				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				parser.printUsage(System.err);
			}
		}
		
		public String getInputN5Path() {
			return inputN5Path;
		}

		public String getInputN5DatasetName() {
			return inputN5DatasetName;
		}
		
		public String getOutputDirectory() {
			if(outputDirectory == null) {
				outputDirectory = inputN5Path.split(".n5")[0]+"_results";
			}
			return outputDirectory;
		}
		public String getOutputN5Path() {
			return outputN5Path;
		}

	}
	
	/**
	 * 
	 * Create a sheetness volume from the medial surface by expanding the medial surface with a sphere centered at each voxel in the medial surface, with a radius equal to the distance transform at that voxel. The value of voxels within the sphere is the sheetness value at the center voxel, with all voxels in the the resultant volume set to the average value of all spheres containing that voxel.
	 * 
	 * @param sc					Spark context
	 * @param n5Path				Path to n5
	 * @param datasetName			N5 dataset name
	 * @param n5OutputPath			Path to output n5
	 * @param blockInformationList	Block inforation list
	 * @return						Histogram maps class with sheetness histograms
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final HistogramMaps projectCurvatureToSurface(
			final JavaSparkContext sc,
			final String n5Path,
			final String datasetName,
			final String n5OutputPath,
			final List<BlockInformation> blockInformationList) throws IOException {

		//General information
		final N5Reader n5Reader = new N5FSReader(n5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		double [] pixelResolution = IOHelper.getResolution(n5Reader, datasetName);
		double voxelVolume = pixelResolution[0]*pixelResolution[1]*pixelResolution[2];
		double voxelFaceArea = pixelResolution[0]*pixelResolution[1];

		//Create output that will contain volume averaged sheetness
		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		n5Writer.createDataset(
				datasetName + "_sheetnessVolumeAveraged",
				dimensions,
				blockSize,
				DataType.UINT8,
				new GzipCompression());
		n5Writer.setAttribute(datasetName + "_sheetnessVolumeAveraged", "pixelResolution", new IOHelper.PixelResolution(pixelResolution));

		//Parallelize analysis over blocks
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<HistogramMaps> javaRDDHistogramMaps = rdd.map(blockInformation -> {
			final long [][] gridBlock = blockInformation.gridBlock;
			final long [] offset = gridBlock[0];
			final long [] dimension = gridBlock[1];
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			
			//Get correctly padded distance transform first
			RandomAccessibleInterval<UnsignedLongType> segmentation = (RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, datasetName);
			CorrectlyPaddedDistanceTransform cpdt =  new CorrectlyPaddedDistanceTransform(segmentation, offset, dimension);
			
			//Get corresponding medial surface and sheetness
			IntervalView<UnsignedLongType> medialSurface = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5BlockReader, datasetName+"_medialSurface")
					),cpdt.paddedOffset,cpdt.paddedDimension);
			RandomAccessibleInterval<DoubleType> sheetness = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<DoubleType>) N5Utils.open(n5BlockReader, datasetName+"_sheetness")
					),cpdt.paddedOffset, cpdt.paddedDimension);
			
			//Create images required to calculate the average sheetness at a voxel: the sum and the counts at a given voxel. Add an extra 2 because need to check for surface voxels so need extra border of 1
			final Img<FloatType> sheetnessSum = new ArrayImgFactory<FloatType>(new FloatType())
					.create(new long[]{blockSize[0]+2,blockSize[1]+2,blockSize[2]+2}); 
			final Img<UnsignedIntType> counts = new ArrayImgFactory<UnsignedIntType>(new UnsignedIntType())
					.create(new long[]{blockSize[0]+2,blockSize[1]+2,blockSize[2]+2});
			
			//Get cursor and random accessibles
			Cursor<UnsignedLongType> medialSurfaceCursor = medialSurface.cursor();
			RandomAccess<DoubleType> sheetnessRA = sheetness.randomAccess();
			RandomAccess<FloatType> sheetnessSumRA = sheetnessSum.randomAccess();
			RandomAccess<UnsignedIntType> countsRA = counts.randomAccess();
			
			//Update sheetness and thickness histogram, as well as sum and counts used to create volume averaged sheetness
			Map<List<Integer>,Long> sheetnessAndThicknessHistogram = new HashMap<List<Integer>,Long>();
			createSumAndCountsAndUpdateHistogram(medialSurfaceCursor, cpdt, sheetnessRA, sheetnessSumRA, countsRA, pixelResolution, sheetnessAndThicknessHistogram);			
			
			medialSurface = null;
			cpdt = null;
			sheetness = null;

			//Take average and update histograms
			Map<Integer,Double> sheetnessAndSurfaceAreaHistogram = new HashMap<Integer,Double>();
			Map<Integer,Double> sheetnessAndVolumeHistogram = new HashMap<Integer,Double>();
			final Img<UnsignedByteType> output = createVolumeAveragedSheetnessAndUpdateHistograms(dimension, sheetnessSumRA, countsRA, voxelVolume, voxelFaceArea, sheetnessAndSurfaceAreaHistogram, sheetnessAndVolumeHistogram);
			
			//Write out volume averaged sheetness
			final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
			N5Utils.saveBlock(output, n5BlockWriter, datasetName + "_sheetnessVolumeAveraged", gridBlock[2]);
			return new HistogramMaps(sheetnessAndThicknessHistogram, sheetnessAndSurfaceAreaHistogram, sheetnessAndVolumeHistogram);
		});
		
		HistogramMaps histogramMaps = javaRDDHistogramMaps.reduce((a,b) -> {
			a.merge(b);
			return a;
		});
		
		return histogramMaps;
		
	}
	
	/**
	 * Count how many faces of a voxel are exposed to the surface
	 * 
	 * @param countsRandomAccess 	Random access for counts image
	 * @return						Number of faces on surface
	 */
	public static int getSurfaceAreaContributionOfVoxelInFaces(final RandomAccess<UnsignedIntType> countsRandomAccess) {
		
		final long pos[] = {countsRandomAccess.getLongPosition(0), countsRandomAccess.getLongPosition(1), countsRandomAccess.getLongPosition(2)};
		int surfaceAreaContributionOfVoxelInFaces = 0;

		for(long[] currentVoxel : SparkCosemHelper.voxelsToCheckForSurface) {
			final long currentPosition[] = {pos[0]+currentVoxel[0], pos[1]+currentVoxel[1], pos[2]+currentVoxel[2]};
			countsRandomAccess.setPosition(currentPosition);
			if(countsRandomAccess.get().get() ==0) {
				surfaceAreaContributionOfVoxelInFaces ++;
			}
		}

		return surfaceAreaContributionOfVoxelInFaces;	
	
	}
	
	/**
	 * Update sheetness sum and count images which are used to calculate the volume averaged sheetness.
	 * The averaged sheetness is calculated by taking the radius (thickness) at a medial surface voxel, and filing in all voxels within that sphere with the corresponding sheetness. Sum and counts are used to take the average in cases where multiple spheres contain a single voxel.
	 * 
	 * @param pos					Position
	 * @param radiusPlusPadding		Radius with padding
	 * @param radiusSquared			Radius squared
	 * @param cpdt					Correctly padded distance transform instance
	 * @param sheetnessMeasure		Sheetness value
	 * @param sheetnessSumRA		Random access for sheetness sum
	 * @param countsRA				Random access for counts
	 */
	private static void updateSheetnessSumAndCount(int[] pos, int radiusPlusPadding, float radiusSquared, CorrectlyPaddedDistanceTransform cpdt, float sheetnessMeasure, RandomAccess<FloatType> sheetnessSumRA, RandomAccess<UnsignedIntType> countsRA ) {
		for(int x = pos[0]-radiusPlusPadding; x<=pos[0]+radiusPlusPadding; x++) {
			for(int y = pos[1]-radiusPlusPadding; y<=pos[1]+radiusPlusPadding; y++) {
				for(int z = pos[2]-radiusPlusPadding; z<=pos[2]+radiusPlusPadding; z++) {
					int dx = x-pos[0];
					int dy = y-pos[1];
					int dz = z-pos[2];
					//need to check extra padding of 1 because in next step we need this halo for checking surfaces
					if((x>=cpdt.padding[0]-1 && x<=cpdt.paddedDimension[0]-cpdt.padding[0] && y>=cpdt.padding[1]-1 && y <= cpdt.paddedDimension[1]-cpdt.padding[1] && z >= cpdt.padding[2]-1 && z <= cpdt.paddedDimension[2]-cpdt.padding[2]) && dx*dx+dy*dy+dz*dz<= radiusSquared ) { //then it is in sphere
						long [] spherePos = new long[]{x-(cpdt.padding[0]-1),y-(cpdt.padding[1]-1),z-(cpdt.padding[2]-1)};
						sheetnessSumRA.setPosition(spherePos);
						FloatType outputVoxel = sheetnessSumRA.get();
						outputVoxel.set(outputVoxel.get()+sheetnessMeasure);
						
						countsRA.setPosition(spherePos);
						UnsignedIntType countsVoxel = countsRA.get();
						countsVoxel.set(countsVoxel.get()+1);
																
					}
				}
			}
		}
	}
	
	/**
	 * Use distance transform to get thickness at medial surface. Use that thickness as a radius for spheres to calculate sum and counts for volume averaged sheetness.
	 * 
	 * @param medialSurfaceCursor				Cursor for medial surface
	 * @param cpdt								Correctly padded distance transform class instance
	 * @param sheetnessRA						Sheetness random access
	 * @param sheetnessSumRA					Sheetness sum random access
	 * @param countsRA							Counts random access
	 * @param pixelResolution					Pixel resolution
	 * @param sheetnessAndThicknessHistogram	Histogram as a map containing sheetness and thickness
	 */
	private static void createSumAndCountsAndUpdateHistogram(Cursor<UnsignedLongType> medialSurfaceCursor, CorrectlyPaddedDistanceTransform cpdt, RandomAccess<DoubleType> sheetnessRA, RandomAccess<FloatType> sheetnessSumRA, RandomAccess<UnsignedIntType> countsRA, double [] pixelResolution, Map<List<Integer>,Long> sheetnessAndThicknessHistogram) {
		RandomAccess<FloatType> distanceTransformRA = cpdt.correctlyPaddedDistanceTransform.randomAccess();	
		while (medialSurfaceCursor.hasNext()) {
			final long medialSurfaceValue = medialSurfaceCursor.next().get();
			if ( medialSurfaceValue >0 ) { // then it is on medial surface
				
				int [] pos = {medialSurfaceCursor.getIntPosition(0),medialSurfaceCursor.getIntPosition(1),medialSurfaceCursor.getIntPosition(2) };
				distanceTransformRA.setPosition(pos);
				sheetnessRA.setPosition(pos);
	
				float radiusSquared = distanceTransformRA.get().getRealFloat();
				double radius = Math.sqrt(radiusSquared);
				int radiusPlusPadding = (int) Math.ceil(radius);
				
				float sheetnessMeasure = sheetnessRA.get().getRealFloat();					
				int sheetnessMeasureBin = (int) Math.floor(sheetnessMeasure*256);
				
				if(pos[0]>=cpdt.padding[0] && pos[0]<cpdt.paddedDimension[0]-cpdt.padding[0] &&
						pos[1]>=cpdt.padding[1] && pos[1]<cpdt.paddedDimension[1]-cpdt.padding[1] &&
						pos[2]>=cpdt.padding[2] && pos[2]<cpdt.paddedDimension[2]-cpdt.padding[2]) {
					
					double thickness = radius*2;//convert later
					int thicknessBin = (int) Math.min(Math.floor(thickness*pixelResolution[0]/8),99); //bin thickness in 8 nm bins
					
					List<Integer> histogramBinList = Arrays.asList(sheetnessMeasureBin,thicknessBin);
					sheetnessAndThicknessHistogram.put(histogramBinList,sheetnessAndThicknessHistogram.getOrDefault(histogramBinList,0L)+1L);
				}
				
				
				updateSheetnessSumAndCount(pos, radiusPlusPadding, radiusSquared, cpdt, sheetnessMeasure, sheetnessSumRA, countsRA);
			}
		}
	}
	
	/**
	 * Create output volume averaged sheetness by dividing Sum image by Counts image, and binning from 0-255.
	 * @param dimension								Block dimension
	 * @param sheetnessSumRA						Sheetness sum random access
	 * @param countsRA								Counts random access
	 * @param voxelVolume							Volume of a voxel
	 * @param voxelFaceArea							Area of a voxel face
	 * @param sheetnessAndSurfaceAreaHistogram		Histogram for sheetness and surface area
	 * @param sheetnessAndVolumeHistogram			Histogram for sheetness and volume
	 * @return
	 */
	private static Img<UnsignedByteType> createVolumeAveragedSheetnessAndUpdateHistograms(long [] dimension, RandomAccess<FloatType> sheetnessSumRA, RandomAccess<UnsignedIntType> countsRA, double voxelVolume, double voxelFaceArea, Map<Integer,Double> sheetnessAndSurfaceAreaHistogram, Map<Integer,Double> sheetnessAndVolumeHistogram){
		
		final Img<UnsignedByteType> output = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType()).create(dimension);
		RandomAccess<UnsignedByteType> outputRandomAccess = output.randomAccess();
		for(long x=1; x<dimension[0]+1;x++) {
			for(long y=1; y<dimension[1]+1;y++) {
				for(long z=1; z<dimension[2]+1;z++) {
					long [] pos = new long[]{x,y,z};
					sheetnessSumRA.setPosition(pos);
					countsRA.setPosition(pos);
					if(countsRA.get().get()>0) {
						float sheetnessMeasure = sheetnessSumRA.get().get()/countsRA.get().get();
						sheetnessSumRA.get().set(sheetnessMeasure);//take average
						int sheetnessMeasureBin = (int) Math.floor(sheetnessMeasure*256);
						sheetnessAndVolumeHistogram.put(sheetnessMeasureBin, sheetnessAndVolumeHistogram.getOrDefault(sheetnessMeasureBin,0.0)+voxelVolume);
						int faces = getSurfaceAreaContributionOfVoxelInFaces(countsRA);
						if(faces>0) {
							sheetnessAndSurfaceAreaHistogram.put(sheetnessMeasureBin, sheetnessAndSurfaceAreaHistogram.getOrDefault(sheetnessMeasureBin,0.0)+faces*voxelFaceArea);
						}
	
						outputRandomAccess.setPosition(new long[] {x-1,y-1,z-1});
						
						//rescale to 0-255
						outputRandomAccess.get().set(sheetnessMeasureBin);
					}
				}
			}
		}
		return output;
	}
	
	
	/**
	 * Function to write out histograms.
	 * 
	 * @param histogramMaps 	Maps containing histograms
	 * @param outputDirectory	Output directory to write files to
	 * @param datasetName		Dataset name that is being analyzed
	 * @throws IOException
	 */
	
	public static void writeData(HistogramMaps histogramMaps, String outputDirectory, String datasetName) throws IOException {
		if (! new File(outputDirectory).exists()){
			new File(outputDirectory).mkdirs();
	    }
		
		FileWriter sheetnessVolumeAndAreaHistograms = new FileWriter(outputDirectory+"/"+datasetName+"_sheetnessVolumeAndAreaHistograms.csv");
		sheetnessVolumeAndAreaHistograms.append("Sheetness,Volume (nm^3),Surface Area(nm^2)\n");
		
		FileWriter sheetnessVsThicknessHistogram = new FileWriter(outputDirectory+"/"+datasetName+"_sheetnessVsThicknessHistogram.csv");
		String rowString = "Sheetness/Thickness (nm)";
		for(int thicknessBin=0; thicknessBin<100; thicknessBin++) {
			rowString+=","+Integer.toString(thicknessBin*8+4);
		}
		sheetnessVsThicknessHistogram.append(rowString+"\n");
		
		for(int sheetnessBin=0;sheetnessBin<256;sheetnessBin++) {
			double volume = histogramMaps.sheetnessAndVolumeHistogram.getOrDefault(sheetnessBin, 0.0);
			double surfaceArea = histogramMaps.sheetnessAndSurfaceAreaHistogram.getOrDefault(sheetnessBin, 0.0);
		
			String sheetnessBinString = Double.toString(sheetnessBin/256.0+0.5/256.0);
			sheetnessVolumeAndAreaHistograms.append(sheetnessBinString+","+Double.toString(volume)+","+Double.toString(surfaceArea)+"\n");
			
			rowString =sheetnessBinString;
			for(int thicknessBin=0; thicknessBin<100; thicknessBin++) {
				double thicknessCount = histogramMaps.sheetnessAndThicknessHistogram.getOrDefault(Arrays.asList(sheetnessBin,thicknessBin),0L);
				rowString+=","+Double.toString(thicknessCount);
			}
			sheetnessVsThicknessHistogram.append(rowString+"\n");
		}
		sheetnessVolumeAndAreaHistograms.flush();
		sheetnessVolumeAndAreaHistograms.close();
		
		sheetnessVsThicknessHistogram.flush();
		sheetnessVsThicknessHistogram.close();
		
		
	}
	
	/**
	 * Take input args and perform the calculation
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

		final SparkConf conf = new SparkConf().setAppName("SparkCalculatePropertiesOfMedialSurface");
		
		List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), options.getInputN5DatasetName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		HistogramMaps histogramMaps = projectCurvatureToSurface(sc, options.getInputN5Path(), options.getInputN5DatasetName(), options.getOutputN5Path(), blockInformationList);
		writeData(histogramMaps, options.getOutputDirectory(), options.getInputN5DatasetName());
		
		sc.close();
	}
	
}

class HistogramMaps implements Serializable{
	/**
	 * Class to contain histograms as maps for sheetness, thickness, surface area and volume
	 */
	private static final long serialVersionUID = 1L;
	public Map<List<Integer>,Long> sheetnessAndThicknessHistogram;
	public Map<Integer,Double> sheetnessAndSurfaceAreaHistogram;
	public Map<Integer,Double> sheetnessAndVolumeHistogram;

	public HistogramMaps(Map<List<Integer>, Long> sheetnessAndThicknessHistogram,Map<Integer,Double> sheetnessAndSurfaceAreaHistogram, Map<Integer,Double> sheetnessAndVolumeHistogram){
		this.sheetnessAndThicknessHistogram = sheetnessAndThicknessHistogram;
		this.sheetnessAndSurfaceAreaHistogram = sheetnessAndSurfaceAreaHistogram;
		this.sheetnessAndVolumeHistogram = sheetnessAndVolumeHistogram;
	}
	
	public void merge(HistogramMaps newHistogramMaps) {
		//merge holeIDtoObjectIDMap
		for(Entry<List<Integer>,Long> entry : newHistogramMaps.sheetnessAndThicknessHistogram.entrySet()) 
			sheetnessAndThicknessHistogram.put(entry.getKey(), sheetnessAndThicknessHistogram.getOrDefault(entry.getKey(), 0L) + entry.getValue() );

		//merge holeIDtoVolumeMap
		for(Entry<Integer,Double> entry : newHistogramMaps.sheetnessAndSurfaceAreaHistogram.entrySet())
			sheetnessAndSurfaceAreaHistogram.put(entry.getKey(), sheetnessAndSurfaceAreaHistogram.getOrDefault(entry.getKey(), 0.0) + entry.getValue() );
		
		//merge objectIDtoVolumeMap
		for(Entry<Integer,Double> entry : newHistogramMaps.sheetnessAndVolumeHistogram.entrySet())
			sheetnessAndVolumeHistogram.put(entry.getKey(), sheetnessAndVolumeHistogram.getOrDefault(entry.getKey(), 0.0) + entry.getValue() );
	
	}
	
}


