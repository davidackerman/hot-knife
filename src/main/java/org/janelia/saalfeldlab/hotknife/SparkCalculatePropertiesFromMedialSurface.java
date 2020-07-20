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
import net.imglib2.img.NativeImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

import org.janelia.saalfeldlab.hotknife.CorrectlyPaddedDistanceTransform;
/**
 * Get surface area, volume and thickness of volume based on medial surface.
 * Thickness is calculatued from medial surface, and surface area and thickness are calculated from a volume recreated from the medial surface.
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkCalculatePropertiesFromMedialSurface {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path")
		private String inputN5Path = null;

		@Option(name = "--outputDirectory", required = false, usage = "output directory")
		private String outputDirectory = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "input N5 dataset")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path")
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
	 * @param sc
	 * @param n5Path
	 * @param datasetName
	 * @param n5OutputPath
	 * @param blockInformationList
	 * @return
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
			
			RandomAccessibleInterval<UnsignedLongType> segmentation = (RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, datasetName);
			CorrectlyPaddedDistanceTransform correctlyPaddedDistanceTransform =  new CorrectlyPaddedDistanceTransform(segmentation, offset, dimension);
			long[] padding = correctlyPaddedDistanceTransform.padding;
			long [] paddedOffset = correctlyPaddedDistanceTransform.paddedOffset;
			long [] paddedDimension = correctlyPaddedDistanceTransform.paddedDimension;
			NativeImg<FloatType, ?> distanceTransform = correctlyPaddedDistanceTransform.correctlyPaddedDistanceTransform;
			
			IntervalView<UnsignedLongType> medialSurface = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5BlockReader, datasetName+"_medialSurface")
					),paddedOffset, paddedDimension);
			
			RandomAccessibleInterval<DoubleType> sheetness = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<DoubleType>) N5Utils.open(n5BlockReader, datasetName+"_sheetness")
					),paddedOffset, paddedDimension);
			
			final Img<FloatType> sheetnessSum = new ArrayImgFactory<FloatType>(new FloatType())
					.create(new long[]{blockSize[0]+2,blockSize[1]+2,blockSize[2]+2}); //add an extra 2 because need to check for surface voxels so need extra border of 1
			final Img<UnsignedIntType> counts = new ArrayImgFactory<UnsignedIntType>(new UnsignedIntType())
					.create(new long[]{blockSize[0]+2,blockSize[1]+2,blockSize[2]+2});
			
			Cursor<UnsignedLongType> medialSurfaceCursor = medialSurface.cursor();
			RandomAccess<FloatType> distanceTransformRandomAccess = distanceTransform.randomAccess();
			RandomAccess<DoubleType> sheetnessRandomAccess = sheetness.randomAccess();
			RandomAccess<FloatType> sheetnessSumRandomAccess = sheetnessSum.randomAccess();
			RandomAccess<UnsignedIntType> countsRandomAccess = counts.randomAccess();
			
			Map<List<Integer>,Long> sheetnessAndThicknessHistogram = new HashMap<List<Integer>,Long>();
			
			while (medialSurfaceCursor.hasNext()) {
				final long medialSurfaceValue = medialSurfaceCursor.next().get();
				if ( medialSurfaceValue >0 ) { // then it is on medial surface
					int [] pos = {medialSurfaceCursor.getIntPosition(0),medialSurfaceCursor.getIntPosition(1),medialSurfaceCursor.getIntPosition(2) };
					distanceTransformRandomAccess.setPosition(pos);
					sheetnessRandomAccess.setPosition(pos);

					float radiusSquared = distanceTransformRandomAccess.get().getRealFloat();
					double radius = Math.sqrt(radiusSquared);
					
					int radiusPlusPadding = (int) Math.ceil(radius);
					
					float sheetnessMeasure = sheetnessRandomAccess.get().getRealFloat();					
					int sheetnessMeasureBin = (int) Math.floor(sheetnessMeasure*256);
					
					if(pos[0]>=padding[0] && pos[0]<paddedDimension[0]-padding[0] &&
							pos[1]>=padding[1] && pos[1]<paddedDimension[1]-padding[1] &&
							pos[2]>=padding[2] && pos[2]<paddedDimension[2]-padding[2]) {
						
						double thickness = radius*2;//convert later
						int thicknessBin = (int) Math.min(Math.floor(thickness*pixelResolution[0]/8),99); //bin thickness in 8 nm bins
						
						List<Integer> histogramBinList = Arrays.asList(sheetnessMeasureBin,thicknessBin);
						sheetnessAndThicknessHistogram.put(histogramBinList,sheetnessAndThicknessHistogram.getOrDefault(histogramBinList,0L)+1L);
					}
					
					for(int x = pos[0]-radiusPlusPadding; x<=pos[0]+radiusPlusPadding; x++) {
						for(int y = pos[1]-radiusPlusPadding; y<=pos[1]+radiusPlusPadding; y++) {
							for(int z = pos[2]-radiusPlusPadding; z<=pos[2]+radiusPlusPadding; z++) {
								int dx = x-pos[0];
								int dy = y-pos[1];
								int dz = z-pos[2];
								//need to check extra padding of 1 because in next step we need this halo for checking surfaces
								if((x>=padding[0]-1 && x<=paddedDimension[0]-padding[0] && y>=padding[1]-1 && y <= paddedDimension[1]-padding[1] && z >= padding[2]-1 && z <= paddedDimension[2]-padding[2]) && dx*dx+dy*dy+dz*dz<= radiusSquared ) { //then it is in sphere
									long [] spherePos = new long[]{x-(padding[0]-1),y-(padding[1]-1),z-(padding[2]-1)};
									sheetnessSumRandomAccess.setPosition(spherePos);
									FloatType outputVoxel = sheetnessSumRandomAccess.get();
									outputVoxel.set(outputVoxel.get()+sheetnessMeasure);
									
									countsRandomAccess.setPosition(spherePos);
									UnsignedIntType countsVoxel = countsRandomAccess.get();
									countsVoxel.set(countsVoxel.get()+1);
																			
								}
							}
						}
					}
					
				}
			}
			
			medialSurface = null;
			distanceTransform = null;
			sheetness = null;

			Map<Integer,Double> sheetnessAndSurfaceAreaHistogram = new HashMap<Integer,Double>();
			Map<Integer,Double> sheetnessAndVolumeHistogram = new HashMap<Integer,Double>();
			List<long[]> voxelsToCheck = new ArrayList<long[]>(); 
			voxelsToCheck.add(new long[] {-1, 0, 0});
			voxelsToCheck.add(new long[] {1, 0, 0});
			voxelsToCheck.add(new long[] {0, -1, 0});
			voxelsToCheck.add(new long[] {0, 1, 0});
			voxelsToCheck.add(new long[] {0, 0, -1});
			voxelsToCheck.add(new long[] {0, 0, 1});
			
			final Img<UnsignedByteType> output = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType())
					.create(dimension);
			RandomAccess<UnsignedByteType> outputRandomAccess = output.randomAccess();
			for(long x=1; x<dimension[0]+1;x++) {
				for(long y=1; y<dimension[1]+1;y++) {
					for(long z=1; z<dimension[2]+1;z++) {
						long [] pos = new long[]{x,y,z};
						sheetnessSumRandomAccess.setPosition(pos);
						countsRandomAccess.setPosition(pos);
						if(countsRandomAccess.get().get()>0) {
							float sheetnessMeasure = sheetnessSumRandomAccess.get().get()/countsRandomAccess.get().get();
							sheetnessSumRandomAccess.get().set(sheetnessMeasure);//take average
							//System.out.println(sheetnessMeasure);
							int sheetnessMeasureBin = (int) Math.floor(sheetnessMeasure*256);
							sheetnessAndVolumeHistogram.put(sheetnessMeasureBin, sheetnessAndVolumeHistogram.getOrDefault(sheetnessMeasureBin,0.0)+voxelVolume);
							int faces = getSurfaceAreaContributionOfVoxelInFaces(countsRandomAccess, voxelsToCheck);
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
	
	public static int getSurfaceAreaContributionOfVoxelInFaces(final RandomAccess<UnsignedIntType> countsRandomAccess, List<long[]> voxelsToCheck) {
		final long pos[] = {countsRandomAccess.getLongPosition(0), countsRandomAccess.getLongPosition(1), countsRandomAccess.getLongPosition(2)};
		int surfaceAreaContributionOfVoxelInFaces = 0;


		for(long[] currentVoxel : voxelsToCheck) {
			final long currentPosition[] = {pos[0]+currentVoxel[0], pos[1]+currentVoxel[1], pos[2]+currentVoxel[2]};
			countsRandomAccess.setPosition(currentPosition);
			if(countsRandomAccess.get().get() ==0) {
				surfaceAreaContributionOfVoxelInFaces ++;
			}
		}

		return surfaceAreaContributionOfVoxelInFaces;	
	
	}
	
	public static List<long[]> getSurfaceVoxels(RandomAccess<UnsignedLongType> sourceRA, long [] padding, long [] dimension){
		ArrayList<long[]> surfaceVoxels = new ArrayList<long[]>();
		for(long x=padding[0]; x<padding[0]+dimension[0]; x++) {
			for(long y=padding[1]; y<padding[1]+dimension[1]; y++) {
				for(long z=padding[2]; z<padding[2]+dimension[2]; z++) {
					long [] pos = new long[] {x,y,z};
					if(SparkContactSites.isSurfaceVoxel(sourceRA, pos)) {
						surfaceVoxels.add(pos);
					}
				}
			}
		}
		return surfaceVoxels;
	}
	

	
	public static List<long[]> bressenham3D(long [] start, long [] end){
	// https://www.geeksforgeeks.org/bresenhams-algorithm-for-3-d-line-drawing/
	// Python3 code for generating points on a 3-D line  
	// using Bresenham's Algorithm 
		long x1 = start[0]; long y1 = start[1]; long z1 = start[2];
		long x2 = end[0]; long y2 = end[1]; long z2 = end[2];

	    ArrayList<long[]> listOfPoints = new ArrayList<long[]>();
	    listOfPoints.add(start);
	    long dx = Math.abs(x2 - x1);
	    long dy = Math.abs(y2 - y1);
	    long dz = Math.abs(z2 - z1);
	    
	    long xs, ys, zs;
	    if (x2 > x1) 
	        xs = 1;
	    else 
	        xs = -1;
	    if (y2 > y1) 
	        ys = 1;
	    else 
	        ys = -1;
	    if (z2 > z1) 
	        zs = 1;
	    else 
	        zs = -1;
	  
	    //# Driving axis is X-axis" 
	    if (dx >= dy && dx >= dz) {         
	        long p1 = 2 * dy - dx; 
	        long p2 = 2 * dz - dx ;
	        while (x1 != x2) { 
	            x1 += xs; 
	            if (p1 >= 0) { 
	                y1 += ys; 
	                p1 -= 2 * dx; 
	            }
	            if (p2 >= 0) { 
	                z1 += zs; 
	                p2 -= 2 * dx;
	            }
	            p1 += 2 * dy; 
	            p2 += 2 * dz; 
	    	    listOfPoints.add(new long [] {x1, y1, z1});
	        }
	    }
	    
	    //# Driving axis is Y-axis" 
	    else if (dy >= dx && dy >= dz) {        
	        long p1 = 2 * dx - dy; 
	        long p2 = 2 * dz - dy; 
	        while (y1 != y2) { 
	            y1 += ys; 
	            if (p1 >= 0){ 
	                x1 += xs; 
	                p1 -= 2 * dy; 
	            }
	            if (p2 >= 0) { 
	                z1 += zs; 
	                p2 -= 2 * dy;
	            }
	            p1 += 2 * dx; 
	            p2 += 2 * dz; 
	    	    listOfPoints.add(new long [] {x1, y1, z1});
	        }
	    }
	    //# Driving axis is Z-axis" 
	    else{         
	        long p1 = 2 * dy - dz; 
	        long p2 = 2 * dx - dz; 
	        while (z1 != z2) { 
	            z1 += zs; 
	            if (p1 >= 0) { 
	                y1 += ys; 
	                p1 -= 2 * dz;
	            }
	            if (p2 >= 0) { 
	                x1 += xs; 
	                p2 -= 2 * dz; 
	            }
	            p1 += 2 * dy; 
	            p2 += 2 * dx;
	    	    listOfPoints.add(new long [] {x1, y1, z1});
	        }
	    }
	    return listOfPoints;
	}
	
	public static void writeData(HistogramMaps histogramMaps, String outputDirectory, String organelle1) throws IOException {
		if (! new File(outputDirectory).exists()){
			new File(outputDirectory).mkdirs();
	    }
		
		FileWriter sheetnessVolumeAndAreaHistograms = new FileWriter(outputDirectory+"/"+organelle1+"_sheetnessVolumeAndAreaHistograms.csv");
		sheetnessVolumeAndAreaHistograms.append("Sheetness,Volume (nm^3),Surface Area(nm^2)\n");
		
		FileWriter sheetnessVsThicknessHistogram = new FileWriter(outputDirectory+"/"+organelle1+"_sheetnessVsThicknessHistogram.csv");
		String rowString = "Sheetness/Thickness (nm)";
		for(int thicknessBin=0; thicknessBin<100; thicknessBin++) {
		//	int thicknessBinStart = thicknessBin*2-1;
			rowString+=","+Integer.toString(thicknessBin*8+4);
		}
		sheetnessVsThicknessHistogram.append(rowString+"\n");
		
		for(int sheetnessBin=0;sheetnessBin<256;sheetnessBin++) {
			double volume = histogramMaps.sheetnessAndVolumeHistogram.getOrDefault(sheetnessBin, 0.0);
			double surfaceArea = histogramMaps.sheetnessAndSurfaceAreaHistogram.getOrDefault(sheetnessBin, 0.0);
			//double sheetnessBinStart = Math.max(sheetnessBin-0.5, 0.0)/100.0;
			//double sheetnessBinEnd = Math.min(sheetnessBinStart+0.1,1);
			//String sheetnessBinString = "["+Double.toString(sheetnessBinStart)+"-"+Double.toString(sheetnessBinStart);
			//			sheetnessBinString = sheetnessBin==99 ? sheetnessBinString+"]" : sheetnessBinString+")"; 

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
	
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {
		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkCalculatePropertiesOfMedialSurface");
		
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
			HistogramMaps histogramMaps = projectCurvatureToSurface(sc, options.getInputN5Path(), options.getInputN5DatasetName(), options.getOutputN5Path(), blockInformationList);
			writeData(histogramMaps, options.getOutputDirectory(), currentOrganelle);
			
			sc.close();
		}
		//Remove temporary files

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


