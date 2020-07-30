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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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

import ij.ImageJ;
import ij.ImagePlus;
import net.imagej.ops.OpService;
import net.imagej.ops.image.watershed.Watershed;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.algorithm.neighborhood.Neighborhood;
import net.imglib2.algorithm.neighborhood.RectangleShape;
import net.imglib2.algorithm.region.hypersphere.HyperSphere;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.ImgFactory;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.roi.labeling.ImgLabeling;
import net.imglib2.type.logic.BitType;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

import org.janelia.saalfeldlab.hotknife.IOHelper;
import org.janelia.saalfeldlab.hotknife.ops.SimpleGaussRA;


/**
 * Threshold a prediction but label it using another segmented volume's ids.
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkRibosomeConnectedComponents {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "Path to input N5")
		private String inputN5Path = null;
		
		@Option(name = "--outputN5Path", required = false, usage = "Output N5 path")
		private String outputN5Path = null;
		
		@Option(name = "--sigmaNm", required = false, usage = "Gaussian smoothing sigma (nm)")
		private double sigmaNm = 2;
		
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
		
		public String getOutputN5Path() {
			return outputN5Path;
		}
		
		public double getSigmaNm() {
			return sigmaNm;
		}
		
	}

	
	/**
	 * Method that relabels predictions above a certain threshold with the connected component object ID they are within.
	 * 
	 * @param sc								Spark context
	 * @param predictionN5Path					N5 path to predictions
	 * @param predictionDatasetName				Name of predictions
	 * @param connectedComponentsN5Path			N5 path to connected components
	 * @param connectedComponentsDatasetName	Name of connected components
	 * @param outputN5Path						N5 path to output
	 * @param thresholdIntensityCutoff			Threshold intensity cutoff
	 * @param blockInformationList				List of block information
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final void getRibosomeConnectedComponents(
			final JavaSparkContext sc, final String inputN5Path, final String outputN5Path, final double sigmaNm, List<BlockInformation> blockInformationList) throws IOException {

		final String inputN5DatasetName = "ribosomes";
		// Get attributes of input data sets.
		final N5Reader predictionN5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = predictionN5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();
		final double [] pixelResolution = IOHelper.getResolution(predictionN5Reader, inputN5DatasetName);
				
		// Create output dataset
		final N5Writer n5Writer = new N5FSWriter(outputN5Path);
		final String outputCentersDatasetName = inputN5DatasetName+"_centers";
		n5Writer.createGroup(outputCentersDatasetName);
		n5Writer.createDataset(outputCentersDatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
		n5Writer.setAttribute(outputCentersDatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
		
		final String outputSpheresDatasetName = inputN5DatasetName+"_spheres";
		n5Writer.createGroup(outputSpheresDatasetName);
		n5Writer.createDataset(outputSpheresDatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
		n5Writer.setAttribute(outputSpheresDatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
		
		
		double ribosomeRadiusInNm = 10.0;
		double [] ribosomeRadiusInVoxels = 	new double[] {ribosomeRadiusInNm/pixelResolution[0],ribosomeRadiusInNm/pixelResolution[0],ribosomeRadiusInNm/pixelResolution[0]}; //gives 3 pixel sigma at 4 nm resolution

		
		// Do the labeling, parallelized over blocks
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		
		rdd.foreach(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			
			double [] sigma = new double[] {sigmaNm/pixelResolution[0],sigmaNm/pixelResolution[0],sigmaNm/pixelResolution[0]}; //gives 3 pixel sigma at 4 nm resolution
			int[] sizes = Gauss3.halfkernelsizes( sigma );
			long padding = (long) (sizes[0]+2*Math.ceil(ribosomeRadiusInVoxels[0])+3);//add extra padding so if a ribosome appears in adjacent block, will be correctly shown in current block
			long [] paddedOffset = new long [] {offset[0]-padding,offset[1]-padding,offset[2]-padding};
			long [] paddedDimension = new long [] {dimension[0]+2*padding,dimension[1]+2*padding,dimension[2]+2*padding};
			
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			/* RandomAccessibleInterval<UnsignedByteType> rawPredictions = Views.offsetInterval(Views.extendMirrorSingle(
					(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)
					),paddedOffset, paddedDimension); */
			RandomAccessibleInterval<UnsignedByteType> rawPredictions = 
					(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName);
			/*final Img<UnsignedByteType> smoothedPredictions =  new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType()).create(paddedDimension);	
			SimpleGaussRA<UnsignedByteType> gauss = new SimpleGaussRA<UnsignedByteType>(sigma);
			gauss.compute(rawPredictions, smoothedPredictions);*/
			
	        
			CorrectlyPaddedDistanceTransform cpdt = new CorrectlyPaddedDistanceTransform(rawPredictions, paddedOffset, paddedDimension, 127);
			long [] updatedPadding = new long[] {cpdt.padding[0]+padding,cpdt.padding[1]+padding,cpdt.padding[2]+padding};

			Img< UnsignedLongType > outputCenters = new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType()).create(cpdt.paddedDimension);
	        Img< UnsignedLongType > outputSpheres = new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType()).create(cpdt.paddedDimension);
	        RandomAccessibleInterval<UnsignedByteType> rawPredictionsCropped = Views.offsetInterval(rawPredictions,cpdt.paddedOffset, cpdt.paddedDimension);
	        Img< IntType > sphereness = new ArrayImgFactory<IntType>(new IntType()).create(cpdt.paddedDimension);
	     
	        //  doWatershedding(rawPredictions);
	        getSphereness(rawPredictionsCropped.randomAccess(), sphereness.randomAccess(), ribosomeRadiusInVoxels[0], paddedDimension);
	        //keepNonOverlappingSpheres(outputCenters);

		    findAndDisplayLocalMaxima(cpdt.correctlyPaddedDistanceTransform, sphereness.randomAccess(), outputCenters.randomAccess(), outputSpheres.randomAccess(), ribosomeRadiusInVoxels[0], cpdt.paddedOffset, cpdt.paddedDimension, outputDimensions);

	       //findAndDisplayLocalMaxima(sphereness, outputCenters.randomAccess(), outputSpheres.randomAccess(), ribosomeRadiusInVoxels[0], paddedOffset, paddedDimension, outputDimensions);
			
			// Write out output to temporary n5 stack
			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(Views.offsetInterval(outputCenters, updatedPadding, dimension), n5WriterLocal, outputCentersDatasetName, gridBlock[2]);
			N5Utils.saveBlock(Views.offsetInterval(outputSpheres, updatedPadding, dimension), n5WriterLocal, outputSpheresDatasetName, gridBlock[2]);

		});
	}
	
	
	public static Set<List<Long>> keepNonOverlappingSpheres(Set<List<Long>> centers, int centerSeparationInVoxels, long [] paddedOffset){
		Set<List<Long>> keptCenters = new HashSet<List<Long>>();
		//simpleBorderPoints.get((x+paddedOffset[0])%centerSeparationInVoxels+((y+paddedOffset[1])%centerSeparationInVoxels)*centerSeparationInVoxels+((z+paddedOffset[2])%centerSeparationInVoxels)*Math.pow(centerSeparationInVoxels,2)).add(index);

		return centers;
	}
	
	public static void doWatershedding(RandomAccessibleInterval<UnsignedByteType> ra) {
		//https://forum.image.sc/t/watershed-segmentation-using-ops/24713/3
		//Thresold the Output mask from fiji (where 0 is false and 255 is true)
		final RandomAccessibleInterval<NativeBoolType> mask = Converters.convert(
				ra,
				(a, b) -> {
					b.set(a.getIntegerLong()>=127);
				},
				new NativeBoolType());
		//Fill the holes
		//maskFilled = ij.op().morphology().fillHoles(maskBitType);

		//Perform the watershed
		boolean useEightConnectivity=true;
		boolean drawWatersheds=false;
		double sigma=2.0;
		net.imagej.ImageJ ij = new net.imagej.ImageJ();
		ImgLabeling<Integer, IntType> watershedImgLabeling = ij.op().image().watershed(null,mask,useEightConnectivity,drawWatersheds,sigma,mask);
		
		ImageJ temp = new ij.ImageJ();
		//Display the result
		RandomAccessibleInterval<IntType> watershedImg = watershedImgLabeling.getIndexImg();
		ImagePlus watershedImgLabelingImp = ImageJFunctions.wrap(watershedImg, "wrapped");
		watershedImgLabelingImp.show();
		
	}
	public static void getSphereness(RandomAccess<UnsignedByteType> rawPredictionsRA, RandomAccess<IntType> spherenessRA, double radius, long[] dimensions){
		Set<List<Long>> spherePoints = getSpherePoints(radius);

		for(long x=0; x<dimensions[0]; x++) {
			for(long y=0; y<dimensions[1]; y++) {
				for(long z=0; z<dimensions[2]; z++) {
					long [] sphereCenter = new long [] {x,y,z};
					rawPredictionsRA.setPosition(sphereCenter);
					if(rawPredictionsRA.get().get()>=127) {//center is greater than 127
						int countForSphereCenteredAtxyz = 0;
					
						for(List<Long> currentSpherePoint : spherePoints) {
							long[] sphereCoordinate = new long [] {x+currentSpherePoint.get(0),y+currentSpherePoint.get(1),z+currentSpherePoint.get(2)};
							if(sphereCoordinate[0]>=0 && sphereCoordinate[1]>=0 && sphereCoordinate[2]>=0 && 
								sphereCoordinate[0]<dimensions[0] && sphereCoordinate[1]<dimensions[1] && sphereCoordinate[2]<dimensions[2]) {
								rawPredictionsRA.setPosition(sphereCoordinate);
								if(rawPredictionsRA.get().get()>=127) {
									countForSphereCenteredAtxyz ++;
								}
							}
						}
						if(countForSphereCenteredAtxyz>spherePoints.size()/10) {
							spherenessRA.setPosition(new long[] {x,y,z});
							spherenessRA.get().set(countForSphereCenteredAtxyz);
						}
					}
				}
			}
		}
	}
	
	/**
	 * Checks all pixels in the image if they are a local maxima and
     * labels the centers and creates sphere around them
	 * 
	 * @param <T>
	 * @param <U>
	 * @param source					Source interval for finding maxima
	 * @param centersRA					Random accessible for storing maxima centers
	 * @param spheresRA					Random accessible for storing maxima spheres
	 * @param ribosomeRadiusInVoxels	Radius of ribosomes in voxels
	 * @param paddedOffset				Padded offset
	 * @param paddedDimension			Padded dimensions
	 * @param overallDimensions			Overall dimensions of image
	 */
    public static <T extends RealType<T>,U extends IntegerType< U >, V extends IntegerType< V >>
        void findAndDisplayLocalMaxima(
            RandomAccessibleInterval< T > source,
            RandomAccess<V> spherenessRA,
            RandomAccess< U > centersRA,
            RandomAccess< U > spheresRA,
            double ribosomeRadiusInVoxels,
            long paddedOffset[],
            long paddedDimension[],
            long overallDimensions[]
            )
    { 
        // define an interval that is one pixel smaller on each side in each dimension,
        // so that the search in the 8-neighborhood (3x3x3...x3) never goes outside
        // of the defined interval
        Interval interval = Intervals.expand( source, -1 );
 
        // create a view on the source with this interval
        source = Views.interval( source, interval );
 
        // create a Cursor that iterates over the source and checks in a 8-neighborhood
        // if it is a minima
        final Cursor< T > center = Views.iterable( source ).cursor();
        
        // instantiate a RectangleShape to access rectangular local neighborhoods
        // of radius 1 (that is 3x3x...x3 neighborhoods), skipping the center pixel
        // (this corresponds to an 8-neighborhood in 2d or 26-neighborhood in 3d, ...)
        final RectangleShape shape = new RectangleShape( 1, true );

        // iterate over the set of neighborhoods in the image
        for ( final Neighborhood< T > localNeighborhood : shape.neighborhoods( source ) )
        {
            // what is the value that we investigate?
            // (the center cursor runs over the image in the same iteration order as neighborhood)
            final T centerValue = center.next();
            if(centerValue.getRealDouble()>0) {//Then is above threshold
            	// keep this boolean true as long as no other value in the local neighborhood
	            // is larger or equal
	            boolean isMaximum = true;
	 
	            // check if all pixels in the local neighborhood that are smaller
	            for ( final T value : localNeighborhood )
	            {
	                // test if the center is smaller than the current pixel value
	                if ( centerValue.compareTo( value ) < 0 )
	                {
	                    isMaximum = false;
	                    break;
	                }
	            }
	 
	            if ( isMaximum)
	            {
	            	long [] centerPosition = new long [] {center.getLongPosition(0), center.getLongPosition(1), center.getLongPosition(2)};//add 1 because of -1 earlier
	            	long [] overallCenterPosition = {centerPosition[0]+paddedOffset[0],centerPosition[1]+paddedOffset[1],centerPosition[2]+paddedOffset[2]};
	                
	            	if(overallCenterPosition[0]>=0 && overallCenterPosition[1]>=0 && overallCenterPosition[2]>=0 && 
	            			overallCenterPosition[0]<overallDimensions[0] && overallCenterPosition[1]<overallDimensions[1] && overallCenterPosition[2]<overallDimensions[2]) {
	                	long setValue = SparkCosemHelper.convertPositionToGlobalID(overallCenterPosition, overallDimensions);
	                	
	                	//set center to setValue
	                	centersRA.setPosition(centerPosition);
	                	centersRA.get().setInteger(setValue);

	                	// draw a sphere of radius one in the new image
	                	fillSphereWithValue( spheresRA, centerPosition, ribosomeRadiusInVoxels, setValue, paddedDimension );
	                }
	            }
	        } 					
        }
    }
    
    public static < T extends IntegerType< T > >void fillSphereWithValue(RandomAccess<T> ra, long [] center, double radius, long setValue, long[] dimensions){
    	long radiusCeiling = (long) Math.ceil(radius);
    	long radiusCeilingSquared = radiusCeiling*radiusCeiling;
    	for(long dx=-radiusCeiling; dx<=radiusCeiling; dx++) {
    		for(long dy=-radiusCeiling; dy<=radiusCeiling; dy++) {
    			for(long dz=-radiusCeiling; dz<=radiusCeiling; dz++) {
    				if(dx*dx+dy*dy+dz*dz<=radiusCeilingSquared) {
    					long[] positionInSphere = new long[] {center[0]+dx, center[1]+dy, center[2]+dz};
    					if(positionInSphere[0]>=0 && positionInSphere[1]>=0 && positionInSphere[2]>=0 &&
    							positionInSphere[0]<dimensions[0] && positionInSphere[1]<dimensions[1] && positionInSphere[2]<dimensions[2]) {
    						ra.setPosition(positionInSphere);
	    					ra.get().setInteger(setValue);
    					}
    				}
    			}
    		}
    	}
    }
    
    public static Set<List<Long>> getSpherePoints(double radius){
    	Set<List<Long>> pointsInSphere = new HashSet<List<Long>>();
    	long radiusCeiling = (long) Math.ceil(radius);
    	long radiusCeilingSquared = radiusCeiling*radiusCeiling;
    	for(long dx=-radiusCeiling; dx<=radiusCeiling; dx++) {
    		for(long dy=-radiusCeiling; dy<=radiusCeiling; dy++) {
    			for(long dz=-radiusCeiling; dz<=radiusCeiling; dz++) {
    				if(dx*dx+dy*dy+dz*dz<=radiusCeilingSquared) {
    					pointsInSphere.add(Arrays.asList(dx,dy,dz));
    					}
    				}
    			}
    		}
    	return pointsInSphere;
    }
    
    
	/**
	 * Take input arguments and produce ribosome connected components
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkLabelPredictionWithConnectedComponents");
		
		//Create block information list
		List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), "ribosomes");
	
		//Run connected components
		JavaSparkContext sc = new JavaSparkContext(conf);
		getRibosomeConnectedComponents(sc, options.getInputN5Path(), options.getOutputN5Path(), options.getSigmaNm(), blockInformationList);
		sc.close();
		
	}
}

