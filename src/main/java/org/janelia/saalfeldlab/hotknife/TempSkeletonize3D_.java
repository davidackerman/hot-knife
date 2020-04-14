package org.janelia.saalfeldlab.hotknife;

/**
 * Skeletonize3D plugin for ImageJ(C).
 * Copyright (C) 2008 Ignacio Arganda-Carreras 
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation (http://www.gnu.org/licenses/gpl.txt )
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 * 
 */

import ij.IJ;
import ij.ImagePlus;
import ij.ImageStack;
import ij.plugin.filter.PlugInFilter;
import ij.process.ImageProcessor;

import java.util.ArrayList;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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

import bdv.labels.labelset.Label;
import ij.ImageJ.*;
import ij.IJ;
import ij.ImagePlus;
import net.imagej.ImageJ;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.NativeImg;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.BooleanArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.label.Test;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.SubsampleIntervalView;
import net.imglib2.view.Views;
import net.imagej.ops.morphology.thin.*;
import net.imglib2.type.logic.BitType;

import ij.plugin.*;

/**
 * Main class.
 * This class is a plugin for the ImageJ interface for 2D and 3D thinning 
 * (skeletonization) of binary images (2D/3D).
 *
 * <p>
 * This work is an implementation by Ignacio Arganda-Carreras of the
 * 3D thinning algorithm from Lee et al. "Building skeleton models via 3-D 
 * medial surface/axis thinning algorithms. Computer Vision, Graphics, and 
 * Image Processing, 56(6):462–478, 1994." Based on the ITK version from
 * Hanno Homann <a href="http://hdl.handle.net/1926/1292"> http://hdl.handle.net/1926/1292</a>
 * <p>
 *  More information at Skeletonize3D homepage:
 *  http://imagejdocu.tudor.lu/doku.php?id=plugin:morphology:skeletonize3d:start
 *
 * @version 1.0 11/19/2008
 * @author Ignacio Arganda-Carreras (iargandacarreras at gmail.com)
 *
 */
public class TempSkeletonize3D_ implements PlugInFilter 
{
	/** working image plus */
	private ImagePlus imRef;

	/** working image width */
	private int width = 0;
	/** working image height */
	private int height = 0;
	/** working image depth */
	private int depth = 0;
	/** working image stack*/
	private ImageStack inputImage = null;
    /** number of iterations thinning took */
	private int iterations;
	
	/* -----------------------------------------------------------------------*/
	/**
	 * This method is called once when the filter is loaded.
	 * 
	 * @param arg argument specified for this plugin
	 * @param imp currently active image
	 * @return flag word that specifies the filters capabilities
	 */
	public int setup(String arg, ImagePlus imp) 
	{
		this.imRef = imp;
		
		if (arg.equals("about")) {
			showAbout();
			return DONE;
		}

		return DOES_8G;
	} /* end setup */
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Process the image.
	 * 
	 * @see ij.plugin.filter.PlugInFilter#run(ij.process.ImageProcessor)
	 */
	public void run(ImageProcessor ip) 
	{
		
		this.width = this.imRef.getWidth();
		this.height = this.imRef.getHeight();
		this.depth = this.imRef.getStackSize();
		this.inputImage = this.imRef.getStack();
							
		// Prepare data
		prepareData(this.inputImage);
		
		// Compute Thinning	
		computeThinImage(this.inputImage);
		//computeMedialSurface(this.inputImage);
		
		// Convert image to binary 0-255
		for(int i = 1; i <= this.inputImage.getSize(); i++)
		//	this.inputImage.getProcessor(i).multiply(255);
		
		this.inputImage.update(ip);
	} /* end run */

	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException{ 
		new ij.ImageJ();
		ImagePlus imp = IJ.openImage("/groups/cosem/cosem/ackermand/mito_cc_filled.tif");
		//ImagePlus imp = IJ.openImage("/groups/cosem/cosem/ackermand/er_cc.tif");
		//ImagePlus imp = IJ.openImage("/groups/scicompsoft/home/ackermand/Desktop/rectangles_lee_figure12_attempt2.tif");
		//ImagePlus imp = IJ.openImage("/groups/cosem/cosem/ackermand/mito_minVolume.tif");
		imp.show();
		TempSkeletonize3D_ skeletonize3D = new TempSkeletonize3D_();
		String arg = "";
		skeletonize3D.setup(arg,imp);
		skeletonize3D.run(imp.getStack().getProcessor(1));
		imp.show();
	}
	
	
	
	public final boolean thinningForParallelization(ImagePlus inputForSkeletonization) {
		String arg = "";
		this.setup(arg,inputForSkeletonization);

		this.width = this.imRef.getWidth();
		this.height = this.imRef.getHeight();
		this.depth = this.imRef.getStackSize();
		this.inputImage = this.imRef.getStack();
							
		// Prepare data
		prepareData(this.inputImage);
		
		// Compute Thinning	
		boolean needToThinAgain = thinPaddedImageOneIteration(this.inputImage);
		
		// Convert image to binary 0-255
		for(int i = 1; i <= this.inputImage.getSize(); i++)
			this.inputImage.getProcessor(i).multiply(255);
	
		return needToThinAgain;

	}
	/* -----------------------------------------------------------------------*/
	/**
	 * Prepare data for computation.
	 * Copy the input image to the output image, changing from the input
	 * type to the output type.
	 * 
	 * @param outputImage output image stack
	 */
	public void prepareData(ImageStack outputImage) 
	{		
		// Copy the input to the output, changing all foreground pixels to
        // have value 1 in the process.
		for (int z = 0; z < depth; z++) 
			for (int x = 0; x < width; x++) 
				for (int y = 0; y < height; y++)
					if ( ((byte[]) this.inputImage.getPixels(z + 1))[x + y * width] != 0 )
						((byte[]) outputImage.getPixels(z + 1))[x + y * width] = 1;

	} /* end prepareData */
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Post processing for computing thinning.
	 * 
	 * @param outputImage output image stack
	 */
	public boolean thinPaddedImageOneIteration(ImageStack outputImage) 
	{						
		// Prepare Euler LUT [Lee94]
		int eulerLUT[] = new int[256]; 
		fillEulerLUT( eulerLUT );
		
		// Prepare number of points LUT
		int pointsLUT[] = new int[ 256 ];
		fillnumOfPointsLUT(pointsLUT);
		
		// Following Lee[94], save versions (Q) of input image S, while 
		// deleting each type of border points (R)

		ArrayList<ArrayList<int[]>> simpleBorderPoints = new ArrayList<ArrayList<int[]>>();
		for(int i=0; i<8; i++) {
			simpleBorderPoints.add(new ArrayList<int[]>());
		}

		iterations = 0;
		// Loop through the image several times until there is no change.
		int unchangedBorders = 0;
		{						
			unchangedBorders = 0;
			iterations++;
			for( int currentBorder = 1; currentBorder <= 6; currentBorder++)
			{
				IJ.showStatus("Thinning iteration " + iterations + " (" + currentBorder +"/6 borders) ...");

				boolean noChange = true;				
				
				// Loop through the image.				 
				for (int z = 0; z < depth; z++)
				{
					for (int y = 0; y < height; y++)
					{
						for (int x = 0; x < width; x++)						
						{

							// check if point is foreground
							if ( getPixelNoCheck(outputImage, x, y, z) != 1 )
							{
								continue;         // current point is already background 
							}
																				
							// check 6-neighbors if point is a border point of type currentBorder
							boolean isBorderPoint = false;
							// North
							if( currentBorder == 3 && N(outputImage, x, y, z) <= 0 )
								isBorderPoint = true;
							// South
							if( currentBorder == 4 && S(outputImage, x, y, z) <= 0 )
								isBorderPoint = true;
							// East
							if( currentBorder == 6 && E(outputImage, x, y, z) <= 0 )
								isBorderPoint = true;
							// West
							if( currentBorder == 5 && W(outputImage, x, y, z) <= 0 )
								isBorderPoint = true;
							if(outputImage.getSize() > 1)
							{
								// Up							
								if( currentBorder == 1 && U(outputImage, x, y, z) <= 0 )
									isBorderPoint = true;
								// Bottom
								if( currentBorder == 2 && B(outputImage, x, y, z) <= 0 )
									isBorderPoint = true;
							}
							if( !isBorderPoint )
							{
								continue;         // current point is not deletable
							}

							if( isEndPoint( outputImage, x, y, z))
							{
								continue;
							}

							final byte[] neighborhood = getNeighborhood(outputImage, x, y, z);
							
							// Check if point is Euler invariant (condition 1 in Lee[94])
							if( !isEulerInvariant( neighborhood, eulerLUT ) )
							{
								continue;         // current point is not deletable
							}

							// Check if point is simple (deletion does not change connectivity in the 3x3x3 neighborhood)
							// (conditions 2 and 3 in Lee[94])
							if( !isSimplePoint( neighborhood ) )
							{
								continue;         // current point is not deletable
							}



							// add all simple border points to a list for sequential re-checking
							int[] index = new int[3];
							index[0] = x;
							index[1] = y;
							index[2] = z;
							simpleBorderPoints.get((x%2) + (y%2)*2 + (z%2)*4).add(index);
						}
					}					
				}							


				// sequential re-checking to preserve connectivity when
				// deleting in a parallel way
				int[] index;
				for (ArrayList<int[]> subvolumeSimpleBorderPoints : simpleBorderPoints) {
				for (int[] simpleBorderPoint : subvolumeSimpleBorderPoints) {
					index = simpleBorderPoint;

					// Check if border points is simple			        
					if (isSimplePoint(getNeighborhood(outputImage, index[0], index[1], index[2]))) {
						// we can delete the current point
						setPixel(outputImage, index[0], index[1], index[2], (byte) 0);
						if((index[0]>0 && index[0]<width-1) && (index[1]>0 && index[1]<height-1) && (index[2]>0 && index[2]<depth-1)) //Don't care about changes to padded part
							noChange = false;
					}


				}
				}
				if( noChange )
					unchangedBorders++;

				simpleBorderPoints.clear();
			} // end currentBorder for loop
		}

		IJ.showStatus("Computed thin image.");
		return unchangedBorders<6;
	} 	
	
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Post processing for computing thinning.
	 * 
	 * @param outputImage output image stack
	 */
	public int computeThinImage(ImageStack outputImage) 
	{
		IJ.showStatus("Computing thin image ...");
						
		// Prepare Euler LUT [Lee94]
		int eulerLUT[] = new int[256]; 
		fillEulerLUT( eulerLUT );
		
		// Prepare number of points LUT
		int pointsLUT[] = new int[ 256 ];
		fillnumOfPointsLUT(pointsLUT);
		
		// Following Lee[94], save versions (Q) of input image S, while 
		// deleting each type of border points (R)
		ArrayList<ArrayList<int[]>> simpleBorderPoints = new ArrayList<ArrayList<int[]>>();
		for(int i=0; i<8; i++) {
			simpleBorderPoints.add(new ArrayList<int[]>());
		}		
		iterations = 0;
		// Loop through the image several times until there is no change.
		int unchangedBorders = 0;
		while( unchangedBorders < 6 )  // loop until no change for all the six border types
		{						
			unchangedBorders = 0;
			iterations++;
			System.out.println(iterations);
			for( int currentBorder = 1; currentBorder <= 6; currentBorder++)
			{
				IJ.showStatus("Thinning iteration " + iterations + " (" + currentBorder +"/6 borders) ...");

				boolean noChange = true;				
				
				// Loop through the image.				 
				for (int z = 0; z < depth; z++)
				{
					for (int y = 0; y < height; y++)
					{
						for (int x = 0; x < width; x++)						
						{

							// check if point is foreground
							if ( getPixelNoCheck(outputImage, x, y, z) != 1 )
							{
								continue;         // current point is already background 
							}
																				
							// check 6-neighbors if point is a border point of type currentBorder
							boolean isBorderPoint = false;
							// North
							if( currentBorder == 3 && N(outputImage, x, y, z) <= 0 )
								isBorderPoint = true;
							// South
							if( currentBorder == 4 && S(outputImage, x, y, z) <= 0 )
								isBorderPoint = true;
							// East
							if( currentBorder == 6 && E(outputImage, x, y, z) <= 0 )
								isBorderPoint = true;
							// West
							if( currentBorder == 5 && W(outputImage, x, y, z) <= 0 )
								isBorderPoint = true;
							if(outputImage.getSize() > 1)
							{
								// Up							
								if( currentBorder == 1 && U(outputImage, x, y, z) <= 0 )
									isBorderPoint = true;
								// Bottom
								if( currentBorder == 2 && B(outputImage, x, y, z) <= 0 )
									isBorderPoint = true;
							}
							if( !isBorderPoint )
							{
								continue;         // current point is not deletable
							}

							if( isEndPoint( outputImage, x, y, z))
							{
								continue;
							}

							final byte[] neighborhood = getNeighborhood(outputImage, x, y, z);
							
							// Check if point is Euler invariant (condition 1 in Lee[94])
							if( !isEulerInvariant( neighborhood, eulerLUT ) )
							{
								continue;         // current point is not deletable
							}

							// Check if point is simple (deletion does not change connectivity in the 3x3x3 neighborhood)
							// (conditions 2 and 3 in Lee[94])
							if( !isSimplePoint( neighborhood ) )
							{
								continue;         // current point is not deletable
							}



							// add all simple border points to a list for sequential re-checking
							int[] index = new int[3];
							index[0] = x;
							index[1] = y;
							index[2] = z;
							simpleBorderPoints.get((x%2)+(y%2)*2+(z%2)*4).add(index);
						}
					}					
					IJ.showProgress(z, this.depth);				
				}							


				// sequential re-checking to preserve connectivity when
				// deleting in a parallel way
				for (ArrayList<int[]> subvolumeSimpleBorderPoints : simpleBorderPoints) {
					for (int[] index : subvolumeSimpleBorderPoints) {			
					final byte[] neighborhood = getNeighborhood(outputImage, index[0], index[1], index[2]);
					final byte[] neighborhoodIfRemoved = neighborhood;
					neighborhoodIfRemoved[13]=0;
					//Do actual rechecking, ie, making sure that if point is removed, everything is still satisfied
					// Check if border points is simple
					if (isSimplePoint(neighborhood) && isEulerInvariant( neighborhood, eulerLUT ) &&
							isSimplePoint(neighborhoodIfRemoved) && isEulerInvariant( neighborhoodIfRemoved, eulerLUT ))//condition 4 in paper
							{						// we can delete the current point
						setPixel(outputImage, index[0], index[1], index[2], (byte) 0);
						noChange = false;
					}
				}
			}
				
				
				if( noChange )
					unchangedBorders++;

				for(int i=0; i<8; i++) {
					simpleBorderPoints.get(i).clear();
				}					
				} // end currentBorder for loop
		}

		IJ.showStatus("Computed thin image.");
		return unchangedBorders;
	} /* end computeThinImage */	
	
	public int computeMedialSurface(ImageStack outputImage) 
	{
		
		
		IJ.showStatus("Computing thin image ...");
						
		// Prepare Euler LUT [Lee94]
		int eulerLUT[] = new int[256]; 
		fillEulerLUT( eulerLUT );
		
		// Prepare number of points LUT
		int pointsLUT[] = new int[ 256 ];
		fillnumOfPointsLUT(pointsLUT);
		
		/*byte[] testNeighborhood = {0, 0, 1, 0, 1, 0, 1, 0, 0,
				0,0,1,0,1,0,1,0,0,
				0,0,0,1,1,1,0,0,0
		};*/
		byte  [] testNeighborhood = {0, 0, 0, 0, 0, 0, 0, 0, 0,
				1,1,1,1,1,1,1,0,1,
				0,0,0,0,0,0,0,0,0
		};
		
		System.out.println(!isSurfaceEndPoint(testNeighborhood)+" "+isSimplePoint(testNeighborhood)+" "+isEulerInvariant(testNeighborhood, eulerLUT));
		
		// Following Lee[94], save versions (Q) of input image S, while 
		// deleting each type of border points (R)
		//ArrayList <int[]> simpleBorderPoints = new ArrayList<int[]>();				
		
		iterations = 0;
		// Loop through the image several times until there is no change.
		int unchangedBorders = 0;
		while( unchangedBorders < 6 )  // loop until no change for all the six border types
		{						
			unchangedBorders = 0;
			iterations++;
			for( int currentBorder = 1; currentBorder <= 6; currentBorder++)
			{ //if(i==3 && currentBorder==2) break;
				IJ.showStatus("Thinning iteration " + iterations + " (" + currentBorder +"/6 borders) ...");
				ArrayList<ArrayList<int[]>> simpleBorderPoints = new ArrayList<ArrayList<int[]>>();
				for(int i=0; i<8; i++) {
					simpleBorderPoints.add(new ArrayList<int[]>());
				}
				boolean noChange = true;				
				// Loop through the image.		
				for (int z = 0; z < depth; z++)
				{

					for (int y = 0; y < height; y++)
					{
						for (int x = 0; x < width; x++)						
						{


							// check if point is foreground
							if ( getPixelNoCheck(outputImage, x, y, z) != 1 )
							{
								continue;         // current point is already background 
							}
																				
							// check 6-neighbors if point is a border point of type currentBorder
							boolean isBorderPoint = false;
							// North
							if( currentBorder == 3 && N(outputImage, x, y, z) <= 0 )
								isBorderPoint = true;
							// South
							if( currentBorder ==4 && S(outputImage, x, y, z) <= 0 )
								isBorderPoint = true;
							// East
							if( currentBorder == 6 && E(outputImage, x, y, z) <= 0 )
								isBorderPoint = true;
							// West
							if( currentBorder == 5 && W(outputImage, x, y, z) <= 0 )
								isBorderPoint = true;
							if(outputImage.getSize() > 1)
							{
								// Up							
								if( currentBorder == 1 && U(outputImage, x, y, z) <= 0 )
									isBorderPoint = true;
								// Bottom
								if( currentBorder == 2 && B(outputImage, x, y, z) <= 0 )
									isBorderPoint = true;
							}
							if( !isBorderPoint )
							{
								continue;         // current point is not deletable
							}
							
							final byte[] neighborhood = getNeighborhood(outputImage, x, y, z);

							if( isSurfaceEndPoint(neighborhood))
							{ //check it again anyway but just saves some time
								continue;
							}

							
							// Check if point is Euler invariant (condition 1 in Lee[94])
							if( !isEulerInvariant( neighborhood, eulerLUT ) )
							{
								continue;         // current point is not deletable
							}

							// Check if point is simple (deletion does not change connectivity in the 3x3x3 neighborhood)
							// (conditions 2 and 3 in Lee[94])
							if( !isSimplePoint( neighborhood ) )
							{
								continue;         // current point is not deletable
							}

							//condition 4 for surfaces in Lee[94]
							//if( isSurfaceEndPoint(neighborhood) ) {//|| numberOfNeighbors(neighborhood)<2) {
							//	continue;
							//}

							// add all simple border points to a list for sequential re-checking
							int[] index = new int[3];
							index[0] = x;
							index[1] = y;
							index[2] = z;
							//simpleBorderPoints.add(index);
							simpleBorderPoints.get((x%2)+(y%2)*2+(z%2)*4).add(index);
							/*if(currentBorder==1 || currentBorder==2) {
								simpleBorderPoints.get((x%2) + (y%2)*2 + (z%2)*4).add(index);
							}
							else if(currentBorder==3 || currentBorder==4) {
								simpleBorderPoints.get((z%2) + (x%2)*2 + (y%2)*4).add(index);

							}
							else {
								simpleBorderPoints.get((y%2) + (z%2)*2 + (x%2)*4).add(index);
							}*/

						}
					}					
					//IJ.showProgress(y, this.depth);				
				}							


				// sequential re-checking to preserve connectivity when
				// deleting in a parallel way
				//int[] index;
				//for (int[] simpleBorderPoint : simpleBorderPoints) {
				//	index = simpleBorderPoint;
				for (ArrayList<int[]> subvolumeSimpleBorderPoints : simpleBorderPoints) {
					for (int[] index : subvolumeSimpleBorderPoints) {	
						//System.out.println(Arrays.toString(index));
					final byte[] neighborhood = getNeighborhood(outputImage, index[0], index[1], index[2]);
					final byte[] neighborhoodIfRemoved = neighborhood;
					neighborhoodIfRemoved[13]=0;
					// Check if border points is simple			        
					
					if (isSimplePoint(neighborhood) && isEulerInvariant( neighborhood, eulerLUT ) &&
							isSimplePoint(neighborhoodIfRemoved) && isEulerInvariant( neighborhoodIfRemoved, eulerLUT ))// || numberOfNeighbors(neighborhood)>=2)//condition 4 in paper
							 {						// we can delete the current point
						setPixel(outputImage, index[0], index[1], index[2], (byte) 0);
						noChange = false;
					}
				}
				}
				if( noChange )
					unchangedBorders++;

				simpleBorderPoints.clear();
			} // end currentBorder for loop
		}
/*
		ArrayList <int[]> surfacePoints = new ArrayList<int[]>();
		for (int z = 0; z < depth; z++)
		{
			for (int y = 0; y < height; y++)
			{
				for (int x = 0; x < width; x++)						
				{
					if ( getPixelNoCheck(outputImage, x, y, z) ==0 )
					{	
						continue;         // current point is already background 
					}
					final byte[] neighborhood = getNeighborhood(outputImage, x, y, z);
					if(isSheet(neighborhood)) {
						surfacePoints.add(new int[] {x,y,z});
					}
				}
			}
		}
		for(int[] index : surfacePoints) {
			setPixel(outputImage, index[0], index[1], index[2], (byte) 2);
		}*/
		IJ.showStatus("Computed thin image.");
		return unchangedBorders;
	} /* end computeThinImage */	
	
	int numberOfNeighbors(byte[] neighborhood) {
		int numberOfNeighbors = -1;
		for( int i = 0; i < 27; i++ ) // i =  0..26
	      {					        	
	        if( neighborhood[i] == 1 )
	          numberOfNeighbors++;
	      }
		return numberOfNeighbors;
	}
	
	boolean isSurfaceEndPoint(byte[] neighbors)
	{ //Definition 1 in paper
		
		//List<Character> allowedIndexValues = Arrays.asList((char)(255-240), (char)(255-165), (char)(255-170), (char)(255-204));
		
		List<Character> allowedIndexValues = Arrays.asList((char)(51), (char)(15), (char)(85), //non-diagonals
				(char)(195), (char)(165), (char)(153)); //diagonals
		// Octant SWU
		char indices [] = new char[8];
		indices[0] = indexOctantSWU(neighbors);
		indices[1] = indexOctantSEU(neighbors);
		indices[2] = indexOctantNWU(neighbors);
		indices[3] = indexOctantNEU(neighbors);
		indices[4] = indexOctantSWB(neighbors);
		indices[5] = indexOctantSEB(neighbors);
		indices[6] = indexOctantNWB(neighbors);
		indices[7] = indexOctantNEB(neighbors);
		for(int octant=0; octant<8; octant++) {
			boolean conditionA = allowedIndexValues.contains(indices[octant]);
			int numberOfPointsInOctant = Integer.bitCount((int) indices[octant])-1;//-1 to exclude v?
			boolean conditionB = numberOfPointsInOctant<3;
			if (! (conditionA || conditionB) ) {
				return false;
			}
		}
		return true;
	}
	
	boolean isSheet(byte[] neighbors)
	{ //Definition 1 in paper
		
		//List<Character> allowedIndexValues = Arrays.asList((char)(255-240), (char)(255-165), (char)(255-170), (char)(255-204));
		
		List<Character> allowedIndexValues = Arrays.asList((char)(51), (char)(15), (char)(85), //non-diagonals
				(char)(195), (char)(165), (char)(153)); //diagonals
		// Octant SWU
		char indices [] = new char[8];
		indices[0] = indexOctantSWU(neighbors);
		indices[1] = indexOctantSEU(neighbors);
		indices[2] = indexOctantNWU(neighbors);
		indices[3] = indexOctantNEU(neighbors);
		indices[4] = indexOctantSWB(neighbors);
		indices[5] = indexOctantSEB(neighbors);
		indices[6] = indexOctantNWB(neighbors);
		indices[7] = indexOctantNEB(neighbors);
		for(int octant=0; octant<8; octant++) {
			if(allowedIndexValues.contains(indices[octant])) //if any of the octants has this pattern, then we consider it a sheet
				return true;
		}
		return false;
	}
	
	/**
	 * Check if a point in the given stack is at the end of an arc
	 * 
	 * @param image	The stack of a 3D binary image
	 * @param x		The x-coordinate of the point
	 * @param y		The y-coordinate of the point
	 * @param z		The z-coordinate of the point (>= 1)
	 * @return		true if the point has exactly one neighbor
	 */
	boolean isEndPoint(ImageStack image, int x, int y, int z)
	{
		int numberOfNeighbors = -1;   // -1 and not 0 because the center pixel will be counted as well
        byte[] neighbor = getNeighborhood(image, x, y, z);
        for( int i = 0; i < 27; i++ ) // i =  0..26
        {					        	
          if( neighbor[i] == 1 )
            numberOfNeighbors++;
        }

        return  numberOfNeighbors == 1;        
	}
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Get neighborhood of a pixel in a 3D image (0 border conditions) 
	 * 
	 * @param image 3D image (ImageStack)
	 * @param x x- coordinate
	 * @param y y- coordinate
	 * @param z z- coordinate (in image stacks the indexes start at 1)
	 * @return corresponding 27-pixels neighborhood (0 if out of image)
	 */
	public byte[] getNeighborhood(ImageStack image, int x, int y, int z)
	{
		byte[] neighborhood = new byte[27];
		
 		neighborhood[ 0] = getPixel(image, x-1, y-1, z-1);
		neighborhood[ 1] = getPixel(image, x  , y-1, z-1);
		neighborhood[ 2] = getPixel(image, x+1, y-1, z-1);
		
		neighborhood[ 3] = getPixel(image, x-1, y,   z-1);
		neighborhood[ 4] = getPixel(image, x,   y,   z-1);
		neighborhood[ 5] = getPixel(image, x+1, y,   z-1);
		
		neighborhood[ 6] = getPixel(image, x-1, y+1, z-1);
		neighborhood[ 7] = getPixel(image, x,   y+1, z-1);
		neighborhood[ 8] = getPixel(image, x+1, y+1, z-1);
		
		neighborhood[ 9] = getPixel(image, x-1, y-1, z  );
		neighborhood[10] = getPixel(image, x,   y-1, z  );
		neighborhood[11] = getPixel(image, x+1, y-1, z  );
		
		neighborhood[12] = getPixel(image, x-1, y,   z  );
		neighborhood[13] = getPixel(image, x,   y,   z  );
		neighborhood[14] = getPixel(image, x+1, y,   z  );
		
		neighborhood[15] = getPixel(image, x-1, y+1, z  );
		neighborhood[16] = getPixel(image, x,   y+1, z  );
		neighborhood[17] = getPixel(image, x+1, y+1, z  );
		
		neighborhood[18] = getPixel(image, x-1, y-1, z+1);
		neighborhood[19] = getPixel(image, x,   y-1, z+1);
		neighborhood[20] = getPixel(image, x+1, y-1, z+1);
		
		neighborhood[21] = getPixel(image, x-1, y,   z+1);
		neighborhood[22] = getPixel(image, x,   y,   z+1);
		neighborhood[23] = getPixel(image, x+1, y,   z+1);
		
		neighborhood[24] = getPixel(image, x-1, y+1, z+1);
		neighborhood[25] = getPixel(image, x,   y+1, z+1);
		neighborhood[26] = getPixel(image, x+1, y+1, z+1);
		
		return neighborhood;
	} /* end getNeighborhood */
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Get pixel in 3D image (0 border conditions) 
	 * 
	 * @param image 3D image
	 * @param x x- coordinate
	 * @param y y- coordinate
	 * @param z z- coordinate (in image stacks the indexes start at 1)
	 * @return corresponding pixel (0 if out of image)
	 */
	private byte getPixel(ImageStack image, int x, int y, int z)
	{
		if(x >= 0 && x < this.width && y >= 0 && y < this.height && z >= 0 && z < this.depth)
			return ((byte[]) image.getPixels(z + 1))[x + y * this.width];
		else return 0;
	} /* end getPixel */
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Get pixel in 3D image (no border checking) 
	 * 
	 * @param image 3D image
	 * @param x x- coordinate
	 * @param y y- coordinate
	 * @param z z- coordinate (in image stacks the indexes start at 1)
	 * @return corresponding pixel
	 */
	private byte getPixelNoCheck(ImageStack image, int x, int y, int z)
	{		
		return ((byte[]) image.getPixels(z + 1))[x + y * this.width];		
	} /* end getPixelNocheck */
	
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Set pixel in 3D image 
	 * 
	 * @param image 3D image
	 * @param x x- coordinate
	 * @param y y- coordinate
	 * @param z z- coordinate (in image stacks the indexes start at 1)
	 * @param value pixel value
	 */
	private void setPixel(ImageStack image, int x, int y, int z, byte value)
	{
		if(x >= 0 && x < this.width && y >= 0 && y < this.height && z >= 0 && z < this.depth)
			((byte[]) image.getPixels(z + 1))[x + y * this.width] = value;
	} /* end setPixel */

	/* -----------------------------------------------------------------------*/
	/**
	 * North neighborhood (0 border conditions) 
	 * 
	 * @param image 3D image
	 * @param x x- coordinate
	 * @param y y- coordinate
	 * @param z z- coordinate (in image stacks the indexes start at 1)
	 * @return corresponding north pixel
	 */
	private byte N(ImageStack image, int x, int y, int z)
	{
		return getPixel(image, x, y-1, z);
	} /* end N */
	
	/* -----------------------------------------------------------------------*/
	/**
	 * South neighborhood (0 border conditions) 
	 * 
	 * @param image 3D image
	 * @param x x- coordinate
	 * @param y y- coordinate
	 * @param z z- coordinate (in image stacks the indexes start at 1)
	 * @return corresponding south pixel
	 */
	private byte S(ImageStack image, int x, int y, int z)
	{
		return getPixel(image, x, y+1, z);
	} /* end S */
	
	/* -----------------------------------------------------------------------*/
	/**
	 * East neighborhood (0 border conditions) 
	 * 
	 * @param image 3D image
	 * @param x x- coordinate
	 * @param y y- coordinate
	 * @param z z- coordinate (in image stacks the indexes start at 1)
	 * @return corresponding east pixel
	 */
	private byte E(ImageStack image, int x, int y, int z)
	{
		return getPixel(image, x+1, y, z);
	} /* end E */
	
	/* -----------------------------------------------------------------------*/
	/**
	 * West neighborhood (0 border conditions) 
	 * 
	 * @param image 3D image
	 * @param x x- coordinate
	 * @param y y- coordinate
	 * @param z z- coordinate (in image stacks the indexes start at 1)
	 * @return corresponding west pixel
	 */
	private byte W(ImageStack image, int x, int y, int z)
	{
		return getPixel(image, x-1, y, z);
	} /* end W */
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Up neighborhood (0 border conditions) 
	 * 
	 * @param image 3D image
	 * @param x x- coordinate
	 * @param y y- coordinate
	 * @param z z- coordinate (in image stacks the indexes start at 1)
	 * @return corresponding up pixel
	 */
	private byte U(ImageStack image, int x, int y, int z)
	{
		return getPixel(image, x, y, z+1);
	} /* end U */
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Bottom neighborhood (0 border conditions) 
	 * 
	 * @param image 3D image
	 * @param x x- coordinate
	 * @param y y- coordinate
	 * @param z z- coordinate (in image stacks the indexes start at 1)
	 * @return corresponding bottom pixel
	 */
	private byte B(ImageStack image, int x, int y, int z)
	{
		return getPixel(image, x, y, z-1);
	} /* end B */
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Fill Euler LUT
	 * 
	 * @param LUT Euler LUT
	 */
	private void fillEulerLUT(int[] LUT) 
	{
		LUT[1]  =  1;
		LUT[3]  = -1;
		LUT[5]  = -1;
		LUT[7]  =  1;
		LUT[9]  = -3;
		LUT[11] = -1;
		LUT[13] = -1;
		LUT[15] =  1;
		LUT[17] = -1;
		LUT[19] =  1;
		LUT[21] =  1;
		LUT[23] = -1;
		LUT[25] =  3;
		LUT[27] =  1;
		LUT[29] =  1;
		LUT[31] = -1;
		LUT[33] = -3;
		LUT[35] = -1;
		LUT[37] =  3;
		LUT[39] =  1;
		LUT[41] =  1;
		LUT[43] = -1;
		LUT[45] =  3;
		LUT[47] =  1;
		LUT[49] = -1;
		LUT[51] =  1;

		LUT[53] =  1;
		LUT[55] = -1;
		LUT[57] =  3;
		LUT[59] =  1;
		LUT[61] =  1;
		LUT[63] = -1;
		LUT[65] = -3;
		LUT[67] =  3;
		LUT[69] = -1;
		LUT[71] =  1;
		LUT[73] =  1;
		LUT[75] =  3;
		LUT[77] = -1;
		LUT[79] =  1;
		LUT[81] = -1;
		LUT[83] =  1;
		LUT[85] =  1;
		LUT[87] = -1;
		LUT[89] =  3;
		LUT[91] =  1;
		LUT[93] =  1;
		LUT[95] = -1;
		LUT[97] =  1;
		LUT[99] =  3;
		LUT[101] =  3;
		LUT[103] =  1;

		LUT[105] =  5;
		LUT[107] =  3;
		LUT[109] =  3;
		LUT[111] =  1;
		LUT[113] = -1;
		LUT[115] =  1;
		LUT[117] =  1;
		LUT[119] = -1;
		LUT[121] =  3;
		LUT[123] =  1;
		LUT[125] =  1;
		LUT[127] = -1;
		LUT[129] = -7;
		LUT[131] = -1;
		LUT[133] = -1;
		LUT[135] =  1;
		LUT[137] = -3;
		LUT[139] = -1;
		LUT[141] = -1;
		LUT[143] =  1;
		LUT[145] = -1;
		LUT[147] =  1;
		LUT[149] =  1;
		LUT[151] = -1;
		LUT[153] =  3;
		LUT[155] =  1;

		LUT[157] =  1;
		LUT[159] = -1;
		LUT[161] = -3;
		LUT[163] = -1;
		LUT[165] =  3;
		LUT[167] =  1;
		LUT[169] =  1;
		LUT[171] = -1;
		LUT[173] =  3;
		LUT[175] =  1;
		LUT[177] = -1;
		LUT[179] =  1;
		LUT[181] =  1;
		LUT[183] = -1;
		LUT[185] =  3;
		LUT[187] =  1;
		LUT[189] =  1;
		LUT[191] = -1;
		LUT[193] = -3;
		LUT[195] =  3;
		LUT[197] = -1;
		LUT[199] =  1;
		LUT[201] =  1;
		LUT[203] =  3;
		LUT[205] = -1;
		LUT[207] =  1;

		LUT[209] = -1;
		LUT[211] =  1;
		LUT[213] =  1;
		LUT[215] = -1;
		LUT[217] =  3;
		LUT[219] =  1;
		LUT[221] =  1;
		LUT[223] = -1;
		LUT[225] =  1;
		LUT[227] =  3;
		LUT[229] =  3;
		LUT[231] =  1;
		LUT[233] =  5;
		LUT[235] =  3;
		LUT[237] =  3;
		LUT[239] =  1;
		LUT[241] = -1;
		LUT[243] =  1;
		LUT[245] =  1;
		LUT[247] = -1;
		LUT[249] =  3;
		LUT[251] =  1;
		LUT[253] =  1;
		LUT[255] = -1;
	}
	
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Fill number of points in octant LUT
	 * 
	 * @param LUT number of points in octant LUT
	 */
	public void fillnumOfPointsLUT(int[] LUT) 
	{
		for(int i=0; i<256; i++)
			LUT[ i ] = Integer.bitCount( i );			
	}

	/**
	 * Check if a point is Euler invariant
	 * 
	 * @param neighbors neighbor pixels of the point
	 * @param LUT Euler LUT
	 * @return true or false if the point is Euler invariant or not
	 */
	boolean isEulerInvariant(byte[] neighbors, int [] LUT)
	{
		// Calculate Euler characteristic for each octant and sum up
		int eulerChar = 0;
		char n;
		// Octant SWU
		n = indexOctantSWU(neighbors);
		eulerChar += LUT[n];
		
		// Octant SEU
		n = indexOctantSEU(neighbors);
		eulerChar += LUT[n];
		
		// Octant NWU
		n = indexOctantNWU(neighbors);
		eulerChar += LUT[n];
		
		// Octant NEU
		n = indexOctantNEU(neighbors);
		eulerChar += LUT[n];
		
		// Octant SWB
		n = indexOctantSWB(neighbors);
		eulerChar += LUT[n];
		
		// Octant SEB
		n = indexOctantSEB(neighbors);
		eulerChar += LUT[n];
		
		// Octant NWB
		n = indexOctantNWB(neighbors);
		eulerChar += LUT[n];
		
		// Octant NEB
		n = indexOctantNEB(neighbors);
		eulerChar += LUT[n];

		return eulerChar == 0;
		}

	public char indexOctantNEB(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[2]==1 )
			n |= 128;
		if( neighbors[1]==1 )
			n |=  64;
		if( neighbors[11]==1 )
			n |=  32;
		if( neighbors[10]==1 )
			n |=  16;
		if( neighbors[5]==1 )
			n |=   8;
		if( neighbors[4]==1 )
			n |=   4;
		if( neighbors[14]==1 )
			n |=   2;
		return n;
	}

	public char indexOctantNWB(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[0]==1 )
			n |= 128;
		if( neighbors[9]==1 )
			n |=  64;
		if( neighbors[3]==1 )
			n |=  32;
		if( neighbors[12]==1 )
			n |=  16;
		if( neighbors[1]==1 )
			n |=   8;
		if( neighbors[10]==1 )
			n |=   4;
		if( neighbors[4]==1 )
			n |=   2;
		return n;
	}

	public char indexOctantSEB(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[8]==1 )
			n |= 128;
		if( neighbors[7]==1 )
			n |=  64;
		if( neighbors[17]==1 )
			n |=  32;
		if( neighbors[16]==1 )
			n |=  16;
		if( neighbors[5]==1 )
			n |=   8;
		if( neighbors[4]==1 )
			n |=   4;
		if( neighbors[14]==1 )
			n |=   2;
		return n;
	}

	public char indexOctantSWB(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[6]==1 )
			n |= 128;
		if( neighbors[15]==1 )
			n |=  64;
		if( neighbors[7]==1 )
			n |=  32;
		if( neighbors[16]==1 )
			n |=  16;
		if( neighbors[3]==1 )
			n |=   8;
		if( neighbors[12]==1 )
			n |=   4;
		if( neighbors[4]==1 )
			n |=   2;
		return n;
	}

	public char indexOctantNEU(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[20]==1 )
			n |= 128;
		if( neighbors[23]==1 )
			n |=  64;
		if( neighbors[19]==1 )
			n |=  32;
		if( neighbors[22]==1 )
			n |=  16;
		if( neighbors[11]==1 )
			n |=   8;
		if( neighbors[14]==1 )
			n |=   4;
		if( neighbors[10]==1 )
			n |=   2;
		return n;
	}

	public char indexOctantNWU(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[18]==1 )
			n |= 128;
		if( neighbors[21]==1 )
			n |=  64;
		if( neighbors[9]==1 )
			n |=  32;
		if( neighbors[12]==1 )
			n |=  16;
		if( neighbors[19]==1 )
			n |=   8;
		if( neighbors[22]==1 )
			n |=   4;
		if( neighbors[10]==1 )
			n |=   2;
		return n;
	}

	public char indexOctantSEU(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[26]==1 )
			n |= 128;
		if( neighbors[23]==1 )
			n |=  64;
		if( neighbors[17]==1 )
			n |=  32;
		if( neighbors[14]==1 )
			n |=  16;
		if( neighbors[25]==1 )
			n |=   8;
		if( neighbors[22]==1 )
			n |=   4;
		if( neighbors[16]==1 )
			n |=   2;
		return n;
	}

	public char indexOctantSWU(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[24]==1 )
			n |= 128;
		if( neighbors[25]==1 )
			n |=  64;
		if( neighbors[15]==1 )
			n |=  32;
		if( neighbors[16]==1 )
			n |=  16;
		if( neighbors[21]==1 )
			n |=   8;
		if( neighbors[22]==1 )
			n |=   4;
		if( neighbors[12]==1 )
			n |=   2;
		return n;
	}
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Check if current point is a Simple Point.
	 * This method is named 'N(v)_labeling' in [Lee94].
	 * Outputs the number of connected objects in a neighborhood of a point
	 * after this point would have been removed.
	 * 
	 * @param neighbors neighbor pixels of the point
	 * @return true or false if the point is simple or not
	 */
	private boolean isSimplePoint(byte[] neighbors) 
	{
		// copy neighbors for labeling
		int cube[] = new int[26];
		int i;
		for( i = 0; i < 13; i++ )  // i =  0..12 -> cube[0..12]
			cube[i] = neighbors[i];
		// i != 13 : ignore center pixel when counting (see [Lee94])
		for( i = 14; i < 27; i++ ) // i = 14..26 -> cube[13..25]
			cube[i-1] = neighbors[i];
		// set initial label
		int label = 2;
		// for all points in the neighborhood
		for( i = 0; i < 26; i++ )
		{
			if( cube[i]==1 )     // voxel has not been labeled yet
			{
				// start recursion with any octant that contains the point i
				switch( i )
				{
				case 0:
				case 1:
				case 3:
				case 4:
				case 9:
				case 10:
				case 12:
					octreeLabeling(1, label, cube );
					break;
				case 2:
				case 5:
				case 11:
				case 13:
					octreeLabeling(2, label, cube );
					break;
				case 6:
				case 7:
				case 14:
				case 15:
					octreeLabeling(3, label, cube );
					break;
				case 8:
				case 16:
					octreeLabeling(4, label, cube );
					break;
				case 17:
				case 18:
				case 20:
				case 21:
					octreeLabeling(5, label, cube );
					break;
				case 19:
				case 22:
					octreeLabeling(6, label, cube );
					break;
				case 23:
				case 24:
					octreeLabeling(7, label, cube );
					break;
				case 25:
					octreeLabeling(8, label, cube );
					break;
				}
				label++;
				if( label-2 >= 2 )
				{
					return false;
				}
			}
		}
		//return label-2; in [Lee94] if the number of connected components would be needed
		return true;
	}
	/* -----------------------------------------------------------------------*/
	/**
	 * This is a recursive method that calculates the number of connected
	 * components in the 3D neighborhood after the center pixel would
	 * have been removed.
	 * 
	 * @param octant
	 * @param label
	 * @param cube
	 */
	private void octreeLabeling(int octant, int label, int[] cube) 
	{
		// check if there are points in the octant with value 1
		  if( octant==1 )
		  {
		  	// set points in this octant to current label
		  	// and recursive labeling of adjacent octants
		    if( cube[0] == 1 )
		      cube[0] = label;
		    if( cube[1] == 1 )
		    {
		      cube[1] = label;        
		      octreeLabeling( 2, label, cube);
		    }
		    if( cube[3] == 1 )
		    {
		      cube[3] = label;        
		      octreeLabeling( 3, label, cube);
		    }
		    if( cube[4] == 1 )
		    {
		      cube[4] = label;        
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 4, label, cube);
		    }
		    if( cube[9] == 1 )
		    {
		      cube[9] = label;        
		      octreeLabeling( 5, label, cube);
		    }
		    if( cube[10] == 1 )
		    {
		      cube[10] = label;        
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 5, label, cube);
		      octreeLabeling( 6, label, cube);
		    }
		    if( cube[12] == 1 )
		    {
		      cube[12] = label;        
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 5, label, cube);
		      octreeLabeling( 7, label, cube);
		    }
		  }
		  if( octant==2 )
		  {
		    if( cube[1] == 1 )
		    {
		      cube[1] = label;
		      octreeLabeling( 1, label, cube);
		    }
		    if( cube[4] == 1 )
		    {
		      cube[4] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 4, label, cube);
		    }
		    if( cube[10] == 1 )
		    {
		      cube[10] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 5, label, cube);
		      octreeLabeling( 6, label, cube);
		    }
		    if( cube[2] == 1 )
		      cube[2] = label;        
		    if( cube[5] == 1 )
		    {
		      cube[5] = label;        
		      octreeLabeling( 4, label, cube);
		    }
		    if( cube[11] == 1 )
		    {
		      cube[11] = label;        
		      octreeLabeling( 6, label, cube);
		    }
		    if( cube[13] == 1 )
		    {
		      cube[13] = label;        
		      octreeLabeling( 4, label, cube);
		      octreeLabeling( 6, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		  }
		  if( octant==3 )
		  {
		    if( cube[3] == 1 )
		    {
		      cube[3] = label;        
		      octreeLabeling( 1, label, cube);
		    }
		    if( cube[4] == 1 )
		    {
		      cube[4] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 4, label, cube);
		    }
		    if( cube[12] == 1 )
		    {
		      cube[12] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 5, label, cube);
		      octreeLabeling( 7, label, cube);
		    }
		    if( cube[6] == 1 )
		      cube[6] = label;        
		    if( cube[7] == 1 )
		    {
		      cube[7] = label;        
		      octreeLabeling( 4, label, cube);
		    }
		    if( cube[14] == 1 )
		    {
		      cube[14] = label;        
		      octreeLabeling( 7, label, cube);
		    }
		    if( cube[15] == 1 )
		    {
		      cube[15] = label;        
		      octreeLabeling( 4, label, cube);
		      octreeLabeling( 7, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		  }
		  if( octant==4 )
		  {
		  	if( cube[4] == 1 )
		    {
		      cube[4] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 3, label, cube);
		    }
		  	if( cube[5] == 1 )
		    {
		      cube[5] = label;        
		      octreeLabeling( 2, label, cube);
		    }
		    if( cube[13] == 1 )
		    {
		      cube[13] = label;        
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 6, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		    if( cube[7] == 1 )
		    {
		      cube[7] = label;        
		      octreeLabeling( 3, label, cube);
		    }
		    if( cube[15] == 1 )
		    {
		      cube[15] = label;        
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 7, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		    if( cube[8] == 1 )
		      cube[8] = label;        
		    if( cube[16] == 1 )
		    {
		      cube[16] = label;        
		      octreeLabeling( 8, label, cube);
		    }
		  }
		  if( octant==5 )
		  {
		  	if( cube[9] == 1 )
		    {
		      cube[9] = label;        
		      octreeLabeling( 1, label, cube);
		    }
		    if( cube[10] == 1 )
		    {
		      cube[10] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 6, label, cube);
		    }
		    if( cube[12] == 1 )
		    {
		      cube[12] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 7, label, cube);
		    }
		    if( cube[17] == 1 )
		      cube[17] = label;        
		    if( cube[18] == 1 )
		    {
		      cube[18] = label;        
		      octreeLabeling( 6, label, cube);
		    }
		    if( cube[20] == 1 )
		    {
		      cube[20] = label;        
		      octreeLabeling( 7, label, cube);
		    }
		    if( cube[21] == 1 )
		    {
		      cube[21] = label;        
		      octreeLabeling( 6, label, cube);
		      octreeLabeling( 7, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		  }
		  if( octant==6 )
		  {
		  	if( cube[10] == 1 )
		    {
		      cube[10] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 5, label, cube);
		    }
		    if( cube[11] == 1 )
		    {
		      cube[11] = label;        
		      octreeLabeling( 2, label, cube);
		    }
		    if( cube[13] == 1 )
		    {
		      cube[13] = label;        
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 4, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		    if( cube[18] == 1 )
		    {
		      cube[18] = label;        
		      octreeLabeling( 5, label, cube);
		    }
		    if( cube[21] == 1 )
		    {
		      cube[21] = label;        
		      octreeLabeling( 5, label, cube);
		      octreeLabeling( 7, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		    if( cube[19] == 1 )
		      cube[19] = label;        
		    if( cube[22] == 1 )
		    {
		      cube[22] = label;        
		      octreeLabeling( 8, label, cube);
		    }
		  }
		  if( octant==7 )
		  {
		  	if( cube[12] == 1 )
		    {
		      cube[12] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 5, label, cube);
		    }
		  	if( cube[14] == 1 )
		    {
		      cube[14] = label;        
		      octreeLabeling( 3, label, cube);
		    }
		    if( cube[15] == 1 )
		    {
		      cube[15] = label;        
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 4, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		    if( cube[20] == 1 )
		    {
		      cube[20] = label;        
		      octreeLabeling( 5, label, cube);
		    }
		    if( cube[21] == 1 )
		    {
		      cube[21] = label;        
		      octreeLabeling( 5, label, cube);
		      octreeLabeling( 6, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		    if( cube[23] == 1 )
		      cube[23] = label;        
		    if( cube[24] == 1 )
		    {
		      cube[24] = label;        
		      octreeLabeling( 8, label, cube);
		    }
		  }
		  if( octant==8 )
		  {
		  	if( cube[13] == 1 )
		    {
		      cube[13] = label;        
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 4, label, cube);
		      octreeLabeling( 6, label, cube);
		    }
		  	if( cube[15] == 1 )
		    {
		      cube[15] = label;        
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 4, label, cube);
		      octreeLabeling( 7, label, cube);
		    }
		  	if( cube[16] == 1 )
		    {
		      cube[16] = label;        
		      octreeLabeling( 4, label, cube);
		    }
		  	if( cube[21] == 1 )
		    {
		      cube[21] = label;        
		      octreeLabeling( 5, label, cube);
		      octreeLabeling( 6, label, cube);
		      octreeLabeling( 7, label, cube);
		    }
		  	if( cube[22] == 1 )
		    {
		      cube[22] = label;        
		      octreeLabeling( 6, label, cube);
		    }
		  	if( cube[24] == 1 )
		    {
		      cube[24] = label;        
		      octreeLabeling( 7, label, cube);
		    }
		  	if( cube[25] == 1 )
		      cube[25] = label;        
		  }
		
	}

	/* -----------------------------------------------------------------------*/
	/**
	 * Show plug-in information.
	 * 
	 */
	void showAbout() 
	{
		IJ.showMessage(
						"About Skeletonize3D...",
						"This plug-in filter produces 3D thinning (skeletonization) of binary 3D images.\n");
	} /* end showAbout */
	/* -----------------------------------------------------------------------*/

	/**
	 * @return 	Number of iterations the thinning algorithm completed on the last run
	 *       	If 0 is returned, then the plugin hasn't been run yet
	 */
	public int getThinningIterations() {
		return iterations;
	}

} /* end skeletonize3D */