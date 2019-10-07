/*
 * #%L
 * ImageJ software for multidimensional image processing and analysis.
 * %%
 * Copyright (C) 2014 - 2017 Board of Regents of the University of
 * Wisconsin-Madison, University of Konstanz and Brian Northan.
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */

package org.janelia.saalfeldlab.hotknife.ops;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

/**
 * Connected Components Op
 *
 * Implement connected components for lazy process operation
 * @author David Ackerman
 */
public class ConnectedComponentsOp<T extends RealType<T> & NumericType<T> & NativeType<T>> implements Consumer<RandomAccessibleInterval<T>> {

	// class attributes: source organelle information and source dimensions
	final private RandomAccessible<? extends T> source;
	final private long [] sourceDimensions;
	final boolean isDisplayed;
	public ConnectedComponentsOp(final RandomAccessible<T> source, long [] sourceDimensions, boolean isDisplayed) {
		// constructor that takes in source and source dimensions
		this.source = source;
		this.sourceDimensions = sourceDimensions;
		this.isDisplayed = isDisplayed;
	}

	@Override
	public void accept(final RandomAccessibleInterval<T> output) {
		// performs the actual connected component analysis
		
		// Get offset interval for passing to connectedComponents
		long [] minimumPosition = {0,0,0};
		long [] outputDimensions = {0,0,0};
		output.min(minimumPosition);
		output.dimensions(outputDimensions);
		final RandomAccessibleInterval<? extends T>  sourceInterval = Views.offsetInterval(source,  minimumPosition, outputDimensions);
		
		computeConnectedComponents(sourceInterval, output, outputDimensions, null);
	}
	
	public Set<List<Long>> computeConnectedComponents(RandomAccessibleInterval<? extends T>  sourceInterval, RandomAccessibleInterval<T>  output, long [] outputDimensions, long [] offset) {
		// threshold sourceInterval using cutoff of 127
		final RandomAccessibleInterval<BoolType> thresholded = Converters.convert(sourceInterval, (a, b) -> b.set(a.getRealDouble() >127), new BoolType());
		
		// run connected component analysis, storing results in components
		final ArrayImg<UnsignedLongType, LongArray> components = ArrayImgs.unsignedLongs(Intervals.dimensionsAsLongArray(thresholded));
		ConnectedComponentAnalysis.connectedComponents(thresholded, components);
	
		// cursors over output and components
		Cursor<T> o = Views.flatIterable(output).cursor();
		final Cursor<UnsignedLongType> c = Views.flatIterable(components).cursor();
		
		
		// assign values from components to output and create array for relabeling connected components based on the first voxel in the connected component
		
		long totalNumberOfVoxelsInSource = (sourceDimensions[0]*sourceDimensions[1]*sourceDimensions[2]);
		double labelBasedOnMaxVoxelIndexInComponent[] = new double[(int) (outputDimensions[0]*outputDimensions[1]*outputDimensions[2])]; 
		
		while (o.hasNext()) {
			final T tO = o.next();
			final UnsignedLongType tC = c.next();
			if(tC.getRealDouble()>0) {
				tO.setReal(tC.getRealDouble());

				// if connected component exists, assign its value to output and update its new label based on first voxel
				long [] currentVoxelPosition = {o.getIntPosition(0), o.getIntPosition(1), o.getIntPosition(2)};
				if(offset!=null) {
					currentVoxelPosition[0]+=offset[0];
					currentVoxelPosition[1]+=offset[1];
					currentVoxelPosition[2]+=offset[2];
				}
				double currentVoxelIndex = (double) (sourceDimensions[0]*sourceDimensions[1]*currentVoxelPosition[2]+//Z position
									sourceDimensions[0]*currentVoxelPosition[1]+
									currentVoxelPosition[0]);
				
				int defaultLabel = tC.getInteger();
				if(currentVoxelIndex>labelBasedOnMaxVoxelIndexInComponent[defaultLabel]) {
					labelBasedOnMaxVoxelIndexInComponent[defaultLabel] = currentVoxelIndex;
				}
				
			}
		}
		
		Set<List<Long>> uniqueIDSet = new HashSet<List<Long>>();
		// update output labels based on max voxel labels
		o = Views.flatIterable(output).cursor();
		while (o.hasNext()) {
			final T tO = o.next();
			if (tO.getRealDouble()!=0) {
				double newLabel = 0;
				if (isDisplayed) {//scale to fit in range
					newLabel *= 65535.0/totalNumberOfVoxelsInSource;
				}
				else {
					newLabel = labelBasedOnMaxVoxelIndexInComponent[(int) tO.getRealDouble()];
					uniqueIDSet.add(Arrays.asList((long) newLabel, (long) newLabel));
				}
				tO.setReal(newLabel);
			}
			
		}
		
		return uniqueIDSet;
	}
	
}
