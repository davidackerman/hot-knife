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
import java.util.function.Consumer;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

/**
 * ContactSites
 *
 * class to calculate contact sites between two predicted organelles from COSEM data, using a distance threshold:
 *      if a voxel is within the distance threshold of both organelles, it is considered a contact site
 * @author David Ackerman
 */
public class ConnectedComponentsOp<T extends RealType<T> & NumericType<T> & NativeType<T>> implements Consumer<RandomAccessibleInterval<T>> {

	// class attributes: the two source organelle images and a distance cutoff, provided as input 
	final private RandomAccessible<? extends T> source;
	
	public ConnectedComponentsOp(final RandomAccessible<T> source ) {
		// constructor that takes in distance cutoff
		this.source = source;
	}

	@Override
	public void accept(final RandomAccessibleInterval<T> output) {
		// performs the actual calculation of whether a voxel is a contact site, with the result being stored in output
		long [] min_pos = {0,0,0};
		long [] dimensions = {0,0,0};
		output.min(min_pos);
		output.dimensions(dimensions);
		final RandomAccessibleInterval<? extends T>  sourceInterval = Views.offsetInterval(source,  min_pos, dimensions);
		final RandomAccessibleInterval<BoolType> thresholded = Converters.convert(sourceInterval, (a, b) -> b.set(a.getRealDouble() >127), new BoolType());
		final ArrayImg<UnsignedLongType, LongArray> components = ArrayImgs.unsignedLongs(Intervals.dimensionsAsLongArray(thresholded));
		
		ConnectedComponentAnalysis.connectedComponents(thresholded, components);
	
		final Cursor<T> o = Views.flatIterable(output).cursor();
		final Cursor<UnsignedLongType> c = Views.flatIterable(components).cursor();

		while (o.hasNext()) {
			final T tO = o.next();
			final UnsignedLongType tC = c.next();
			if(tC.getRealDouble()>0) {
				tO.setReal(tC.getRealDouble()+min_pos[0]);
			}
		}
	}
	
}
