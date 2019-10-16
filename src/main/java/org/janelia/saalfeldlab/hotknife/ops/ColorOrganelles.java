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

import java.util.Random;
import java.util.function.Consumer;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

/**
 * ContactSites
 *
 * class to calculate contact sites between two predicted organelles from COSEM data, using a distance threshold:
 *      if a voxel is within the distance threshold of both organelles, it is considered a contact site
 * @author David Ackerman
 */
public class ColorOrganelles<T extends RealType<T> & NumericType<T> & NativeType<T>> implements Consumer<RandomAccessibleInterval<T>> {

	// class attributes: the two source organelle images and a distance cutoff, provided as input 
	final private RandomAccessible<? extends T> source;
	final private int colorChannelIndex;
	
	public ColorOrganelles(final RandomAccessible<T> source, int colorChannelIndex) {
		// constructor that takes in distance cutoff
		this.source = source;
		this.colorChannelIndex = colorChannelIndex;
	}

	@Override
	public void accept(final RandomAccessibleInterval<T> output) {		
		
		// gets an interval over the source data and creates a cursor to iterate over the voxels
		final Cursor<? extends T> a = Views.flatIterable(Views.interval(source, output)).cursor();
		final Cursor<T> b = Views.flatIterable(output).cursor();
		Random generator = new Random();

		while (b.hasNext()) {
			final T tA = a.next();
			final T tB = b.next();
			if(tA.getRealDouble()>0) {
				generator.setSeed((long) tA.getRealDouble()*1000 + colorChannelIndex*100);
				// System.out.format("%d %f %d %d \n", colorChannelIndex, tA.getRealDouble(),generator.nextInt(65535), generator2.nextInt(65535));
				//System.out.println(tA.getRealDouble()+" "+generator.nextDouble()*65535);
				tB.setReal(generator.nextInt(65535));
			}
		}
	}
	
}
