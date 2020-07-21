package org.janelia.saalfeldlab.hotknife;

import java.util.Arrays;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.converter.Converters;
import net.imglib2.img.NativeImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

public class CorrectlyPaddedDistanceTransform{
	/**
	 * Class to help with getting correctly padded distance transform
	 */
	public long [] padding, paddedOffset, paddedDimension;
	public NativeImg<FloatType, ?> correctlyPaddedDistanceTransform;
	
	public CorrectlyPaddedDistanceTransform(RandomAccessibleInterval<UnsignedLongType> source, long[] offset, long[] dimension){
		
		long[] sourceDimensions = {0,0,0};
		source.dimensions(sourceDimensions);
		final RandomAccessibleInterval<NativeBoolType> sourceBinarized = Converters.convert(
				source,
				(a, b) -> {
					b.set(a.getIntegerLong()<1);
				},
				new NativeBoolType());
		
		final long[] initialPadding = {16,16,16};
		
		padding = new long [3];
		paddedOffset = new long[3];
		paddedDimension = new long[3];
		final long[] minInside = new long[3];
		final long[] dimensionsInside = new long[3];

		long [] testPadding = initialPadding.clone();
		int shellPadding = 1;
		//Distance Transform
A:			for (boolean paddingIsTooSmall = true; paddingIsTooSmall; Arrays.setAll(testPadding, i -> testPadding[i] + initialPadding[i])) {

			paddingIsTooSmall = false;
			padding = testPadding.clone();
			final long maxPadding =  Arrays.stream(padding).max().getAsLong();
			final long squareMaxPadding = maxPadding * maxPadding;

			Arrays.setAll(paddedOffset, i -> offset[i] - padding[i]);
			Arrays.setAll(paddedDimension, i -> dimension[i] + 2*padding[i]);
			
			final IntervalView<NativeBoolType> sourceBlock =
					Views.offsetInterval(
							Views.extendValue(
									sourceBinarized,
									new NativeBoolType(true)),
							paddedOffset,
							paddedDimension);
			
			/* make distance transform */				
			correctlyPaddedDistanceTransform = ArrayImgs.floats(paddedDimension);
			
			DistanceTransform.binaryTransform(sourceBlock, correctlyPaddedDistanceTransform, DISTANCE_TYPE.EUCLIDIAN);

			Arrays.setAll(minInside, i -> padding[i] );
			Arrays.setAll(dimensionsInside, i -> dimension[i] );

			final IntervalView<FloatType> insideBlock = Views.offsetInterval(Views.extendZero(correctlyPaddedDistanceTransform), minInside, dimensionsInside);

			/* test whether distances at inside boundary are smaller than padding */
			for (int d = 0; d < 3; ++d) {

				final IntervalView<FloatType> topSlice = Views.hyperSlice(insideBlock, d, 1);
				for (final FloatType t : topSlice)
					if (t.get() >= squareMaxPadding-shellPadding) { //Subtract one from squareMaxPadding because we want to ensure that if we have a shell in later calculations for finding surface points, we can access valid points
						paddingIsTooSmall = true;
						continue A;
					}

				final IntervalView<FloatType> botSlice = Views.hyperSlice(insideBlock, d, insideBlock.max(d));
				for (final FloatType t : botSlice)
					if (t.get() >= squareMaxPadding-shellPadding) {
						paddingIsTooSmall = true;
						continue A;
					}
			}
		}
	}
	
}
