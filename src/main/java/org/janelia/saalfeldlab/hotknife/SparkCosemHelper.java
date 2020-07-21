package org.janelia.saalfeldlab.hotknife;

import java.util.ArrayList;
import java.util.List;

import net.imglib2.RandomAccess;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.integer.UnsignedIntType;

public class SparkCosemHelper {
	public static List<long[]> voxelsToCheckForSurface = new ArrayList<long[]>(); 
	static{
		voxelsToCheckForSurface.add(new long[] {-1, 0, 0});
		voxelsToCheckForSurface.add(new long[] {1, 0, 0});
		voxelsToCheckForSurface.add(new long[] {0, -1, 0});
		voxelsToCheckForSurface.add(new long[] {0, 1, 0});
		voxelsToCheckForSurface.add(new long[] {0, 0, -1});
		voxelsToCheckForSurface.add(new long[] {0, 0, 1});
	}
	
	public static <T extends NumericType<T> & NativeType<T>> int getSurfaceAreaContributionOfVoxelInFaces(final RandomAccess<T> countsRandomAccess) {
		
		final long pos[] = {countsRandomAccess.getLongPosition(0), countsRandomAccess.getLongPosition(1), countsRandomAccess.getLongPosition(2)};
		int surfaceAreaContributionOfVoxelInFaces = 0;

		for(long[] currentVoxel : SparkCosemHelper.voxelsToCheckForSurface) {
			final long currentPosition[] = {pos[0]+currentVoxel[0], pos[1]+currentVoxel[1], pos[2]+currentVoxel[2]};
			countsRandomAccess.setPosition(currentPosition);
			if(countsRandomAccess.get() == countsRandomAccess.get().getClass().cast(0) ) {
				surfaceAreaContributionOfVoxelInFaces ++;
			}
		}

		return surfaceAreaContributionOfVoxelInFaces;	
	
	}
}
