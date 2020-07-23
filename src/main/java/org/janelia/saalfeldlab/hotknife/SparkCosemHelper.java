package org.janelia.saalfeldlab.hotknife;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.scijava.tool.IconDrawer;

import ch.qos.logback.core.Context;
import net.imglib2.RandomAccess;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;

public class SparkCosemHelper {
	
	/**
	 * Count how many faces of a voxel are exposed to the surface (a 0 value)
	 * 
	 * @param ra 	Random access for image
	 * @return		Number of faces on surface
	 */
	public static <T extends IntegerType<T> & NativeType<T>> int getSurfaceAreaContributionOfVoxelInFaces(final RandomAccess<T> ra) {
		List<long[]> voxelsToCheckForSurface = new ArrayList<long[]>(); 
		voxelsToCheckForSurface.add(new long[] {-1, 0, 0});
		voxelsToCheckForSurface.add(new long[] {1, 0, 0});
		voxelsToCheckForSurface.add(new long[] {0, -1, 0});
		voxelsToCheckForSurface.add(new long[] {0, 1, 0});
		voxelsToCheckForSurface.add(new long[] {0, 0, -1});
		voxelsToCheckForSurface.add(new long[] {0, 0, 1});
		
		final long pos[] = {ra.getLongPosition(0), ra.getLongPosition(1), ra.getLongPosition(2)};
		int surfaceAreaContributionOfVoxelInFaces = 0;

		for(long[] currentVoxel : voxelsToCheckForSurface) {
			final long currentPosition[] = {pos[0]+currentVoxel[0], pos[1]+currentVoxel[1], pos[2]+currentVoxel[2]};
			ra.setPosition(currentPosition);
			if(ra.get().getIntegerLong() == 0 ) {
				surfaceAreaContributionOfVoxelInFaces ++;
			}
		}

		return surfaceAreaContributionOfVoxelInFaces;	
	
	}
	
	/**
	 * Log the memory usage for given {@link Context}
	 * @param context	Name to prepend to message
	 */
	public static void logMemory(final String context) {
		final long freeMem = Runtime.getRuntime().freeMemory() / 1000000L;
		final long totalMem = Runtime.getRuntime().totalMemory() / 1000000L;
		logMsg(context + ", Total: " + totalMem + " MB, Free: " + freeMem + " MB, Delta: " + (totalMem - freeMem)
				+ " MB");
	}

	/**
	 * Print out the formatted memory usage message
	 * 
	 * @param msg	Message to log
	 */
	public static void logMsg(final String msg) {
		final String ts = new SimpleDateFormat("HH:mm:ss").format(new Date()) + " ";
		System.out.println(ts + " " + msg);
	}
	
	/**
	 * Convert global position to global ID
	 * @param position		Global position
	 * @param dimensions	Dimensions of original image
	 * @return				Global ID
	 */
	public static Long convertPositionToGlobalID(long[] position, long[] dimensions) {
		Long ID = dimensions[0] * dimensions[1] * position[2]
				+ dimensions[0] * position[1] + position[0] + 1;
		return ID;
	}
}
