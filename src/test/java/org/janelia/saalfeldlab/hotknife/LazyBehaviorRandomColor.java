/**
 *
 */
package org.janelia.saalfeldlab.hotknife;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.janelia.saalfeldlab.hotknife.ops.ConnectedComponentsOp;
import org.janelia.saalfeldlab.hotknife.ops.ColorOrganelles;
import org.janelia.saalfeldlab.hotknife.util.Lazy;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import bdv.util.volatiles.VolatileViews;
import ij.ImageJ;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.Volatile;
import net.imglib2.converter.Converters;
import net.imglib2.img.basictypeaccess.AccessFlags;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.Views;

/**
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 * Based on LazyBehavior.java by Stephen Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public class LazyBehaviorRandomColor {

	private static int[] blockSize = new int[] {32, 32,32};

	/**
	 * @param args
	 */
	public static void main(final String[] args) throws IOException {

		new ImageJ();

		// set up N5 readers to read in raw EM data and predictions for COSEM
		//N5FSReader n5Raw = new N5FSReader("/groups/cosem/cosem/data/HeLa_Cell3_4x4x4nm/HeLa_Cell3_4x4x4nm.n5/");
		//N5FSReader n5CC = new N5FSReader("/groups/cosem/cosem/ackermand/hela_cell3_314000_connected_components.n5");

		N5FSReader n5Raw = new N5FSReader("/groups/cosem/cosem/ackermand/hela_cell3_314000_crop.n5");
		N5FSReader n5CC = new N5FSReader("/groups/cosem/cosem/ackermand/hela_cell3_314000_crop.n5");

		
		// store raw data and predictions for two organelles in array of UnsignedByteType random accessible intervals
		RandomAccessibleInterval<UnsignedByteType> rawImg = N5Utils.openVolatile(n5Raw, "mito");
		RandomAccessibleInterval<DoubleType> organelleImg = N5Utils.openVolatile(n5CC, "mito_cc");

		
		// convert UnsignedByteType random accessible intervals to DoubleType, extend them using a mirroring strategy.
		// add them to an extended random accessible array
		
		
		// display the raw data and predicted organelles
		BdvOptions options = BdvOptions.options();
			final BdvStackSource<Volatile<UnsignedByteType>> stackSource =
					BdvFunctions.show(
							VolatileViews.wrapAsVolatile(rawImg),
							"",
							options);
				stackSource.setDisplayRange(0, 255);
			options = options.addTo(stackSource);

			BdvStackSource<Volatile<DoubleType>> rgb;
			for(int i=0; i<3; i++) {
				ColorOrganelles<DoubleType> currentColor = new ColorOrganelles<>(organelleImg, i);
				final RandomAccessibleInterval<DoubleType> currentChannelRandomColor = Lazy.process(
						organelleImg,
						blockSize,
						new DoubleType(),
						AccessFlags.setOf(AccessFlags.VOLATILE),
						currentColor);
				rgb =
						BdvFunctions.show(
								VolatileViews.wrapAsVolatile(currentChannelRandomColor),
								"",
								options
								);
				if(i==0) {
					rgb.setColor(new ARGBType( ARGBType.rgba(65535,0,0,0)));
				}
				else if(i==1) {
					rgb.setColor(new ARGBType( ARGBType.rgba(0,65535,0,0)));
				}
				else {
					rgb.setColor(new ARGBType( ARGBType.rgba(0,0,65535,0)));
				}
				options = options.addTo(rgb);

				
			}
		


	}
}
