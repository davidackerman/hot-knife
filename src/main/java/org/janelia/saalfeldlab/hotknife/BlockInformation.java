package org.janelia.saalfeldlab.hotknife;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.janelia.saalfeldlab.hotknife.util.Grid;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;

public class BlockInformation implements Serializable {
	/**
	 * Class to contain relevant information for doing connected components:
	 * information about block location/size, object ids on edge of block and a map
	 * for relabeling those ids if necessary
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public long[][] gridBlock;
	public Set<Long> edgeComponentIDs;
	public Map<Long, Long> edgeComponentIDtoRootIDmap;

	public BlockInformation(long[][] gridBlock, Set<Long> edgeComponentIDs,
			Map<Long, Long> edgeComponentIDtoRootIDmap) {
		this.edgeComponentIDs = edgeComponentIDs;
		this.edgeComponentIDtoRootIDmap = edgeComponentIDtoRootIDmap;
		this.gridBlock = gridBlock;
	}
	
	public static List<BlockInformation> buildBlockInformationList(final String inputN5Path,
			final String inputN5DatasetName) throws IOException {
		//Get block attributes
		N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();
		
		//Build list
		List<long[][]> gridBlockList = Grid.create(outputDimensions, blockSize);
		List<BlockInformation> blockInformationList = new ArrayList<BlockInformation>();
		for (int i = 0; i < gridBlockList.size(); i++) {
			long[][] currentGridBlock = gridBlockList.get(i);
			blockInformationList.add(new BlockInformation(currentGridBlock, null, null));
		}
		return blockInformationList;
	}
}