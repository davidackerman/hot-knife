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
	 * Class to contain relevant block information for doing COSEM analysis
	 */
	private static final long serialVersionUID = 1L;
	public long[][] gridBlock;
	public long[][] paddedGridBlock;
	public long[] padding;
	public boolean needToThinAgainPrevious;
	public boolean needToThinAgainCurrent;
	public boolean isIndependent;
	public boolean areObjectsTouching;
	
	public Map<Long,Long> edgeComponentIDtoVolumeMap;
	public Map<Long, Long> edgeComponentIDtoRootIDmap;
	public Map<Long, List<Long>> edgeComponentIDtoOrganelleIDs;//for contact sites
	public Set<Long> selfContainedMaxVolumeOrganelles;
	public Long selfContainedMaxVolume;
	public Set<Long> maxVolumeObjectIDs;
	public BlockInformation() {
		
	}
	
	public BlockInformation(long[][] gridBlock, Map<Long,Long> edgeComponentIDs,
			Map<Long, Long> edgeComponentIDtoRootIDmap) {
		this.edgeComponentIDtoVolumeMap = edgeComponentIDs;
		this.edgeComponentIDtoRootIDmap = edgeComponentIDtoRootIDmap;
		this.gridBlock = gridBlock;
	}
	
	public BlockInformation(long[][] gridBlock, long[][] paddedGridBlock, long[] padding, Map<Long,Long> edgeComponentIDs,
			Map<Long, Long> edgeComponentIDtoRootIDmap) {
		this.edgeComponentIDtoVolumeMap = edgeComponentIDs;
		this.edgeComponentIDtoRootIDmap = edgeComponentIDtoRootIDmap;
		this.gridBlock = gridBlock;
		this.padding = padding;
		this.paddedGridBlock = paddedGridBlock;
		this.needToThinAgainPrevious = true;
		this.needToThinAgainCurrent = true;
		this.isIndependent = false;
		this.areObjectsTouching = true;
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