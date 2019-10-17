package org.janelia.saalfeldlab.hotknife;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

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
}