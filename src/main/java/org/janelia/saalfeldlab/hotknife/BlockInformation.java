package org.janelia.saalfeldlab.hotknife;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BlockInformation implements Serializable{
    public long[][] gridBlock;
	public Set<Long> edgeComponentIDs;
	public Map<Long,Long> edgeComponentIDtoRootIDmap;
    public BlockInformation(long[][]  gridBlock, Set<Long> edgeComponentIDs, Map<Long,Long> edgeComponentIDtoRootIDmap){
    	this.edgeComponentIDs = edgeComponentIDs;
        this.edgeComponentIDtoRootIDmap = edgeComponentIDtoRootIDmap;
        this.gridBlock = gridBlock;
    }
}