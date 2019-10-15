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
    /*public static void main(final String[] args) {
    	
    	final Map<Long,Long> m = new HashMap<Long,Long>();
    	final long[][] l = null;
    	BlockInformation bi= new BlockInformation(m, l);
    	List< BlockInformation> blockInformation = new ArrayList< BlockInformation>();
    	blockInformation.add(bi);
    	blockInformation.add(bi);

    	if(blockInformation instanceof Serializable) {
    		System.out.println("yes");
    	}
    	System.out.println("done");
}*/
}