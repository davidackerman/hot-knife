package org.janelia.saalfeldlab.hotknife;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlockInformation implements Serializable{
	Map<Long,Long> map;
    long[][] gridBlock;
    public BlockInformation(Map<Long,Long> map, long[][]  gridBlock){
        this.map = map;
        this.gridBlock = gridBlock;
    }
    public static void main(final String[] args) {
    	
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
}
}