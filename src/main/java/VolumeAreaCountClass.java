import java.io.Serializable;
import java.util.Set;

public class VolumeAreaCountClass implements Serializable {
	private static final long serialVersionUID = 1L;
	public long volumeCount, surfaceAreaCount, selfContainedObjectCounts;
	public Set<Long> edgeObjectIDs;
	
	public VolumeAreaCountClass(long volumeCount, long surfaceAreaCount, long selfContainedObjectCounts, Set<Long> edgeObjectIDs) {
		this.volumeCount = volumeCount;
		this.surfaceAreaCount = surfaceAreaCount;
		this.edgeObjectIDs = edgeObjectIDs;
		this.selfContainedObjectCounts = selfContainedObjectCounts;
	}
}