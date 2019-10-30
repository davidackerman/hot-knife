package org.janelia.saalfeldlab.hotknife;

import java.io.Serializable;
import java.util.Map;

public class VolumeAreaCountClass implements Serializable {
	private static final long serialVersionUID = 1L;
	public long totalObjectCounts;
	public Map<Long, Long> volumeHistogramAsMap;
	public Map<Long, Long> surfaceAreaHistogramAsMap;
	public Map<Float, Long> surfaceAreaToVolumeRatioHistogramAsMap;
	public Map<Long, Long> edgeObjectIDtoVolume;
	public Map<Long, Long> edgeObjectIDtoSurfaceArea;

	public VolumeAreaCountClass(final Map<Long, Long> volumeHistogramAsMap,
			final Map<Long, Long> surfaceAreaHistogramAsMap,
			final Map<Float, Long> surfaceAreaToVolumeRatioHistogramAsMap, final Map<Long, Long> edgeObjectIDtoVolume,
			final Map<Long, Long> edgeObjectIDtoSurfaceArea) {
		this.volumeHistogramAsMap = volumeHistogramAsMap;
		this.surfaceAreaHistogramAsMap = surfaceAreaHistogramAsMap;
		this.surfaceAreaToVolumeRatioHistogramAsMap = surfaceAreaToVolumeRatioHistogramAsMap;
		this.edgeObjectIDtoVolume = edgeObjectIDtoVolume;
		this.edgeObjectIDtoSurfaceArea = edgeObjectIDtoSurfaceArea;
	}
}