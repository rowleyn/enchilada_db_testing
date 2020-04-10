package edu.carleton.clusteringbenchmark.analysis;

public class DummyNormalizer extends Normalizable{

	public float normalize(BinnedPeakList peakList, DistanceMetric dMetric) {
		return 0;
	}

	public float posNegNormalize(BinnedPeakList peakList, DistanceMetric dMetric) {
		return 0;
	}

	public float roundDistance(BinnedPeakList peakList, BinnedPeakList toList, DistanceMetric dMetric, float distance) {
		return distance;
	}
	
	//added this method to deal with arrays when clustering
	// - benzaids
	public float roundDistance(BinnedPeakList peakList, float[] toList, DistanceMetric dMetric, float distance) {
		return distance;
	}

}
