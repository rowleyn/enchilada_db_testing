package edu.carleton.clusteringbenchmark.analysis;

import java.util.Iterator;
import java.util.Map.Entry;

public abstract class Normalizable {
	public abstract float normalize(BinnedPeakList peakList, DistanceMetric dMetric);
	public abstract float roundDistance(BinnedPeakList peakList, BinnedPeakList toList, DistanceMetric dMetric, float distance);
	
	//added this method to deal with arrays when clustering
	// - benzaids
	public abstract float roundDistance(BinnedPeakList peakList, float[] toList, DistanceMetric dMetric, float distance);
	
	/**
	 * @author steinbel
	 * Raises peak areas to the power passed in for preprocessing.
	 * @param peakList	The list to process.
	 * @param powerValue The number to which the peaks' areas is raised.
	 * 						(.5 is a good value.)
	 */
	public void reducePeaks(BinnedPeakList peakList, double powerValue) {
		Entry<Integer, Float> entry;
		Iterator<Entry<Integer, Float>> iter = peakList.peaks.entrySet().iterator();
		float newVal;
		while (iter.hasNext()){
			entry = iter.next();
			newVal = (float)Math.pow((double)entry.getValue(), powerValue);
			//System.out.println("Old value " + entry.getValue() + " sqrt " + newVal); //DEBUGGING
			entry.setValue(newVal);
		}
	}
	public abstract float posNegNormalize(BinnedPeakList list, DistanceMetric metric);
}
