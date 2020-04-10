/* ***** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is EDAM Enchilada's sparse BinnedPeakList.
 *
 * The Initial Developer of the Original Code is
 * The EDAM Project at Carleton College.
 * Portions created by the Initial Developer are Copyright (C) 2005
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 * Ben J Anderson andersbe@gmail.com
 * David R Musicant dmusican@carleton.edu
 * Anna Ritz ritza@carleton.edu
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the MPL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL or the LGPL.
 *
 * ***** END LICENSE BLOCK ***** */

package edu.carleton.clusteringbenchmark.analysis;

import edu.carleton.clusteringbenchmark.analysis.dataCompression.Pair;

import java.util.*;
import java.util.Map.Entry;

/**
 * @author andersbe
 * @author smitht
 * @author jtbigwoo
 *
 * An implementation of a sparse array, this class is essentially
 * a peak list where every key is an integer value (rounded 
 * appropriately from a float).  Provides methods for adding peaks
 * from a regular peaklist, as well as methods for adding values
 * with no checks.
 */
public class BinnedPeakList implements Iterable<BinnedPeak> {
	
	protected SortedMap<Integer, Float> peaks;

	private Normalizable normalizable;
	public static long distTime = 0;
	public static long dist2Time = 0;
	private boolean isTransformed;

	/**
	 * A constructor for the peaklist, initializes the underlying
	 * ArrayLists to a size of 20.
	 */
	public BinnedPeakList(Normalizable norm)
	{
		peaks = new TreeMap<Integer, Float>();
		normalizable = norm;
	}


	/**
	 * Creates a BinnedPeakList with a new Normalizer
	 */
	public BinnedPeakList() {
		peaks = new TreeMap<Integer, Float>();
		normalizable = new Normalizer();
	}
	
	public Normalizable getNormalizable(){
		return normalizable;
	}
	
	/**
	 * Deep-copies the data in this BinnedPeakList from another BinnedPeakList
	 * @param original the BinnedPeakList to copy from
	 * @author dmusican
	 */
	public void copyBinnedPeakList(BinnedPeakList original) {
		// Note that the TreeMap copy-constructor is likely a shallow copy;
		// it does not copy the keys and values. But this contains Integer and
		// Float immutable objects, so this doesn't matter.
		peaks = new TreeMap<Integer,Float>(original.peaks);
		normalizable = original.getNormalizable();
	}
	
	// return a copy of this BPL, remember if it was xformed -- MM 2014
	public BinnedPeakList copyOf() {
		BinnedPeakList copy = new BinnedPeakList();
		copy.copyBinnedPeakList(this);
		copy.isTransformed = this.isTransformed;
		return copy;
	}


	/**
	 * Create a new BinnedPeakList and copy in all non-zero valued
	 * BinnedPeaks from this BinnedPeakList
	 * @return a BinnedPeakList with no zero-valued BinnedPeaks
	 */
	public BinnedPeakList getFilteredZerosList() {
		BinnedPeakList newSums = new BinnedPeakList(new Normalizer());
		Iterator<BinnedPeak> iter = iterator();
		BinnedPeak p;
		while (iter.hasNext()) {
			p = iter.next();
			if (p.getValue()!=0) {
				newSums.add(p);
			}
		}
		return newSums;
	}


	/**
	 * Return the magnitude of this peaklist, according to the supplied
	 * distance metric (it varies according to measurement).
	 * @param dMetric the DistanceMetric to use. Definitions of magnitude are:
	 * 			DistanceMetric.CITY_BLOCK: the sum of the list's values
	 * 			DistanceMetric.EUCLIDEAN_SQUARED, DistanceMetric.DOT_PRODUCT:
	 * 				the standard deviation of the list
	 */
	public float getMagnitude(DistanceMetric dMetric)
	{
		float magnitude = 0;

		Iterator<Entry<Integer, Float>> i = peaks.entrySet().iterator();
		if (dMetric == DistanceMetric.CITY_BLOCK)
			while (i.hasNext())
			{
				magnitude += i.next().getValue();
			}
		else if (dMetric == DistanceMetric.EUCLIDEAN_SQUARED ||
		         dMetric == DistanceMetric.DOT_PRODUCT)
		{
			float currentArea;
			while (i.hasNext())
			{
				currentArea = i.next().getValue();
				magnitude += currentArea*currentArea;
			}
			magnitude = (float) Math.sqrt(magnitude);
		}
	//	if(magnitude>1.0)
	//		System.out.println("BAD MAGNITUDE " + magnitude);
		return magnitude;
	}

	/**
	 * Returns the magnitude of a peaklist represented by a one-dimensional
	 * array.  The method is static, since it does not apply directly to the
	 * BinnedPeakList data structure, and it needs to be accessed from outside.
	 *
	 * @author benzaids
	 *
	 * @param peakarray - a one-dimensional array representing a peaklist.
	 * @param dMetric - the distance metric to be used in the calculation.
	 */
	public static float getMagnitude4Array(float[] peakarray, DistanceMetric dMetric)
	{
		float magnitude = 0;

		if (dMetric == DistanceMetric.CITY_BLOCK)
			for (int i = 0; i < peakarray.length; i++)
			{
				magnitude += peakarray[i];
			}
		else if (dMetric == DistanceMetric.EUCLIDEAN_SQUARED ||
		         dMetric == DistanceMetric.DOT_PRODUCT)
		{
			float currentArea;
			for (int i = 0; i < peakarray.length; i++)
			{
				currentArea = peakarray[i];
				magnitude += currentArea*currentArea;
			}
			magnitude = (float) Math.sqrt(magnitude);
		}
		return magnitude;
	}

	public Pair<Float, Float> getNegPosMagnitude(DistanceMetric dMetric)
	{
		float negMagnitude = 0;
		float posMagnitude = 0;
		Entry<Integer, Float> entry;

		Iterator<Entry<Integer, Float>> i = peaks.entrySet().iterator();
		if (dMetric == DistanceMetric.CITY_BLOCK)
			while (i.hasNext())
			{
				entry = i.next();
				if(entry.getKey()<0)
					negMagnitude += entry.getValue();
				else
					posMagnitude += entry.getValue();
			}
		else if (dMetric == DistanceMetric.EUCLIDEAN_SQUARED ||
		         dMetric == DistanceMetric.DOT_PRODUCT)
		{
			float currentArea;
			while (i.hasNext())
			{
				entry = i.next();
				if(entry.getKey()<0)
					negMagnitude += entry.getValue()*entry.getValue();
				else
					posMagnitude += entry.getValue()*entry.getValue();
			}
			negMagnitude = (float) Math.sqrt(negMagnitude);
			posMagnitude = (float) Math.sqrt(posMagnitude);
		}
	//	if(magnitude>1.0)
	//		System.out.println("BAD MAGNITUDE " + magnitude);
		return new Pair<Float, Float>(negMagnitude, posMagnitude);
	}


	/**
	 * Find the distance between this peaklist and another one.
	 * @param other The peaklist to compare to
	 * @param metric The distance metric to use
	 * @return the distance between the lists.
	 */
	public float getDistance(BinnedPeakList other, DistanceMetric metric) {
		long beginTime = System.currentTimeMillis();
		/*
		 * The following distance calculation algorithm is very similar to the
		 * merge part of merge sort, where you riffle through both lists looking
		 * for the lowest entry, and choosing that one.  The extra condition
		 * is when we have an entry for the same dimension in each list,
		 * in which case we don't choose one but calculate the distance between
		 * them.
		 */

		Entry<Integer, Float> i = null, j = null;
		Iterator<Entry<Integer, Float>> thisIter = peaks.entrySet().iterator(),
			thatIter = other.peaks.entrySet().iterator();

		float distance = 0;

		// if one of the peak lists is empty, do something about it.
		if (thisIter.hasNext()) {
			i = thisIter.next();
		}
		if (thatIter.hasNext()) {
			j = thatIter.next();
		}
		// both lists have some particles, so
		while (i != null && j != null) {
			if (i.getKey().equals(j.getKey()))
			{
				distance += DistanceMetric.getDistance(i.getValue(),
						j.getValue(),
						metric);


				if (thisIter.hasNext())
					i = thisIter.next();
				else i = null;

				if (thatIter.hasNext())
					j = thatIter.next();
				else j = null;
			}
			else if (i.getKey() < j.getKey())
			{
				distance += DistanceMetric.getDistance(0, i.getValue(), metric);

				if (thisIter.hasNext())
					i = thisIter.next();
				else i = null;
			}
			else
			{
				distance += DistanceMetric.getDistance(0, j.getValue(), metric);

				if (thatIter.hasNext())
					j = thatIter.next();
				else j = null;
			}
		}

		if (i != null) {
			assert(j == null);
			distance += DistanceMetric.getDistance(0, i.getValue(), metric);
			while (thisIter.hasNext()) {
				distance += DistanceMetric.getDistance(0,
						thisIter.next().getValue(), metric);
			}
		} else if (j != null) {
			distance += DistanceMetric.getDistance(0, j.getValue(), metric);
			while (thatIter.hasNext()) {
				distance += DistanceMetric.getDistance(0,
						thatIter.next().getValue(), metric);
			}
		}

		if (metric == DistanceMetric.DOT_PRODUCT)
		    distance = 1-distance;
		// dot product actually comes up with similarity, rather than distance,
		// so we take 1- it to find "distance".

		float rndDist = normalizable.roundDistance(this, other, metric, distance);
		distTime += (System.currentTimeMillis()-beginTime);
		return rndDist;
	}


	/**
	 * Retrieve the value of the peaklist at a given key
	 * @param key	The key of the value you wish to
	 * 					retrieve.
	 * @return			The value at the given key.
	 */
	public float getAreaAt(int location)
	{
		Float area = peaks.get(location);
		if (area == null) {
			return 0;
		} else {
			return area;
		}
	}

	/**
	 * Find the distance between this peaklist and another one. By including
	 * the magnitude of the other peak list, the method can be run faster.
	 * @param other The peaklist (in array form) to compare to
	 * @param magnitude The magnitude of the other peak list
	 * @param metric The distance metric to use
	 * @return the distance between the lists.
	 */
	public float getDistance(float[] other, float magnitude,
							 DistanceMetric metric, int zeroOffset) {

		/*
		 * The following distance calculation algorithm is very similar to the
		 * merge part of merge sort, where you riffle through both lists looking
		 * for the lowest entry, and choosing that one.  The extra condition
		 * is when we have an entry for the same dimension in each list,
		 * in which case we don't choose one but calculate the distance between
		 * them.
		 */
		long temptime = System.currentTimeMillis();

		float distance = magnitude;

		// loop over this peak list, accumulating magnitude as you go,
		// but calculating distance if match with other (and subtracting off
		// that portion from magnitude with other)
		for (Entry<Integer,Float> i : peaks.entrySet()) {
			int iKey = i.getKey();
			if (iKey >= -zeroOffset && iKey <= zeroOffset) {
				float iValue = i.getValue();
				if (other[iKey + zeroOffset] != 0) {
					float otherValue = other[iKey + zeroOffset];
					distance = distance +
						(DistanceMetric.getDistance(iValue,otherValue,metric) -
								DistanceMetric.getDistance(0,otherValue,metric));
				}
				else {
					distance += DistanceMetric.getDistance(0,iValue,metric);
				}
			}
		}

		if (metric == DistanceMetric.DOT_PRODUCT)
		    distance = 1-distance;
		// dot product actually comes up with similarity, rather than distance,
		// so we take 1- it to find "distance".

		dist2Time += System.currentTimeMillis() - temptime;

		return normalizable.roundDistance(this, other, metric, distance);
	}

	/**
	 * This is just like add(float, float) except that it is assumed that
	 * rounding the peaks to the right location has been done already.
	 * @param location
	 * @param area
	 */
	public void add(int location, float area) {
		assert !(peaks.containsKey(location)== true && peaks.get(location)== null) : "null peak is present in list";
		Float tempArea = peaks.get(location);
		if (tempArea != null)
		{
			peaks.put(location, tempArea + area);
		} else {
			peaks.put(location, area);
		}
	}

	/**
	 * Adds a BinnedPeak, with the same checks as add(float, float).
	 * Equivalent to add(bp.key, bp.value).
	 * @param bp the BinnedPeak to add.
	 */
	public void add(BinnedPeak bp) {
		add(bp.getKey(), bp.getValue());
	}
	private void add(Entry<Integer, Float> entry) {
		add(entry.getKey(), entry.getValue());

	}

	/**
	 * Returns the number of locations represented by this
	 * Binned peaklist
	 * @return the number of locations in the list
	 */
	public int length()
	{
		return peaks.size();
	}

	/**
	 * Divide the value at each peak by this amount.
	 * @param divisor
	 */
	public void divideAreasBy(int divisor) {
		Entry<Integer,Float> e;
		Iterator<Entry<Integer,Float>> i = peaks.entrySet().iterator();

		while (i.hasNext()) {
			e = i.next();
			e.setValue(e.getValue() / divisor);
		}
	}

	public SortedMap<Integer, Float> getPeaks() {
		return peaks;
	}

	/**
	 * Find the sum of two particles.
	 * @param other the particle to add to this one.
	 */
	public void addAnotherParticle(BinnedPeakList other) {
		Iterator<Entry<Integer, Float>> i = other.peaks.entrySet().iterator();
		while (i.hasNext()) {
			add(i.next());
		}
	}

	// method to permit choice of normalization technique
	// Michael Murphy 2014
	public void normalize(DistanceMetric dMetric, boolean posNegNorm) {
		if (posNegNorm)
			posNegNormalize(dMetric);
		else
			normalize(dMetric);
	}

	/**
	 * A method to normalize this BinnedPeakList.  Depending
	 * on which distance metric is
	 * used, this method will change the peaklist so that the distance
	 * from <0,0,0,....,0> to the vector represented by the list is equal to 1.
	 * @param 	dMetric the distance metric to use to measure length
	 */
	public void normalize(DistanceMetric dMetric) {
		normalizable.normalize(this,dMetric);
	}

	/**
	 * @author steinbel
	 * A method to normalize this BinnedPeakList by normalizing the positive
	 * and negative spectra separately and then together.
	 * @param dMetric  the distance metric to use when normalizing
	 */
	public float posNegNormalize(DistanceMetric dMetric){
		return normalizable.posNegNormalize(this, dMetric);
	}

	/**
	 * @author steinbel
	 * A method to reduce the peak area by the power passed in (preprocessing
	 * to be used before clustering).
	 * @param power	The power to which to raise the area of the peaks.  (.5 is good.)
	 */
	public void preProcess(double power){
		normalizable.reducePeaks(this, power);
	}

	/**
	 * Divide each value by a scalar factor.
	 * @param factor
	 */
	public void divideAreasBy(float factor) {
		Iterator<Entry<Integer, Float>> iter = peaks.entrySet().iterator();
		Entry<Integer,Float> temp;
		while (iter.hasNext()) {
			temp = iter.next();
			temp.setValue(temp.getValue() / factor);
		}
	}

	// peak area transforms -- Michael Murphy 2014
	public BinnedPeakList transformAreas(PeakTransform transform) {
		if (transform == PeakTransform.NONE || isTransformed) // prevent compound log transforms...
			return this;

		Iterator<Entry<Integer, Float>> iter = peaks.entrySet().iterator();
		Entry<Integer,Float> temp;
		float value;
		while (iter.hasNext()) {
			temp = iter.next();
			if (transform == PeakTransform.LOG)
				temp.setValue(new Float(Math.log10(temp.getValue())));
			else if (transform == PeakTransform.SQRT)
				temp.setValue(new Float(Math.sqrt(temp.getValue())));
		}

		isTransformed = true;
		return this;
	}

	/**
	 * Return an iterator view of the binned peak list.  Note that modifying
	 * the elements accessed by the iterator will NOT modify the peaklist itself.
	 */
	public Iterator<BinnedPeak> iterator() {
		return new Iter(this);
	}

	/**
	 * Warning!  This does not actually provide you with access to the
	 * underlying map structure, so any changes made to elements accessed by
	 * this iterator will NOT BE REFLECTED in the BPL itself.
	 *
	 * @author smitht
	 *
	 */
	public class Iter implements Iterator<BinnedPeak> {
		private Iterator<Entry<Integer,Float>> entries;

		/*
		 * Copy the set of peaks into a list, and get an iterator on the list.
		 * This happens to be sorted, yay.
		 */
		public Iter(BinnedPeakList bpl) {
			this.entries = bpl.peaks.entrySet().iterator();
		}

		public boolean hasNext() {
			return entries.hasNext();
		}

		public BinnedPeak next() {
			Entry<Integer,Float> e = entries.next();
			return new BinnedPeak(e.getKey(), e.getValue(), e);
		}

		public void remove() {
			throw new Error("Not implemented!");
		}
	}
	
}
