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
 * The Original Code is EDAM Enchilada's Centroid class.
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


/*
 * Created on Aug 24, 2004
 *
 * By Ben Anderson
 */
package edu.carleton.clusteringbenchmark.analysis.clustering;

import edu.carleton.clusteringbenchmark.analysis.BinnedPeakList;

/**
 * @author andersbe
 * @author steinbel
 * Holds information about a centroid.  
 */
public class Centroid {
	/**
	 * The peaklist of this centroid.
	 */
	public BinnedPeakList peaks;
	/**
	 * The number of atoms that belong to this centroid.
	 */
	public int numMembers;
	
	/**
	 * The number of the cluster (within the k clusters) this centroid represents.
	 */
	public int subCollectionNum;
	
	// associate this centroid with its stats -- Michael Murphy 2014
	//public float sumDistances;
	//public float sumSqDistances;
	
	/**
	 * Constructor.  SubCollectionNum is automatically set to -1.
	 * @param peaks
	 * @param numMembers
	 */
	public Centroid(BinnedPeakList peaks, int numMembers)
	{
		this.peaks = peaks;
		this.numMembers = numMembers;
//		sumDistances = sumSqDistances = 0;
		subCollectionNum = -1;
	}
	
	/**
	 * Constructor with subCollectionNumPassed as a parameter.
	 * @param peaks
	 * @param numMembers
	 * @param subCollectionNum
	 */
	public Centroid (BinnedPeakList peaks, int numMembers,
			int subCollectionNum)
	{
		this.peaks = peaks;
		this.numMembers = numMembers;
		this.subCollectionNum = subCollectionNum;
	}
	
//	public float getMeanDistance() {
//		return Math.max(sumDistances / numMembers, 0);
//	}
//	
//	public float getStdDevDistance() {
//		return (float) Math.sqrt((sumSqDistances/numMembers)-Math.pow(sumDistances/numMembers, 2));
//	}
}
