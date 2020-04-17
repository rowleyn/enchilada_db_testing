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
 * The Original Code is EDAM Enchilada's BinnedPeak class.
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

import java.util.Map;

/**
 * @author andersbe, jtbigwoo
 *
 * A convenience class for returning both an value and a key
 * of a given peak.
 * @version 1.1 added the map entry so that we can modify the underlying 
 * BinnedPeakList if there is one.
 */
public class BinnedPeak {
	private float value;
	private int key;
	private Map.Entry<Integer, Float> entry;

	/**
	 * Creates a new binned peak with a reference back to the entry in the 
	 * BinnedPeakList in case we change something.
	 * @param l the integer key for this peak
	 * @param a the float area for this peak
	 * @param entry the entry in the BinnedPeakList for this peak.  If you're
	 * not going to be changing the entry behind the BinnedPeak (or if there
	 * isn't a BinnedPeakList containing this one) you should use the other
	 * constructor (or you can just pass null for this parameter. 
	 * @author jtbigwoo
	 */
	public BinnedPeak(int l, float a, Map.Entry<Integer, Float> entry)
	{
		key = l;
		value = a;
		this.entry = entry;
	}
	
	public String toString() {
		return "BinnedPeak["+getKey()+","+getValue()+"]";
	}

	public void setValue(float value) {
		this.value = value;
		if (entry != null) {
			entry.setValue(value);
		}
	}

	public float getValue() {
		return value;
	}

	public int getKey() {
		return key;
	}
}
