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
 * The Original Code is EDAM Enchilada's DataPoint class.
 *
 * The Initial Developer of the Original Code is
 * The EDAM Project at Carleton College.
 * Portions created by the Initial Developer are Copyright (C) 2005
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 * Jonathan Sulman sulmanj@carleton.edu
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
 * Created on Feb 5, 2005
 *
 */
package edu.carleton.dataloadbenchmark;

/**
 * @author sulmanj
 *
 * A simple class for managing x and y coordinates.
 * Implements the Comparable interface by comparing x coordinates.2
 */
public class DataPoint implements Comparable {
	public double x;
	public double y;
	
	/**
	 * An empty datapoint;
	 *
	 */
	public DataPoint()
	{
		x = y = 0;
	}
	
	/**
	 * 
	 * @param x The X coordinate.
	 * @param y The Y coordinate.
	 */
	public DataPoint(double x, double y)
	{
		this.x = x;
		this.y = y;
	}
	
	/**
	 * Compares two datapoints by comparing their x coordinates.
	 * Two datapoints may be equal with regard to this relation even
	 * if their y coordinates are not equal.
	 */
	public int compareTo(Object o) throws ClassCastException
	{
		DataPoint dp = (DataPoint)o;
		if(x < dp.x) return -1;
		else if(x > dp.x) return 1;
		else return 0;
	}
	
	
	public String toString() {
		return "DataPoint[x="+x+"; y="+y+"]";
	}
}
