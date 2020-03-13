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
 * The Original Code is EDAM Enchilada's CentroidListCursor class.
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
 * Created on Oct 11, 2004
 */
package edu.carleton.clusteringbenchmark.analysis.clustering;

import java.util.ArrayList;

import edu.carleton.clusteringbenchmark.ATOFMS.ParticleInfo;
import edu.carleton.clusteringbenchmark.analysis.BinnedPeakList;
import edu.carleton.clusteringbenchmark.database.CollectionCursor;

/**
 * @author andersbe
 * 
 * Cursor that iterates through the centroids.
 */
public class CentroidListCursor implements CollectionCursor {
	int i = -1;
	private ArrayList<Centroid> centroidList;
	public CentroidListCursor(ArrayList<Centroid> centroidList)
	{
		this.centroidList = centroidList;
	}
	/* (non-Javadoc)
	 * @see database.CollectionCursor#next()
	 */
	public boolean next() {
		i++;
		if (i < centroidList.size())
			return true;
		else
			return false;
	}

	/* (non-Javadoc)
	 * @see database.CollectionCursor#getCurrent()
	 */
	public ParticleInfo getCurrent() {
		ParticleInfo returnThis = new ParticleInfo();
		returnThis.setID(i);
		returnThis.setBinnedList(centroidList.get(i).peaks);
		return returnThis;
	}

	/* (non-Javadoc)
	 * @see database.CollectionCursor#close()
	 */
	public void close() {
	}

	/* (non-Javadoc)
	 * @see database.CollectionCursor#reset()
	 */
	public void reset() {
		i = -1;
	}

	/* (non-Javadoc)
	 * @see database.CollectionCursor#get(int)
	 */
	public ParticleInfo get(int i) throws NoSuchMethodException {
		ParticleInfo returnThis = new ParticleInfo();
		returnThis.setID(i);
		return returnThis;
	}
	
	public BinnedPeakList getPeakListfromAtomID(int id) {
		return centroidList.get(id).peaks;
	}
}
