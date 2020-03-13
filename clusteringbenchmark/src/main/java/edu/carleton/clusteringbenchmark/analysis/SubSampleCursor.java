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
 * The Original Code is EDAM Enchilada's SubSampleCursor class.
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
 * Created on Oct 17, 2004
 */

package edu.carleton.clusteringbenchmark.analysis;

import edu.carleton.clusteringbenchmark.ATOFMS.ParticleInfo;
import edu.carleton.clusteringbenchmark.database.CollectionCursor;
import edu.carleton.clusteringbenchmark.errorframework.ErrorLogger;

import java.util.ArrayList;

/**
 * @author ritza
 * @author dmusican
 * 
 * Assumes that subsample is small enough to remain in memory, and thus
 * caches the data in memory for repeated reads.
 * 
 * Cursor for the subsamples; gets particleInfo for each particle in the 
 * given subsample.
 */
public class SubSampleCursor implements CollectionCursor {

	private CollectionCursor curs;
	private final int startIndex;
	private int currentIndex = -1;
	private int sampleSize;
	private ArrayList<ParticleInfo> storedInfo = null;
	private boolean firstPass = true;
	
	public SubSampleCursor(CollectionCursor curs,
                           int startIndex, int sampleSize)
	{
		this.curs = curs;
		this.startIndex = startIndex;
		this.sampleSize = sampleSize;
		storedInfo = new ArrayList<ParticleInfo>(sampleSize);
		
		curs.reset();
		for (int i = 0; i < startIndex; i++)
			curs.next();
	}	
	/* (non-Javadoc)
	 * @see database.CollectionCursor#next()
	 */
	public boolean next() {
	    currentIndex++;
	    if (firstPass) {
   	        if (currentIndex < sampleSize) {
	            boolean cursNext = curs.next();
	            if (cursNext)
	                storedInfo.add(curs.getCurrent());
	            else {
	    			ErrorLogger.writeExceptionToLogAndPrompt("SubSampleCursor",
	    					"Sample size was too large, internal error.");
	            }
	            return cursNext;            
	        }
   	        firstPass = false;
	        return false;      
	    } else
	    	return (currentIndex < storedInfo.size());
	}

	/* (non-Javadoc)
	 * @see database.CollectionCursor#getCurrent()
	 */
	public ParticleInfo getCurrent() {
		if (currentIndex < storedInfo.size()) {
			ParticleInfo particleInfo = storedInfo.get(currentIndex);
			return particleInfo;
		}
		else {
			return null;
		}
	}

	/* (non-Javadoc)
	 * @see database.CollectionCursor#close()
	 */
	public void close() {}

	/* (non-Javadoc)
	 * @see database.CollectionCursor#reset()
	 */
	public void reset() {
	    if (firstPass) {
	        storedInfo.clear();
	        curs.reset();
	        currentIndex = -1;
	        for (int i = 0; i < startIndex; i++)
	            curs.next();
	    }
	    currentIndex = -1;
	}

	/* (non-Javadoc)
	 * @see database.CollectionCursor#get(int)
	 * 
	 * Returns null if particle has not been archived yet.
	 */
	public ParticleInfo get(int i) throws NoSuchMethodException {
	    if (i < storedInfo.size())
	        return storedInfo.get(i);
	    else
	        return null;
	}

	/**
	 * This method only returns the peaklist of it has already read the ID.
	 */
	public BinnedPeakList getPeakListfromAtomID(int id) {
	    for (ParticleInfo particleInfo : storedInfo) {
	        if (particleInfo.getID() == id)
	            return particleInfo.getBinnedList();
	    }
	    return null;	
	}

}
