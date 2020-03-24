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
 * The Original Code is EDAM Enchilada's NonZeroCursor class.
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
 * Created on Dec 20, 2004
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package edu.carleton.clusteringbenchmark.database;

import edu.carleton.clusteringbenchmark.ATOFMS.ParticleInfo;
import edu.carleton.clusteringbenchmark.analysis.BinnedPeakList;

/*
 * Wraps around another binned cursor, and automatically filters out
 * atoms with no peaks. Also keeps track of how many such atoms there
 * are. 
 */
public class NonZeroCursor implements CollectionCursor {
    protected CollectionCursor cursor;
    protected ParticleInfo particleInfo;
    protected int zeroCount;
    protected boolean countComplete;
    

    public NonZeroCursor(CollectionCursor wrappee) {
        cursor = wrappee;
        particleInfo = null;
        zeroCount = 0;
        countComplete = false;
    }
    
    public void reset() {
        cursor.reset();
        if (!countComplete)
            zeroCount = 0;
    }
    
    public boolean next() {
        // Make sure that the next record up for grabs actually has
        // a non-empty peak list.
        boolean stillLooking = true;
        while (stillLooking) {
            boolean moreData = cursor.next();
            if (!moreData) {
                countComplete = true;
                return false;
            }
            
            particleInfo = cursor.getCurrent();
            if (particleInfo.getBinnedList() == null || 
                particleInfo.getBinnedList().length() == 0) {
                if (!countComplete)
                    zeroCount++;
                stillLooking = true;
            } else
                stillLooking = false;
        };

        return true;
    }
    
    public ParticleInfo getCurrent() {
        return particleInfo;
    }
	
	public void close() {
	    cursor.close();
	}
}
