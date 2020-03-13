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
 * The Original Code is EDAM Enchilada's CreateATOFMSAtomFromDB class.
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
 * Created on Aug 9, 2004
 */
package edu.carleton.clusteringbenchmark.atom;

import java.util.Date;

/**
 * @author andersbe
 *
 */
public class CreateATOFMSAtomFromDB {

	private String filename;
	private Date timeStamp;
	private int atomID;
	private float laserPower;
	private float size;
	private int scatDelay;
	private String dateString;
	
	/**
	 * 
	 */
	public CreateATOFMSAtomFromDB() {
	}
	
	public CreateATOFMSAtomFromDB(int aID, String fname, int sDelay, float lPower, Date tStamp)
	{
		atomID = aID;
		filename = fname;
		laserPower = lPower;
		timeStamp = tStamp;
		scatDelay = sDelay;
	}

	/**
	 * @return Returns the atomID.
	 */
	public int getAtomID() {
		return atomID;
	}
	/**
	 * @param atomID The atomID to set.
	 */
	public void setAtomID(int atomID) {
		this.atomID = atomID;
	}
	/**
	 * @return Returns the filename.
	 */
	public String getFilename() {
		return filename;
	}
	/**
	 * @param filename The filename to set.
	 */
	public void setFilename(String filename) {
		this.filename = filename;
	}
	/**
	 * @return Returns the size.
	 */
	public float getSize() {
		return size;
	}
	/**
	 * @param size The size to set.
	 */
	public void setSize(float size) {
		this.size = size;
	}
	/**
	 * @return Returns the laserPower.
	 */
	public float getLaserPower() {
		return laserPower;
	}
	/**
	 * @param laserPower The laserPower to set.
	 */
	public void setLaserPower(float laserPower) {
		this.laserPower = laserPower;
	}
	/**
	 * @return Returns the timeStamp.
	 */
	public Date getTimeStamp() {
		return timeStamp;
	}
	/**
	 * @param timeStamp The timeStamp to set.
	 */
	public void setTimeStamp(Date timeStamp) {
		this.timeStamp = timeStamp;
	}
	/**
	 * @return Returns the scatDelay.
	 */
	public int getScatDelay() {
		return scatDelay;
	}
	/**
	 * @param scatDelay The scatDelay to set.
	 */
	public void setScatDelay(int scatDelay) {
		this.scatDelay = scatDelay;
	}
	/**
	 * @return Returns the dateString.
	 */
	public String getDateString() {
		return dateString;
	}
	/**
	 * @param dateString The dateString to set.
	 */
	public void setDateString(String dateString) {
		this.dateString = dateString;
	}
}
