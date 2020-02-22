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
 * The Original Code is EDAM Enchilada's CalInfo class.
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


package edu.carleton.dataloadbenchmark;

import java.io.*;
/*
 * Created on Jul 15, 2004
 */

/**
 * @author andersbe
 */
public class CalInfo {
	public double negSlope, posSlope, negIntercept, posIntercept;
	public float c1,c2,c3,c4;
	public boolean sizecal, autocal;
	
	public CalInfo()
	{
		negSlope = posSlope = 1;
		negIntercept = posIntercept = 0;
		c1 = 1;
		c2 = c3 = c4 = 0;
		sizecal = false;
		autocal = false;
	}

	/**
	 * Creates a CalInfo object from the given mass cal and size cal
	 * files.  
	 * 
	 * @param massCalFile
	 * The path to a mass cal file.
	 * @param sizeCalFile
	 * The path to a size cal file.
	 */
	public CalInfo(String massCalFile, String sizeCalFile, boolean auto) 
		throws IOException, NumberFormatException
	{
		autocal = auto;
		sizecal = true;
		loadMassCal(massCalFile);
		

		File sizeCal = new File(sizeCalFile);
		BufferedReader in = new BufferedReader(new FileReader(sizeCal));
		in.readLine();
		in.readLine();
		boolean negative = false;
		float[] cArray = new float[4];
		for (int i=0; i<4; i++) {
			in.skip(3);
			String number = in.readLine();
			if (number.charAt(1) == '-') {
				number = number.substring(2);
				negative = true;
			}
			Float cFloat = Float.valueOf(number);
			cArray[i] = cFloat.floatValue();
			if (negative)
				cArray[i] = -(cArray[i]);
		}
		c1 = cArray[0];
		c2 = cArray[1];
		c3 = cArray[2];
		c4 = cArray[3];
		in.close();
	}
	
	/**
	 * Creates a CalInfo object from the given mass cal file and 
	 * ignores size.  
	 * 
	 * @param massCalFile
	 * The path to a size cal file. 
	 */
	public CalInfo(String massCalFile, boolean auto) 
		throws IOException, NumberFormatException
	{
		autocal = auto;
		sizecal = false;

		loadMassCal(massCalFile);
	}
	
	private void loadMassCal(String massCalFile) throws IOException
	{
		File massCal = new File(massCalFile);
		BufferedReader in = new BufferedReader(new FileReader(massCal));
		
		// positive slope
		posSlope = Double.valueOf(in.readLine()).doubleValue();
		// positive intercept
		posIntercept = Double.valueOf(in.readLine()).doubleValue();
		negSlope = Double.valueOf(in.readLine()).doubleValue();
		negIntercept = Double.valueOf(in.readLine()).doubleValue();
		
		in.close();
	}
	
	/**
	 * @return Returns true if there is a size cal file associated with
	 * this object.
	 */
	public boolean isSizeCal() {
		return sizecal;
	}
	/**
	 * @return Returns the c1.
	 */
	public double getC1() {
		return c1;
	}
	/**
	 * @return Returns the c2.
	 */
	public double getC2() {
		return c2;
	}
	/**
	 * @return Returns the c3.
	 */
	public double getC3() {
		return c3;
	}
	/**
	 * @return Returns the c4.
	 */
	public double getC4() {
		return c4;
	}
	/**
	 * @return Returns the negIntercept.
	 */
	public double getNegIntercept() {
		return negIntercept;
	}
	/**
	 * @return Returns the negSlope.
	 */
	public double getNegSlope() {
		return negSlope;
	}
	/**
	 * @return Returns the posIntercept.
	 */
	public double getPosIntercept() {
		return posIntercept;
	}
	/**
	 * @return Returns the posSlope.
	 */
	public double getPosSlope() {
		return posSlope;
	}
}
