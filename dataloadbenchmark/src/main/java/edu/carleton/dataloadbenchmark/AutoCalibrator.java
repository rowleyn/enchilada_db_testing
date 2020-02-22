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
 * The Original Code is EDAM Enchilada's AutoCalibrator class.
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
 * Acknowledgments:
 * TSI, Inc. and the Prather Group at UCSD for original spec in
 * c and vb code.
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
 * Created on Jul 30, 2004
 *
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package edu.carleton.dataloadbenchmark;

/**
 * @author andersbe
 *
 */
public class AutoCalibrator {

	public static final int POS = 0;
	public static final int NEG = 1;
	private static final int MAX_CALIB_POINT = 16;
	private static final int MAX_PEAKS = 16;
	private static final int MAX_AMS_SPECTRUM_POINTS = 30000;
	private static final int FLIGHT_TUBE_LENGTH = 1;
	
	
	private static int posZeroPoint;
	private static double posVoltage;
	private static int negZeroPoint;
	private static double negVoltage;
	
	private static float oldDigitRate = -1;

	private static double timeTranslateReverse(double mass,
											   int spectrumType)
	{
		// CalibSlope and CalibIntercept are the A and B values from mass.cal
		if (spectrumType == POS)
				if (mass >= 0) {
					return ((Math.sqrt(Math.abs(mass)) - ATOFMSParticle.currCalInfo.posSlope) / 
							ATOFMSParticle.currCalInfo.posIntercept);
				}
				else 
					return ((-2 * (ATOFMSParticle.currCalInfo.posSlope / 
							ATOFMSParticle.currCalInfo.posIntercept)) - 
							((Math.abs(mass) * Math.abs(mass)) - ATOFMSParticle.currCalInfo.posSlope) / 
							ATOFMSParticle.currCalInfo.posIntercept);

		else
				if (mass >= 0) {
					
					return ((Math.sqrt(Math.abs(mass)) - ATOFMSParticle.currCalInfo.negSlope) / 
							ATOFMSParticle.currCalInfo.negIntercept);
				}
				else {
					return ((-2 * (ATOFMSParticle.currCalInfo.negSlope / 
							ATOFMSParticle.currCalInfo.negIntercept)) - 
							(Math.sqrt(Math.abs(mass)) - ATOFMSParticle.currCalInfo.negSlope) / 
							ATOFMSParticle.currCalInfo.negIntercept);
				}
	}
	
	
	
	
	
	
	///////////////////////////////////////////////////////////////////////////////
	//Function name: AutoCalibrate
	//Description:   Looks at a single spectrum and determines the best
	//			calibration for it.
	//Returns: 
	
	//The autocalibrator does not calibrate mass spectra from scratch.  It
	//fits a calibration to them once an approximate calibration is supplied.
	//The manner in which it does this is farily simple, although it took
	//several tries before we actually got one that worked.
	
	//One way to calibrate a time-of-flight mass spectrum is to determine the
	//time at which the ions were generated (the zero point) and the
	//acceleration voltage used to propel the ions down the flight tube
	//(voltage).  If you know the length of the flight tube, then you can
	//retrocalculate either of these two values if you know the other one and
	//know the time-of-flight of any one ion.  If you know the length of the
	//flight tube and the times-of-flight of any two ions, then you can
	//determine both values.  The first step, then, is to determine the zero
	//point, either by measuring it during the acquisition of the mass
	//spectrum, using a known voltage and an accurate flight tube length and
	//an internal standard, or using the times-of-flight of two known species
	//to predict it.  I recommend the first way.  Second, by assuming a flight
	//tube length (this doesn't have to be accurate--I used 1 M in my
	//calculations), you determine the voltage.  These two values, the
	//parameter voltage and parameter zero point, are used to refine the
	//calibrations of all subsequent mass spectra.
	
	//Once the parameter values for the voltage and zero point are calculated,
	//the times of flight of the first 100 m/z values are determined.  The
	//integral of all points within .1 m/z of each whole number for the first
	//100 m/z is summed and this is recorded as a score.  The zero point is
	//assumed to be a constant.  The voltage is assumed to be able to move
	//around a bit depending upon which part of the source the particle was in
	//when it was ionized.  So the voltage is scanned in .01 % increments from
	//-2% to +2% of the parameter voltage and each example is scored.  The
	//test voltage with the highest score is assumed to be the correct one.
	
	//The single pitfall to this technique, which is the best we have so far,
	//is that there is a tremendous advantage to parking two whole mass units
	//within a wide peak that blows scale on the mass spectrum.  There may be
	//some way of addressing that but it is important to note that
	//occasionally a peak is truly wide enough to cover several m/z worth of
	//spectrum.  Another avenue to consider is the fact that all of the
	//integral within .1 m/z of a whole m/z is considered equal in weight at
	//present, but that could be modified to weight the spectrum nearer the
	//whole m/z higher.  Also, with your instrument's superior mass spectra,
	//it may be possible to narrow up the +/- .1 m/z window.  Finally, this
	//has only ever been tested on 8 bit mass spectra.  It is possible that
	//wider dynamic ranges may need/enable other techniques.  At worst though,
	//it is possible to truncate the tall peaks in a mass spectrum of wider
	//dynamic range.
	//
	//Contributed by: MS Analyze folks (TSI Inc?), appears to be adapted from
	//Davio.bas from TASware.
	///////////////////////////////////////////////////////////////////////////////
	
	public static double[] autoCalibrate(float digitRate,
										 int charge,
										 int data[])
			//int& TOF1, double& MZ1,
			//int& TOF2, double& MZ2)
	{
		// Figure out how often we actually need to do this,
		// It looks like it only needs to be calculated everytime
		// digit rate changes.
		// 12/05/04: Removed todo from above comment, Autocal 
		// seems to work so I think this is correct.  
		if (digitRate != oldDigitRate)
			getZeroPointAndVoltage(digitRate);
		
		long i, ThisMass;
		int k;
		double TempMinFlightTime, TempMaxFlightTime;
		int MinFlightTime,	MaxFlightTime;
		int MaxIntegral;
		double EnergyInJoules;
		double EnergyInDaltonsPerMSquaredPerSSquared;
		double VelocityInMetersPerSecond;
		double AccelerationVoltage;
		int Integral;
		double CalculatedAccelerationVoltage;
		
		
		final int MIN_MASS = 1;
		final int MAX_MASS = 100;
		final double WINDOW	= 0.1;
		
		double Voltage = 0;
		int ZeroPoint = 0;
		if (charge == POS)
		{
			Voltage = posVoltage;
			ZeroPoint = posZeroPoint;
		}
		else
		{
			Voltage = negVoltage;
			ZeroPoint = negZeroPoint;
		}
		
		//System.out.println("Voltage: " + Voltage);
		//System.out.println("ZeroPoint: " + ZeroPoint);
		//System.out.println(ATOFMSParticle.currCalInfo.posSlope);
		//System.out.println(ATOFMSParticle.currCalInfo.negSlope);
		
		if (Voltage > 0)
		{
			MaxIntegral = 0;
			CalculatedAccelerationVoltage = 0;

			////////////////////////////////////////////////////////////////////////
			// 1. Search within plus or minus 2% of the voltage for the best fit.
			////////////////////////////////////////////////////////////////////////
			for (i = -200;i <= 200;++i)
			{
				Integral = 0;
				
				/////////////////////////////////////////////////////////////////////
				// 1a. Determine the energy that would be produced by this voltage
				/////////////////////////////////////////////////////////////////////
				AccelerationVoltage = Voltage * (1.0 + ((double)i / 10000));
				
				EnergyInJoules = 1.602E-19 * AccelerationVoltage;
				EnergyInDaltonsPerMSquaredPerSSquared = EnergyInJoules 
				* 6.02E+23 * 1000;
				
				/////////////////////////////////////////////////////////////////////
				// 1b. Find the integral mass value that has the biggest peak in the
				// value described by this voltage value.
				/////////////////////////////////////////////////////////////////////
				for (ThisMass = MIN_MASS;ThisMass <= MAX_MASS;++ThisMass)
				{
					/////////////////////////////////////////////////////////////////
					// 1c. Calculate a range of TOF values that correspond to this 
					// mass and the current voltage value.
					/////////////////////////////////////////////////////////////////
					VelocityInMetersPerSecond = 
						Math.sqrt(2 * EnergyInDaltonsPerMSquaredPerSSquared / 
								((double)ThisMass - WINDOW));
					TempMinFlightTime = (FLIGHT_TUBE_LENGTH /	
							VelocityInMetersPerSecond) * 1000000 * 500;
					VelocityInMetersPerSecond = 
						Math.sqrt(2 * EnergyInDaltonsPerMSquaredPerSSquared / 
								((double)ThisMass + WINDOW));
					TempMaxFlightTime = (FLIGHT_TUBE_LENGTH /	
							VelocityInMetersPerSecond) * 1000000 * 500;
					MinFlightTime = (int)((TempMinFlightTime + 0.5) + ZeroPoint);
					MaxFlightTime = (int)(TempMaxFlightTime + 0.5) + ZeroPoint;
					//if(MaxIntegral == 0)
					//{
						//System.out.println("MinFlightTime value: " + 
								//data[MinFlightTime]);
						//System.out.println("MaxFlightTime value: " + 
								//data[MaxFlightTime]);
						//System.out.println("Charge: " + charge);
					//}
					
					/////////////////////////////////////////////////////////////////
					// 1d. Find the size of the peak in this range.
					/////////////////////////////////////////////////////////////////
					for (k = MinFlightTime;k <= MaxFlightTime;++k)
					{
						//System.out.println("k: " + k);
						if (k > 0)
						{
							//System.out.println("In the loop");
							//System.out.println(data[k]);
							Integral += data[k];
						}
						else
							Integral = -10000000;
					}
				}
				/////////////////////////////////////////////////////////////////////
				// 1e. If this integral is the largest we've found so far, it implies
				// that the voltage we're currently considering produces the best 
				// fit, so record this.
				/////////////////////////////////////////////////////////////////////
				//System.out.println("Integral: " + Integral);
				if (Integral > MaxIntegral)
				{
					CalculatedAccelerationVoltage = AccelerationVoltage;
					MaxIntegral = Integral;
				}
			}
			
			////////////////////////////////////////////////////////////////////////
			// 2. Now we have the voltage that produces the best fit.  Use this
			// voltage to calculate the energy produced by this voltage.
			////////////////////////////////////////////////////////////////////////
			//System.out.println("Before loop");
			//System.out.println("Voltage: " + Voltage);
			//System.out.println("Calculated Acceleration Voltage: " + CalculatedAccelerationVoltage);
			//System.out.println("ZeroPoint: " + ZeroPoint);

			if ((CalculatedAccelerationVoltage > 0) &&
					(ZeroPoint >= 0)) 
			{
				/////////////////////////////////////////////////////////////////////
				// 3. Calculate the time of flight for two m/z values, 50 and 100.
				// These value will be returned to the caller, and will be used to 
				// create a unique calibration function for this spectrum.
				/////////////////////////////////////////////////////////////////////
				EnergyInJoules = 1.602E-19 * CalculatedAccelerationVoltage;
				EnergyInDaltonsPerMSquaredPerSSquared = EnergyInJoules * 6.02E+23 
				* 1000;
				
				VelocityInMetersPerSecond = 
					Math.sqrt(2 * EnergyInDaltonsPerMSquaredPerSSquared / 50);
				int TOF1 = (int)((FLIGHT_TUBE_LENGTH / VelocityInMetersPerSecond) 
						* 1000000 * 500) + ZeroPoint;
				float MZ1 = 50;
				
				VelocityInMetersPerSecond = 
					Math.sqrt(2 * EnergyInDaltonsPerMSquaredPerSSquared / 100);
				int TOF2 = (int)((FLIGHT_TUBE_LENGTH / VelocityInMetersPerSecond) 
						* 1000000 * 500) + ZeroPoint;
				float MZ2 = 100;
				//System.out.println("About to cal from two points");
				return calFromTwoPoints(TOF1, MZ1, TOF2, MZ2);
			}
			else
				return null;
		}
		else
			return null;
		
	}
	
	
	///////////////////////////////////////////////////////////////////////////////
	//Function name: GetZeroPointAndVoltage
	//Description:   Determines the zero point and voltage that obtained when 
	//	  the current data set was created.  These values are later
	//	  refined in the AutoCalibrate function.
	//Returns: 
	//
	//You've probably figured out by now that you have to round the answer to
	//the nearest whole number to get the right answer. I should have put a
	//safeguard in to prevent the zeropoint from ever being zero (I have one
	//within the scoring procedure to keep any m/z from having a negative
	//channel number) but it hasn't been an issue because this would never in
	//practice be the case. It could cause a crash if it ever were.
	//
	//PARTICLE_MASS is misnamed. It should be named IonMass. It is the m/z if a
	//fictitious peak. Any good calibrator would find the best fit calibration
	//from many calibrated peaks. This finds the channel number of a peak at,
	//say, m/z = 100, and then uses that information to find out the voltage
	//required to put it there.
	//
	//At this point I have to make a suggestion that I should have made a long
	//time ago. TasWare is based on a slope/intercept calibration. This is why
	//I use the above functions to generate a virtual voltage. It would also
	//be possible, and of more physical relevance, to use voltage/intercept as
	//I do in the autocalibration algorithm. That would be better because if
	//the mass spectrometer were modeled properly the calibration could be
	//entered a priori. It is still possible to do this, but harder with
	//slopes and intercepts. Just an idea.
	//
	// Contributed by TSI inc, again seems to be from Tasware
	///////////////////////////////////////////////////////////////////////////////
	private static boolean getZeroPointAndVoltage(float digitRate)
	{
		double TimeOfFlight, Time, VelocityInMetersPerSecond;
		double EnergyInDaltonsPerMSquaredPerSSquared, Joules;
		
		
		final int PARTICLE_MASS = 100;
		
		
		// calculate the zero points
		// The zero value is calculated by finding the channel number of a peak
		// at mass 0.0000000001 or so. You'd get a divide by zero error for 
		// obvious reasons if you tried to find the mass at zero by the 
		// following procedure:
		posZeroPoint = (int) MZ2TOF(1E-44, POS);
		negZeroPoint = (int) MZ2TOF(1E-44, NEG);
		//cout << "debug: negZeroPoint = " << negZeroPoint << endl;
		
		
		///////////////////////////////////////////////////////////////////////
		// calculate the positive voltage
		///////////////////////////////////////////////////////////////////////
		
		// Calculate the transit time of a positive ion.
		TimeOfFlight = MZ2TOF(PARTICLE_MASS, POS) - posZeroPoint; 
		// now it's in TOF units 
		
		// Put it in seconds
		Time = TimeOfFlight / digitRate;
		
		// Now convert it to velocity
		VelocityInMetersPerSecond = FLIGHT_TUBE_LENGTH / Time;
		
		// Put it in terms of energy in AMU * (M/SH)^2
		EnergyInDaltonsPerMSquaredPerSSquared = 0.5 * PARTICLE_MASS * 
		Math.pow(VelocityInMetersPerSecond, 2);
		
		// Put in terms of Joules
		Joules = EnergyInDaltonsPerMSquaredPerSSquared / (6.02E+23 * 1000);
		
		// Put in terms of ev
		posVoltage = Joules / 1.602E-19;
		
		
		///////////////////////////////////////////////////////////////////////
		// calculate the negative voltage
		///////////////////////////////////////////////////////////////////////
		
		// Calculate the transit time of a positive ion.
		TimeOfFlight = MZ2TOF(PARTICLE_MASS, NEG) - negZeroPoint; 
		// now it's in TOF units 
		
		// Put it in seconds
		Time = TimeOfFlight / digitRate;
		
		// Now convert it to velocity
		VelocityInMetersPerSecond = FLIGHT_TUBE_LENGTH / Time;
		
		// Put it in terms of energy in AMU * (M/SH)^2
		EnergyInDaltonsPerMSquaredPerSSquared = 0.5 * PARTICLE_MASS * 
		Math.pow(VelocityInMetersPerSecond, 2);
		
		// Put in terms of Joules
		Joules = EnergyInDaltonsPerMSquaredPerSSquared / (6.02E+23 * 1000);
		
		// Put in terms of ev
		negVoltage = Joules / 1.602E-19;
		
		
		return true;
	}

	//from mass calibration formula
	//note:  In TASWare the results of this calculation were rounded, in 
	//MS-Analyze they are simply truncated. -Ben
	private static int MZ2TOF(double mass, int charge)
	{
		if (charge == POS)
		{
			return (int) (((Math.sqrt(Math.abs(mass)) - 
					ATOFMSParticle.currCalInfo.posIntercept) / 
					ATOFMSParticle.currCalInfo.posSlope) 
					/*+ 0.5*/);
		}
		else if (charge == NEG)
		{
			return (int)(((Math.sqrt(Math.abs(mass)) - 
					ATOFMSParticle.currCalInfo.negIntercept) / 
					ATOFMSParticle.currCalInfo.negSlope) 
					/*+ 0.5*/);
		}
		else
			throw new IllegalArgumentException(
					"Must input charge as either AutoCalibrator.POS " +
					"or AutoCalibrator.NEG");
	}

	//from mass calibration fromula
	private static double[] calFromTwoPoints(int tOF1, double charge1,
		      int tOF2, double charge2)
	{
		double returnVals[] = new double[2];
		returnVals[0] = (Math.sqrt(charge1) - Math.sqrt(charge2)) / (tOF1 - tOF2);
		returnVals[1] = Math.sqrt(charge1) - tOF1 * returnVals[0];
		return returnVals;
	}
}
