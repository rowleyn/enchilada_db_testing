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
 * The Original Code is EDAM Enchilada's Clusters class.
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

// Note: presently the advanced normalization techniques are only implemented for K-Means -- MM

/*
 * Created on Aug 19, 2004
 */
package edu.carleton.clusteringbenchmark.analysis.clustering;

import edu.carleton.clusteringbenchmark.ATOFMS.ParticleInfo;
import edu.carleton.clusteringbenchmark.analysis.*;
import edu.carleton.clusteringbenchmark.database.CollectionCursor;
import edu.carleton.clusteringbenchmark.database.DynamicTable;
import edu.carleton.clusteringbenchmark.database.InfoWarehouse;
import edu.carleton.clusteringbenchmark.errorframework.NoSubCollectionException;
import gnu.trove.map.hash.TIntShortHashMap;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author andersbe
 * @author jtbigwoo
 * This abstract class implements methods specific to Cluster 
 * algorithms.
 */
public abstract class Cluster extends CollectionDivider {
	protected ArrayList<Double> totalDistancePerPass;
	protected int collectionID;
	protected String parameterString;
	protected String folderName;
	protected static double power = 1.0;	//the power to which the peak areas
											//are raised during preprocessing.

	protected static final String quote = "'";

	// this used to be called the EVIL HACK CONSTANT.  Now it's a user-
	// specified value.  It's the smallest normalized peak value that we'll
	// include in the graphing.  We divide by this value to de-normalize the
	// data.  (It's still a hack, but it's only shameful rather than evil now.)
	protected static float smallestNormalizedPeak = 0.0001f;

	public static final int ARRAYOFFSET = 600; // size of centroid array
			// in both negative and positive directions (1201 total) when
			// copying as an array of floats for efficiency

	protected DistanceMetric distanceMetric = DistanceMetric.CITY_BLOCK;
	
	protected int zeroPeakListParticleCount = 0;
	protected int clusterCentroidIters = 0;
	protected int sampleIters = 0;
	
	protected boolean isNormalized;
	protected static PeakTransform peakTransform = PeakTransform.NONE;
	protected static boolean posNegNorm = true;
	
	/**
	 * Builds the cluster name a bit, then sends information off
	 * to the CollectionDivider constructor
	 * @param cID		The id of the collection to cluster
	 * @param database	An active InfoWarehouse
	 * @param name		A name to append to ",CLUST"
	 * @param comment	A comment for the cluster.
	 */
	public Cluster(int cID, InfoWarehouse database, String name,
                   String comment, boolean norm) {
		super(cID,database,name.concat(",CLUST"),comment);
		folderName = "," + super.comment;
		isNormalized = norm;
		totalDistancePerPass = new ArrayList<Double>();
	}
	
	// For efficiency it can be useful to represent a list of centroids as
	// a list of arrays of floats.
	// Code by benzaids, wrapped by dmusican
	public static ArrayList<float[]> generateCentroidArrays(
					ArrayList<Centroid> centroidList, int arrayoffset)
	{
		//BUILD AN ARRAYLIST OF CENTROID INFO
		//THIS INFO IS PASSED TO GETDISTANCE, WHICH MAKES IT FASTER
		// - benzaids
		ArrayList<float[]> tempCentroidList = new ArrayList<float[]>();
		for (int i = 0; i < centroidList.size(); i++)
		{
			BinnedPeakList temp = centroidList.get(i).peaks;
			float[] peakInfo = new float[arrayoffset*2+1];
			for (int q = 0; q < peakInfo.length; q++)
			{
				int tempkey = q - arrayoffset;
				if (temp.getPeaks().containsKey(tempkey))
					peakInfo[q] = temp.getPeaks().get(tempkey);
				else
					peakInfo[q] = 0;
			}
			tempCentroidList.add(peakInfo);
		}
		//**********
		return tempCentroidList;
	}
	
	/**
	 * Writes out the mean and standard deviation of the sizes of particles in
	 * the supplied collection.
	 * @param collectionId
	 */
	protected void writeSizeMeanAndStdDev(int collectionId, PrintWriter out)
	{
		// mean and standard deviation of size
		CollectionCursor densecurs = db.getAtomInfoOnlyCursor(db.getCollection(collectionId));
		int count = 0;
		float sum = 0f;
		float product = 1f;
		float squaresum = 0f;
		for (count = 0; densecurs.next(); count++)
		{
			ParticleInfo info = densecurs.getCurrent();
			sum += info.getATOFMSParticleInfo().getSize();
			squaresum += info.getATOFMSParticleInfo().getSize() * info.getATOFMSParticleInfo().getSize();
			product *= info.getATOFMSParticleInfo().getSize();
		}
		densecurs.close();
		float mean = sum / count;
		float standardDevation = (float) Math.sqrt(squaresum / count - mean * mean);
		out.println("Mean size: " + mean + " Std dev: +/-" + standardDevation);
		float geometricMean = (float) Math.pow(product, 1f/count);
		out.println("Geometric mean size: " + geometricMean);
	}
	
	/**
	 * Prints out each peak in a peaklist.  Used for reporting results.
	 * @param inputList		The list to print out
	 * @param out			A printwriter to print to
	 */
	protected void writeBinnedPeakListToFile(
			BinnedPeakList inputList,
			PrintWriter out)
	{
		out.println("Key:\tValue:");
		Iterator<BinnedPeak> iter = inputList.iterator();
		BinnedPeak tempPeak;
		while (iter.hasNext())
		{
			tempPeak = iter.next();
			out.println(tempPeak.getKey() + "\t" + tempPeak.getValue());
		}
	}

	/**
	 * This method assigns atoms to nearest centroid using only centroidList
	 * and collection cursor. ClusterK only. This also averages the final
	 * centroids with k-medians, so it is easier to compare with k-means.
	 * @param centroidList
	 * @param curs
	 * @return
	 */
	protected int assignAtomsToNearestCentroid(
			ArrayList<Centroid> centroidList,
			CollectionCursor curs, boolean saveCentroids){
		return assignAtomsToNearestCentroid(centroidList, curs, (double)2.5, isNormalized, true, saveCentroids);
	}
	/**
	* This method assigns atoms to nearest centroid using only centroidList
	* and collection cursor.  
	* @param centroidList the list of centroids
	* @param curs the cursor over the collection
	* @param minDistance used by cluster query, indicates the minimum distance
	* a particle must be from a centroid
	* @param normalize whether the centroids are normalized
	* @param changeCentroids whether the centroids should be modified as particles are assigned to them
	* @param saveCentroids whether the we save the centroids in the db.
	* Set this to false when you're interested in saving the centroids to the db, but
	* you don't want to actually divide up the source
	* particles in the db. (Such as in pre-clustering for hierarchical clustering.)
	* @author christej
	*/
	protected int assignAtomsToNearestCentroid(
			ArrayList<Centroid> centroidList,
			CollectionCursor curs, double minDistance, boolean normalize, boolean changeCentroids,
			boolean saveCentroids)
	{
		System.out.println("in assignAtoms");
		ArrayList<BinnedPeakList> sums = new ArrayList<BinnedPeakList>();
		if (isNormalized)
			for (int i = 0; i < centroidList.size(); i++)
				sums.add(new BinnedPeakList(new Normalizer()));
		else
			for (int i = 0; i < centroidList.size(); i++)
				sums.add(new BinnedPeakList(new DummyNormalizer()));
		
		int particleCount = 0;
		ParticleInfo thisParticleInfo = null;
		BinnedPeakList thisBinnedPeakList = null;
		BinnedPeakList rawBinnedPeakList = null;
		double nearestDistance = 3.0;
		double totalDistance = 0.0;
		double distance = 3.0;
		int chosenCluster = -1;
		TIntShortHashMap clusterMapping = new TIntShortHashMap();
		
		ArrayList<float[]> tempCentroidList =
			Cluster.generateCentroidArrays(centroidList,ARRAYOFFSET);
		int k = centroidList.size();
		float[] centroidMags = new float[k];
		for (int i=0; i < k; i++)
			centroidMags[i] = centroidList.get(i).peaks.getMagnitude(distanceMetric);
	
		try {
			db.bulkInsertInit();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try
		{
			while(curs.next())
			{ // while there are particles remaining
				particleCount++;
				thisParticleInfo = curs.getCurrent();
				thisBinnedPeakList = thisParticleInfo.getBinnedList().copyOf();
				
				// save the untransformed peaks
				rawBinnedPeakList = thisBinnedPeakList.copyOf();
				rawBinnedPeakList.normalize(distanceMetric,posNegNorm);
				
				thisBinnedPeakList.transformAreas(peakTransform);
				thisBinnedPeakList.normalize(distanceMetric,posNegNorm);
				
				nearestDistance = Float.MAX_VALUE;
				for (int centroidIndex = 0; centroidIndex < centroidList.size(); 
					centroidIndex++)
				{// for each centroid
					if (particleCount % 10000 == 0)
						System.out.println("Particle number = " + particleCount);
					distance = thisBinnedPeakList.getDistance(
							tempCentroidList.get(centroidIndex),centroidMags[centroidIndex],
							distanceMetric, Cluster.ARRAYOFFSET);
					if (distance < nearestDistance)
					{
						nearestDistance = distance;
						chosenCluster = centroidIndex;
						clusterMapping.put(thisParticleInfo.getID(), (short) chosenCluster);
					}
				}// end for each centroid

				// could add a catch for orphaned particles here -- MM 2014
				if(nearestDistance<minDistance){
					if(changeCentroids){
						// this uses the un-transformed peak areas -- MM 2014
						sums.get(chosenCluster).addAnotherParticle(rawBinnedPeakList);
					}


					Centroid temp = centroidList.get(chosenCluster);
					totalDistance += nearestDistance;
					if (temp.numMembers == 0)
					{
						temp.subCollectionNum = createSubCollection();
						if (temp.subCollectionNum == -1)
							System.err.println(
							"Problem creating sub collection");
					}
					putInSubCollectionBulk(thisParticleInfo.getID(),
							temp.subCollectionNum);
					System.out.println("putting in subcollectionbatch " + thisParticleInfo.getID() + " " + temp.subCollectionNum);
					temp.numMembers++;
				}
			}// end with no particle remaining
			putInSubCollectionBulkExecute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		curs.reset();
		if(!changeCentroids){
			for(int i = 0; i < sums.size(); i++){
				sums.get(i).addAnotherParticle(centroidList.get(i).peaks);
			}
		}
		totalDistancePerPass.add(new Double(totalDistance));
		for (int i = 0; i < sums.size(); i++) {
			sums.get(i).divideAreasBy(centroidList.get(i).numMembers);
			centroidList.get(i).peaks = sums.get(i);
		}
		
		// compute non-transformed centroid distances
		while(curs.next())
		{
			thisParticleInfo = curs.getCurrent();
			//thisBinnedPeakList = thisParticleInfo.getBinnedList().copyOf();
			thisBinnedPeakList = thisParticleInfo.getBinnedList();
			thisBinnedPeakList.normalize(distanceMetric, posNegNorm);
			
			short centroidIndex = clusterMapping.get(thisParticleInfo.getID());
			Centroid temp = centroidList.get(centroidIndex);
			
//			temp.sumDistances += distance;
//			temp.sumSqDistances += distance*distance;
		}
		curs.reset();
		
		if (normalize){
			//boost the peaklist
			// By dividing by the smallest peak area, all peaks get scaled up.
			// Because we're going to convert these to ints in a minute anything
			// smaller than the smallest peak area will get converted to zero.
			// it's a hack, I know-jtbigwoo
			for (Centroid c: centroidList){
				c.peaks.divideAreasBy(smallestNormalizedPeak);
			}
		}
		
		// kind of a hack here, getting the untransformed centroids can end up
		// with zero values in the peaklist. this fixes that -- MM 2014
		for (Centroid c : centroidList)
			c.peaks = c.peaks.getFilteredZerosList();
	
		if (saveCentroids) {
			createCenterAtoms(centroidList, subCollectionIDs);
		}
		printDescriptionToDB(particleCount, centroidList);
		
		return newHostID;
	}

	/**
	 * @author steinbel
	 * Creates the "center atoms" from a list of centroids, and puts them into
	 * a new root-level collection.
	 * 	
	 * @param centerList	The list of centroids.
	 * @param ids			The list of subcollection IDs for the cluster 
	 * 						collections to which the centroids belong.
	 */
	public void createCenterAtoms(ArrayList<Centroid> centerList,
			ArrayList<Integer> ids){
		
		//new root-level collection
		int centerCollID = createCenterCollection(parameterString, "");
		int centerAtomID;

		/*
		 * this is part of the disgusting attempt to make this datatype-flexible
		 */
		CollectionCursor densecurs;
		ParticleInfo info;
		ArrayList<String> denseValues;
		ArrayList<String> denseNames = new ArrayList<String>();
		ArrayList<Float> totValues;
		Float temp = new Float(0);
		boolean first = true;
		String dense = "";
		ArrayList<ArrayList<String>> denseInfo;
		String datatype;
		String sqlType;
		String colName;
		ArrayList<String> avgValues;
		ArrayList<String> intCols = new ArrayList<String>();
		ArrayList<String> charCols = new ArrayList<String>();
		ArrayList<String> bitCols = new ArrayList<String>();
		ArrayList<String> numeric = new ArrayList<String>();
		Pattern intP = Pattern.compile(".*INT");
		Pattern charP = Pattern.compile(".*CHAR.*");
		Pattern dTime = Pattern.compile("DATETIME");
		Pattern bitP = Pattern.compile("BIT");
		Matcher m;
		int q = 0;

		for (Centroid center: centerList){
			
			totValues = new ArrayList<Float>();
			first = true;
			avgValues = new ArrayList<String>();
			
			//System.out.println("Attempting to make an atom of " + center.subCollectionNum);//Debugging
			datatype = db.getCollectionDatatype(centerCollID);
			
			denseInfo = db.getColNamesAndTypes(datatype, DynamicTable.AtomInfoDense);
			
			//TODO: there's got to be a better way to do this - LES
			//identify column names of types that not are appropriate to average

			for (ArrayList<String> nameAndType : denseInfo){
				colName = nameAndType.remove(0);
				sqlType = nameAndType.remove(0);
				//System.out.println("col Name " + colName + " sql type " + sqlType); //Debugging
				m = intP.matcher(sqlType);
				if (m.matches())
					intCols.add(colName);
				else{
					m = charP.matcher(sqlType);
					if (m.matches())
						charCols.add(colName);
					else{
						m = dTime.matcher(sqlType);
						if (m.matches())
							//charCols.add(colName);
							bitCols.add(colName); //hack to get the cluster number to show up
						else{
							m = bitP.matcher(sqlType);
							if (m.matches())
								bitCols.add(colName);
							else
								numeric.add(colName);
						}
					}
				}
			}
			
			
			
			//get the dense info for each particle in the cluster.
			//iterate through the atoms in the cluster
			//adding their averageable and int columns together
			if(center.subCollectionNum == -1){
				throw new NoSubCollectionException();
			}
			densecurs = db.getAtomInfoOnlyCursor(db.getCollection(ids.get(center.subCollectionNum-1)));
			
			//NOTE: this is currently only set up to deal with ATOFMS particles
			while (densecurs.next()){
				info = densecurs.getCurrent();
				denseNames = info.getATOFMSParticleInfo().getFieldNames();
				denseValues = info.getATOFMSParticleInfo().getFieldValues();

				if (first){
					for (int i=1; i<=denseNames.size(); i++)
						totValues.add(new Float(0.0));
						first = false;
				}
				for (int i=1; i<denseNames.size(); i++){
					if (numeric.contains(denseNames.get(i)) || 
							intCols.contains(denseNames.get(i))){
						temp = Float.parseFloat(denseValues.get(i));
						totValues.set(i, totValues.get(i) + temp);
					}else{
						totValues.set(i, new Float(-99));
					}
						
				}
			}
			densecurs.close();
			
			//average the values			
			for (int i=1; i<totValues.size(); i++){
				
				if (numeric.contains(denseNames.get(i)) || intCols.contains(denseNames.get(i))){
					temp = totValues.get(i) / center.numMembers;
					if (intCols.contains(denseNames.get(i))){
						int j = temp.intValue();
						avgValues.add(Integer.toString(j));
					} else
						avgValues.add(Float.toString(temp));
				} else if(charCols.contains(denseNames.get(i))){
					avgValues.add("Center for cluster " + center.subCollectionNum);
				} else if(i == 1) {
					avgValues.add("0");
				} else
					avgValues.add("");

				dense = intersperse(avgValues.get(i-1),
						dense);
			
			}
			
			//System.out.println("DENSE " + dense);	//Debugging

			/*
			 * NOTE: Only the peak locations and areas are recorded right now
			 * because that's the only info found in a BinnedPeakList.
			 */
			BinnedPeakList relAreaPeakList = new BinnedPeakList(new Normalizer());
			relAreaPeakList.copyBinnedPeakList(center.peaks);
			// don't want posNegNormalize here, we've already normalized the individual peaks,
			// doing pos/neg separately would create incorrect data
			relAreaPeakList.normalize(distanceMetric);
			ArrayList<String> sparse = new ArrayList<String>();
			//put the sparse info into appropriate format
			Iterator<BinnedPeak> i = center.peaks.iterator();
			BinnedPeak p;
			//don't forget the rest of the info.  DUH.  Like Height, Rel.Area
			//get the names of fields and their field types
			ArrayList<ArrayList<String>> sparseNamesTypes = 
				db.getColNamesAndTypes(datatype, DynamicTable.AtomInfoSparse);
			String s = "";
			Float area, relArea;
			while (i.hasNext()) {
				p = i.next();
				//TODO: waaaaaaaaay too type-specific
				
				area = p.getValue();
				
				s = intersperse(
							Integer.toString(area.intValue()), Integer.toString(p.getKey()));
			
				// relative area is the normalized peak height.
				relArea = relAreaPeakList.getAreaAt(p.getKey());
				s = intersperse(relArea.toString(), s);

				// hard-code peakheight to 1.  Height is not meaningful
				// for cluster centers.
				s = intersperse("1", s);
				sparse.add(s);
			}
			
			centerAtomID = db.insertParticle(dense, sparse, db.getCollection(centerCollID),
					db.getNextID());
			
			//associate the new center atoms with the collections they represent
			//(the one is because clusters start at 1 and indices start at 0)
			db.addCenterAtom(centerAtomID, ids.get(center.subCollectionNum -1));

			dense = "";
			q++;
		}
	}
	
	/**
	 * prints the relevant info to the database
	 * @param particleCount
	 * @param centroidList
	 */
	protected void printDescriptionToDB(int particleCount,
			ArrayList<Centroid> centroidList)
	{
	
		// sort centroidList.
		ArrayList<Centroid> orderedList = new ArrayList<Centroid>();
		int numCentroids = centroidList.size();
		while (orderedList.size() != numCentroids) {
			int smallestIndex = 0;
			for (int i = 1; i < centroidList.size(); i++) 
				if (centroidList.get(i).subCollectionNum < 
						centroidList.get(smallestIndex).subCollectionNum) 
					smallestIndex = i;
			orderedList.add(centroidList.get(smallestIndex));
			centroidList.remove(smallestIndex);
		}
		centroidList = orderedList;
		
		PrintWriter out = null;
		StringWriter sendToDB = new StringWriter();
		out = new PrintWriter(sendToDB);
		
		out.println("Clustering Parameters: ");
		out.println(parameterString + "\n\n");
		
		out.println("Number of ignored particles with zero peaks = " + 
		        zeroPeakListParticleCount);
		if (distanceMetric == DistanceMetric.DOT_PRODUCT)
		    out.println("Distance shown here is actually 1 - dot product.");
		out.println("Total clustering passes during sampling = " + sampleIters);
		out.println("Total number of centroid clustering passes = " +
		        clusterCentroidIters);
		out.println("Total number of passes = " + totalDistancePerPass.size());
		out.println("Average distance of all points from their centers " +
				"at each iteration:");
		
		for (int distanceIndex = 0; distanceIndex < totalDistancePerPass.size(); 
		distanceIndex++)
		{
			out.println(
					totalDistancePerPass.get(
							distanceIndex).doubleValue()/particleCount);
		}

		out.println();
		out.println("Peaks in centroids:");
		for (int centroidIndex = 0; centroidIndex < centroidList.size();
			centroidIndex++)
		{
			out.println("Centroid " + 
					centroidList.get(centroidIndex).subCollectionNum +
					": Number of particles = " +
					centroidList.get(centroidIndex).numMembers);
		}
		out.println();
		for (int centroidIndex = 0; 
			centroidIndex < centroidList.size();
			centroidIndex++)
		{
			out.println("Centroid " + 
					centroidList.get(centroidIndex).subCollectionNum +
			":");
			out.println("Number of particles in cluster: " + 
					centroidList.get(centroidIndex).numMembers);
			
//			out.println("Mean distance from centroid: " + centroidList.get(centroidIndex).getMeanDistance() + 
//					" Std dev: +/-" + centroidList.get(centroidIndex).getStdDevDistance());

			writeSizeMeanAndStdDev(subCollectionIDs.get(centroidList.get(centroidIndex).subCollectionNum-1), out);
			
			StringWriter centroidPeaks = new StringWriter();
			PrintWriter pr = new PrintWriter(centroidPeaks);
			writeBinnedPeakListToFile(
					centroidList.get(centroidIndex).peaks,pr);
			out.print(centroidPeaks.toString());
			
			db.setCollectionDescription(db.getCollection(
					subCollectionIDs.get(centroidList.get(centroidIndex).subCollectionNum-1)),
				centroidPeaks.toString());
		}
		//out.close();
		System.out.println(sendToDB.toString());
		db.setCollectionDescription(db.getCollection(newHostID),sendToDB.toString());
	}

	/**
	 * Creates a comma-separated string (with all string surrounded by 
	 * single quotes) from an existing string and an addition.
	 * 
	 * @param add		The string to add onto the end of params.
	 * @param params	The existing string.
	 * @return	The comma-separated string.
	 */
	public static String intersperse(String add, String params){
		
		//separate out the numbers from the real men!
		try{
			Float number = new Float(add);
			
			if (params.equals(""))
				params = add;
			else
				params = params + ", " + add;
			
		}
		//if not a number, surround in single quotes, or it's empty so it's NULL
		catch (NumberFormatException e){
			
			if (add.equals("")) {
			
				if (params.equals(""))
					params = "NULL";
				else
					params = params + ",NULL";
						
			} else {
				
				if (params.equals(""))
					params = quote + add + quote;
				else
					params = params + ", " + quote + add + quote;
			}	
		}
		
		return params;
		
	}
}
