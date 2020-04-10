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
 * The Original Code is EDAM Enchilada's CollectionDivider
 * class.
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
 * Created on Aug 19, 2004
 *
 * Ben Anderson
 */
package edu.carleton.clusteringbenchmark.analysis;

import edu.carleton.clusteringbenchmark.collection.Collection;
import edu.carleton.clusteringbenchmark.database.CollectionCursor;
import edu.carleton.clusteringbenchmark.database.InfoWarehouse;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * @author andersbe
 *
 */
public abstract class CollectionDivider {
	
	protected String comment;
	
	private StringBuilder atomIDsToDelete;

	/**
	 * The collection you are dividing
	 */
	protected Collection collection;

	/**
	 * A pointer to an active InfoWarehouse
	 */
	protected InfoWarehouse db;

	/**
	 * The id of the new collection which all subdivisions will
	 * be children of
	 */
	protected int newHostID;

	/**
	 * The current number of active subcollections
	 */
	protected int numSubCollections;

	/**
	 * an array list of the IDs of each sub collection 
	 * (each collection can be refered to by its key
	 * in the array list.)
	 */
	protected ArrayList<Integer> subCollectionIDs;

	/**
	 * The CollectionCursor used to access the atoms of the 
	 * collection you are dividing.  Initialize this to one of the
	 * implementations using a get method from InfoWarehouse
	 */
	protected CollectionCursor curs = null;

	/**
	 * Construct a CollectionDivider.
	 * @param cID		The id of the collection to be divided
	 * @param database	The open InfoWarehouse to write to
	 * @param name		A name for the new host collection
	 * @param comment	A comment for the collection
	 */
	public CollectionDivider(int collectionID, InfoWarehouse database, String name, String comment)
	{
	    if (database == null)
	        throw new IllegalArgumentException(
	                "Parameter 'database' should not be null");
	    	
		db = database;
		
		this.comment = comment;
		
		collection = db.getCollection(collectionID);
		
		subCollectionIDs = new ArrayList<Integer>();
		
		newHostID = db.createEmptyCollection(collection.getDatatype(), collection.getCollectionID(), name, comment,"");
		
		numSubCollections = 0;
	}

	/**
	 * Creates a new subcollection and returns an int by which 
	 * the subcollection can be referred to when putting particles
	 * into it.  
	 * 
	 * @return the handle by which the subcollection should be 
	 * referred to.
	 */
	protected int createSubCollection()
	{
		int subCollectionID = 0;
		numSubCollections++;
		subCollectionID = db.createEmptyCollection(collection.getDatatype(), newHostID,
				Integer.toString(numSubCollections), 
				Integer.toString(numSubCollections),"");
		assert (subCollectionID != -1) : "Error creating empty subcollection";
		subCollectionIDs.add(new Integer(subCollectionID));
		return numSubCollections;
	}

	/**
	 * Creates an emtpy collection of the same datatype in the root level for
	 * cluster centers.
	 * @param	name
	 * @param	comments
	 * @return	- new collection number
	 */
	protected int createCenterCollection(String name, String comments){
		int collID = 0;
		collID = db.createEmptyCollection(collection.getDatatype(),
										0,
										"Centers :" + collection.getName() + "," + name,
										comments,
										"");
		assert (collID != -1) : "Error creating empty center collection.";
		return collID;
	}

	/**
	* Does the bulk version of putInSubCollectionBatch
	* @author christej
	*/
	protected boolean putInSubCollectionBulk(int atomID, int target)
	{
		atomIDsToDelete.append(atomID + ",");

		try {
			db.bulkInsertAtom(atomID,
					subCollectionIDs.get(target-1).intValue());
			return true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

	}

	/**
	 * Executes putInSubCollectionBulk
	 * @author christej 
	 * @author benzaids
	*/
	 
	protected void putInSubCollectionBulkExecute()
	{
		System.out.println("About to execute INSERTs.");
		//System.out.println((new Date()).toString());
		try {
			db.bulkInsertExecute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Done with INSERTS, about to do DELETE");
		
		//build a table for deletes
		//drop the table in case it already (mistakenly) exists
		System.out.println("Creating deletion table...");
		Connection dbCon = db.getCon();
		Statement delStmt = null;
		try {
			delStmt = dbCon.createStatement();
			delStmt.executeUpdate("IF (OBJECT_ID('stuffToDelete') IS NOT NULL)\n" +
								  "DROP TABLE stuffToDelete");
			delStmt.executeUpdate("CREATE TABLE stuffToDelete(atoms int)");
		}
		catch (Exception e) {
			System.out.println("Error creating table stuffToDelete in database.");
		}
		
		//create tempfile used to bulk insert to stuffToDelete
		//tempfile goes in same place as other temp files
		System.out.println("Creating tempdelete.data file...");
//		String tempdir = "";
		File temp = null;
		PrintWriter pw = null;
		try {
			temp = File.createTempFile("tempdelete", "data");
			pw = new PrintWriter(temp);
		}
		catch (Exception e) {
			System.out.println("Error Creating tempDelete file.");
		}
		
		//put stuff in the tempfile
		System.out.println("Putting stuff in tempdelete.data...");
		String atomIDsToDel = atomIDsToDelete.toString();
		Scanner atomIDs = new Scanner(atomIDsToDel).useDelimiter(",");
		while (atomIDs.hasNext()) {
			pw.println(atomIDs.next());
		}
		pw.println();
		pw.close();
		
		//execute a statement to bulk insert into stuffToDelete
		System.out.println("Putting stuff in deletion table...");
		try {
			delStmt.executeUpdate("BULK INSERT stuffToDelete\n" + 
								  "FROM '" + temp.getAbsoluteFile() + "'");
		}
		catch (Exception e) {
			System.out.println("Error inserting into stuffToDelete.");
		}
		
		//delete the temp file
		System.out.println("Deleting tempdelete.data...");
		temp.delete();
		
		//finally, delete what's in stuffToDelete from AtomMembership
		//and drop the stuffToDelete table
		System.out.println("Finally, deleting from AtomMembership...");
		String deletionquery = "DELETE FROM AtomMembership\n" +
		  					   "WHERE CollectionID = " + collection.getCollectionID() + 
		  					   "\n" + "AND AtomID IN \n" +
		  					   "(SELECT atoms FROM stuffToDelete)";
		System.out.println("Query:");
		System.out.println(deletionquery);
		try {
			delStmt.executeUpdate(deletionquery);
			delStmt.executeUpdate("DROP TABLE stuffToDelete");
		}
		catch (Exception e) {
			System.out.println("Error deleting from AtomMembership");
		}
		System.out.println("...and dropping deletion table.");
		
		System.out.println("Done with DELETEs.");
	}


	/**
	 * This is where the actual method of dividing the collection
	 * should take place.  This is the last method that should
	 * be called of your CollectionDivider.  Should return the 
	 * id of the host collection of the subcollections.  
	 */
	abstract public int divide();
}
