
package edu.carleton.clusteringbenchmark.database;

import edu.carleton.clusteringbenchmark.collection.Collection;

import java.sql.Connection;
import java.util.ArrayList;

public interface InfoWarehouse {

    public Collection getCollection(int collectionID); // simple, gets a collection (dataset) from db with associated metadata

    public int getCollectionSize(int collectionID); //simple, gets the count of atoms in DB using SQL command

    public int createEmptyCollection( String datatype,
                                      int parent,
                                      String name,
                                      String comment,
                                      String description); // simple, creates a new empty collection in the db

    public void bulkInsertInit() throws Exception; // initalizes a bulk insert using temporary files

    public void bulkInsertAtom(int newChildID, int newHostID) throws Exception; // writes an atom to bulk-insert to the temp file

    public void bulkInsertExecute() throws Exception; // executes a bulk insert, reading from temp files

    public void bulkDelete() throws Exception; // not from enchilada, this is meant to replace the putInSubCollectionBulkExecute method in CollectionDivider so that it doesn't contain SQL

    public CollectionCursor getAtomInfoOnlyCursor(Collection collection); // simple, just returns a new AtomInfoOnlyCursor object on collection

    public String getCollectionDatatype(int subCollectionNum); // simple, just gets the datatype (ATOFMS, AMs, etc.) of a collection

    public ArrayList<ArrayList<String>> getColNamesAndTypes(String datatype, DynamicTable table); // gets the names of columns for a given datatype, this is metadata

    public int insertParticle(String dense, ArrayList<String> sparse,Collection collection, int nextID); // inserts a particle's sparse (peaks) and dense info into the db

    public int getNextID(); // gets the max id of any particle, this is the next id I guess?

    public boolean addCenterAtom(int centerAtomID, int centerCollID); // simple, adds a particle to the Centeratoms table

    public boolean setCollectionDescription(Collection collection,
                                            String description); // simple, updates a collection's description

    public String getCollectionName(int collectionID);

    public String dbname(); // returns a string naming the database system implementing this interface
}
