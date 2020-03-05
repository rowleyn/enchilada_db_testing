
package edu.carleton.clusteringbenchmark;

import java.sql.Connection;
import java.util.ArrayList;

public interface InfoWarehouse {

    public Collection getCollection(int collectionID); // simple, gets a collection (dataset) from db with associated metadata

    public int createEmptyCollection( String datatype,
                                      int parent,
                                      String name,
                                      String comment,
                                      String description); // simple, creates a new empty collection in the db

    public void atomBatchInit(); // sets up a SQL Statement object for batch add/move

    public boolean addAtomBatch(int atomID, int parentID); // adds an atom creation to the batch Statement

    public boolean deleteAtomBatch(int atomID, Collection collection); // adds an atom deletion to the batch Statement

    public boolean deleteAtomsBatch(String atomIDs, Collection collection); // adds multiple atom deletions from the batch Statement

    public void atomBatchExecute(); // executes the batch Statement

    public void bulkInsertInit() throws Exception; // initalizes a bulk insert using temporary files

    public void bulkInsertAtom(int newChildID, int newHostID) throws Exception; // writes an atom to bulk-insert to the temp file

    public void bulkInsertExecute() throws Exception; // executes a bulk insert, reading from temp files

    public Connection getCon(); // gets a connection to the SQL db, will need to change as this is SQL-specific

    public CollectionCursor getAtomInfoOnlyCursor(Collection collection); // simple, just returns a new AtomInfoOnlyCursor object on collection

    public String getCollectionDatatype(int subCollectionNum); // simple, just gets the datatype (ATOFMS, AMs, etc.) of a collection

    public ArrayList<ArrayList<String>> getColNamesAndTypes(String datatype, DynamicTable table); // gets the names of columns for a given datatype, this is metadata

    public int insertParticle(String dense, ArrayList<String> sparse,Collection collection, int nextID); // inserts a particle's sparse (peaks) and dense info into the db

    public int getNextID(); // gets the max id of any particle, this is the next id I guess?

    public boolean addCenterAtom(int centerAtomID, int centerCollID); // simple, adds a particle to the Centeratoms table

    public boolean setCollectionDescription(Collection collection,
                                            String description); // simple, updates a collection's description

    public void updateInternalAtomOrder(Collection collection); // this is some kind of relational bookkeeping method, not sure exactly what it's doing

    public void updateAncestors(Collection collection); // same kind of thing, relational bookkeeping. Again not sure exactly what it does
}
