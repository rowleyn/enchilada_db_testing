
package edu.carleton.clusteringbenchmark;

import java.sql.Connection;
import java.util.ArrayList;

public interface InfoWarehouse {

    public Collection getCollection(int collectionID);

    public int createEmptyCollection( String datatype,
                                      int parent,
                                      String name,
                                      String comment,
                                      String description);

    public void atomBatchInit();

    public boolean addAtomBatch(int atomID, int parentID);

    public void bulkInsertAtom(int newChildID, int newHostID) throws Exception;

    public void atomBatchExecute();

    public boolean deleteAtomsBatch(String atomIDs, Collection collection);

    public boolean deleteAtomBatch(int atomID, Collection collection);

    public void bulkInsertExecute() throws Exception;

    public Connection getCon(); // will need to change as this is SQL-specific

    public CollectionCursor getAtomInfoOnlyCursor(Collection collection);

    public void bulkInsertInit() throws Exception;

    public String getCollectionDatatype(int subCollectionNum);

    public ArrayList<ArrayList<String>> getColNamesAndTypes(String datatype, DynamicTable table);

    public int insertParticle(String dense, ArrayList<String> sparse,Collection collection, int nextID);

    public int getNextID();

    public boolean addCenterAtom(int centerAtomID, int centerCollID);

    public boolean setCollectionDescription(Collection collection,
                                            String description);


}
