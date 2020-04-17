package edu.carleton.clusteringbenchmark.database;

import com.datastax.driver.core.*;

import edu.carleton.clusteringbenchmark.collection.Collection;
import edu.carleton.clusteringbenchmark.errorframework.ErrorLogger;

import java.util.ArrayList;

public class CassandraWarehouse implements InfoWarehouse {
    public Session session;
    public CassandraWarehouse(){
        Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withoutJMXReporting()
                .build();
        session = cluster.connect();
    }
    @Override
    public Collection getCollection(int collectionID) {
        String id = String.valueOf(collectionID);
        ResultSet rss = session.execute("SELECT * FROM particles.collections WHERE collectionID = \'" + id+"\'";
        if(rss != null){
            return new Collection("ATOFMS", collectionID, this);
        }
        ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Error retrieving collection for collectionID "+collectionID);
        System.out.println("Error");
        return null;


    }

    @Override
    public int getCollectionSize(int collectionID) {
        String id = String.valueOf(collectionID);
        ResultSet rss = session.execute("SELECT * FROM particles.collections WHERE collectionID = \'" + id+"\'");
        if(rss != null){
            rss = session.execute("Select Count(collectionId) from particles.collections where collectionID =\'" + id+"\'");
            Number val = (Number) rss;
            return (int) val;
        }
        ErrorLogger.writeExceptionToLogAndPrompt(dbname(),"Error retrieving the collection size for collectionID "+collectionID);
        System.out.println("Error");
        return 0;
    }

    @Override
    public int createEmptyCollection(String datatype, int parent, String name, String comment, String description) {
        return 0;
    }

    @Override
    public void bulkInsertInit() throws Exception {

    }

    @Override
    public void bulkInsertAtom(int newChildID, int newHostID) throws Exception {

    }

    @Override
    public void bulkInsertExecute() throws Exception {

    }

    @Override
    public void bulkDelete() throws Exception {

    }

    @Override
    public CollectionCursor getAtomInfoOnlyCursor(Collection collection) {
        return null;
    }

    @Override
    public String getCollectionDatatype(int subCollectionNum) {
        return null;
    }

    @Override
    public ArrayList<ArrayList<String>> getColNamesAndTypes(String datatype, DynamicTable table) {
        return null;
    }

    @Override
    public int insertParticle(String dense, ArrayList<String> sparse, Collection collection, int nextID) {
        return 0;
    }

    @Override
    public int getNextID() {
        return 0;
    }

    @Override
    public boolean addCenterAtom(int centerAtomID, int centerCollID) {
        return false;
    }

    @Override
    public boolean setCollectionDescription(Collection collection, String description) {
        return false;
    }

    @Override
    public String getCollectionName(int collectionID) {
        return null;
    }

    @Override
    public String dbname() {
        return null;
    }
}
