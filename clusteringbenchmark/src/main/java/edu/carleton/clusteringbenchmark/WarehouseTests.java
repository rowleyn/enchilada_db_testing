package edu.carleton.clusteringbenchmark;

import edu.carleton.clusteringbenchmark.database.CassandraWarehouse;
import edu.carleton.clusteringbenchmark.database.InfoWarehouse;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


public class WarehouseTests {
    private static InfoWarehouse db;
    public static void main (String[]args) throws Exception {
            db = new CassandraWarehouse();
            createCollectionTest();
            bulkInsertTest();

    }
    public static void createCollectionTest() {
        String dataType = db.getCollectionDatatype(0);
        int collectionid = db.createEmptyCollection(dataType, 0, "name", "comment", "description");
        System.out.println(db.getCollectionName(collectionid));
        System.out.println(db.getCollection(collectionid));
    }

    public static void bulkInsertTest() throws Exception {
        long start = System.currentTimeMillis();

        try {
            db.bulkInsertInit();
        } catch (IOException  e){
            System.err.println("Error in bulk insert init");
            e.printStackTrace();
        }
        //Need to get AtomID and ParentID
        int atomID = db.getNextID();
        db.bulkInsertAtom(atomID, atomID +1 );
        db.bulkInsertExecute();
        System.out.println("Success");

        long end = System.currentTimeMillis();
        System.out.print("Time for Bulk Insert");
        long timeelapsed = end - start;
        System.out.println(" time elapsed" + timeelapsed + " milliseconds");

    }

}
