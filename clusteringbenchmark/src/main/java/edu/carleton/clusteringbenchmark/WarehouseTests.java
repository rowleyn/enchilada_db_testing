package edu.carleton.clusteringbenchmark;

import edu.carleton.clusteringbenchmark.database.CassandraWarehouse;
import edu.carleton.clusteringbenchmark.database.InfoWarehouse;

import java.io.IOException;


public class WarehouseTests {
    private static InfoWarehouse db;
    public static void main (String[]args) throws Exception {
            db = new CassandraWarehouse();
            bulkInsertTest();
    }
    public static int createCollectionTest() {
        return -1;
    }

    public static void bulkInsertTest() throws Exception {

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

    }

}
