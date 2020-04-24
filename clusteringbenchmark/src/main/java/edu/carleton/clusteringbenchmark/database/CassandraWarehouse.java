package edu.carleton.clusteringbenchmark.database;

import com.datastax.driver.core.*;

import edu.carleton.clusteringbenchmark.ATOFMS.ParticleInfo;
import edu.carleton.clusteringbenchmark.collection.Collection;
import edu.carleton.clusteringbenchmark.errorframework.ErrorLogger;
import org.apache.cassandra.exceptions.CassandraException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;

public class CassandraWarehouse implements InfoWarehouse {
    protected String url;

    // for batch stuff
    private java.sql.Statement batchStatement;
    private ArrayList<Integer> alteredCollections;
    private PrintWriter bulkInsertFileWriter;
    private File bulkInsertFile;

    public Session session;

    public CassandraWarehouse() {
        Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withoutJMXReporting()
                .build();
        session = cluster.connect();
    }

    @Override
    public Collection getCollection(int collectionID) {
        try {
            String id = String.valueOf(collectionID);
            ResultSet rss = session.execute("SELECT * FROM particles.collections WHERE CollectionID = \'" + id + "\'");
            if (rss != null) {
                return new Collection("ATOFMS", collectionID, this);
            }
        } catch (Exception e) {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Error retrieving collection for collectionID " + collectionID);
            System.out.println("Error");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String getCollectionName(int collectionID) {
        String id = String.valueOf(collectionID);
        ResultSet rss = session.execute("SELECT name from particles.collections WHERE CollectionID = \'" + id + "\'");
        return rss.toString();
    }


    @Override
    public int getCollectionSize(int collectionID) {
        try {
            String id = String.valueOf(collectionID);
            ResultSet rss = session.execute("SELECT * FROM particles.collections WHERE collectionID = \'" + id + "\'");
            if (rss != null) {
                //rss = session.execute("Select Count(CollectionId) from particles.collections where collectionID =\'" + id + "\'");
                rss = session.execute("Select  from particles.collections where collectionID =\'" + id + "\'");

                Number val = (Number) rss;
                return (int) val;
            }
        } catch (Exception e) {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Error retrieving the collection size for collectionID " + collectionID);
            System.out.println("Error");
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public int createEmptyCollection(String datatype, int parent, String name, String comment, String description) {
        ResultSet rss = session.execute(("SELECT MAX(CollectionId) from particles.collections"));
        Number max = (Number) rss;
        session.execute("INSERT INTO particles.collections (CollectionID , parent, Name , Comment , Description , Datatype) Values(?, ?,?,?,?,?)",
                (int) max + 1, parent, name, comment, description);
        return (int) max;
    }

    @Override
    public void bulkInsertInit() throws Exception {
        if (url.equals("localhost")) {
            if (bulkInsertFile != null && bulkInsertFile.exists()) {
                bulkInsertFile.delete();
            }
            bulkInsertFile = null;
            bulkInsertFileWriter = null;
            alteredCollections = new ArrayList<Integer>();
            try {
                bulkInsertFile = File.createTempFile("bulkfile", ".txt");
                bulkInsertFile.deleteOnExit();
                bulkInsertFileWriter = new PrintWriter(new FileWriter(bulkInsertFile));
            } catch (IOException e) {
                System.err.println("Trouble creating " + bulkInsertFile.getAbsolutePath() + "");
                e.printStackTrace();
            }
        } else {
            throw new Exception("This operation can only be done with a localhost connection");
        }
    }

    @Override
    public void bulkInsertAtom(int atomID, int parentID) throws Exception {
        if (bulkInsertFileWriter == null || bulkInsertFile == null) {
            throw new Exception("Must initialize bulk insert first!");
        }
        if (!alteredCollections.contains((parentID)))
            alteredCollections.add((parentID));

        //alteredCollections.add(parentID);
        bulkInsertFileWriter.println(parentID + "," + atomID);
    }

    @Override
    public void bulkInsertExecute() throws Exception {
        try {
            long time = System.currentTimeMillis();

            if (bulkInsertFileWriter == null || bulkInsertFile == null) {
                try {
                    throw new Exception("Must initialize bulk insert first!");
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            bulkInsertFileWriter.close();

        } catch (Exception e) {
            return;
        }

    }

    @Override
    public void bulkDelete() throws Exception {

    }


    @Override
    public CollectionCursor getAtomInfoOnlyCursor(Collection collection) {
        return new AtomInfoOnlyCursor(collection);
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

        //Note for if this fails later: CQL query using the MAX function returns resultset with rows.size=1 if data is not found. And Row has only null values.
        try {
            ResultSet rs = session.execute("SELECT MAX(AtomID) FROM AtomMembership");
            Number result = (Number) rs;
            int nextID;
            if (result != null)
                nextID = (int) result + 1;
            else
                nextID = 0;
            return nextID;
        } catch (CassandraException e) {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Exception finding the maximum atomID.");
            System.err.println("Exception finding max atom id.");
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public boolean addCenterAtom(int centerAtomID, int centerCollID) {
        return false;
    }

    @Override
    public boolean setCollectionDescription(Collection collection, String description) {
        try {
            int id = collection.getCollectionID();
            ResultSet r1 = session.execute("SELECT name FROM particles.collections WHERE CollectionID = \'" + id + "\'");

            session.execute("UPDATE particles.collections SET description = \'"
                    + "hello" + "\' WHERE CollectionID =  0  AND name = \'"+ r1+"\'");

            session.execute("UPDATE particles.collections SET description = \'"
                    + description + "\' WHERE CollectionID = \'" + id + "\'");
        } catch (Exception e){
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(),"S Exception updating collection description.");
            System.err.println("Error updating collection " +
                    "description:");
            e.printStackTrace();
            return false;
        }
        return true;
    }


    @Override
    public String dbname() {
        return "Cassandra";
    }


    /**
     * AtomInfoOnly cursor.  Returns atom info.
     */
    private class AtomInfoOnlyCursor
            implements CollectionCursor {
        protected ResultSet rs;
        protected java.sql.ResultSet partInfRS = null;
        protected java.sql.Statement stmt = null;
        Collection collection;

        public AtomInfoOnlyCursor(Collection col) {
            super();
            assert (col.getDatatype().equals("ATOFMS")) : "Wrong datatype for cursor.";
            collection = col;

            String q = "SELECT \'" + getDynamicTableName(DynamicTable.AtomInfoDense, collection.getDatatype()) + "\'.AtomID, OrigFilename, ScatDelay," +
                    " LaserPower, [Time], Size FROM \'" + getDynamicTableName(DynamicTable.AtomInfoDense, collection.getDatatype()) + "\', InternalAtomOrder WHERE" +
                    " InternalAtomOrder.CollectionID = " + collection.getCollectionID() +
                    " AND \'" + getDynamicTableName(DynamicTable.AtomInfoDense, collection.getDatatype()) + "\'.AtomID = InternalAtomOrder.AtomID";
            System.out.println(q);
            try {
                partInfRS = stmt.executeQuery(q);
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        /**
         * gets the dynamic table name according to the datatype and the table
         * type.
         *
         * @param table
         * @param datatype
         * @return table name.
         */
        public String getDynamicTableName(DynamicTable table, String datatype) {
            assert (!datatype.equals("root")) : "root isn't a datatype.";

            if (table == DynamicTable.DataSetInfo)
                return datatype + "DataSetInfo";
            if (table == DynamicTable.AtomInfoDense)
                return datatype + "AtomInfoDense";
            if (table == DynamicTable.AtomInfoSparse)
                return datatype + "AtomInfoSparse";
            else return null;
        }

        @Override
        public boolean next() {
            return false;
        }

        @Override
        public ParticleInfo getCurrent() {
            return null;
        }

        @Override
        public void close() {

        }

        @Override
        public void reset() {

        }
    }
}
