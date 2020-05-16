package edu.carleton.clusteringbenchmark.database;

import com.datastax.driver.core.*;

import edu.carleton.clusteringbenchmark.ATOFMS.ParticleInfo;
import edu.carleton.clusteringbenchmark.collection.Collection;
import edu.carleton.clusteringbenchmark.errorframework.ErrorLogger;
import org.apache.cassandra.exceptions.CassandraException;

import javax.xml.transform.Result;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.*;

public class CassandraWarehouse implements InfoWarehouse {
    protected String url;

    // for batch stuff
    private java.sql.Statement batchStatement;
    private ArrayList<Integer> alteredCollections;
    private PrintWriter bulkInsertFileWriter;
    private File bulkInsertFile;

    public Session session;
    public Cluster cluster;

    public CassandraWarehouse() {
          cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withoutJMXReporting()
                .build();
        session = cluster.connect();
    }

    @Override
    public void clear() {

    }

    @Override
    public Collection getCollection(int collectionID) {
        try {
            //String id = String.valueOf(collectionID);
            ResultSet rss = session.execute("SELECT * FROM particles.collections WHERE CollectionID = " + collectionID );
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
        //String id = String.valueOf(collectionID);
        ResultSet rss = session.execute("SELECT name from particles.collections WHERE CollectionID = " + collectionID + "");
        return rss.toString();
    }


    @Override
    public int getCollectionSize(int collectionID) {
        try {
            //String id = String.valueOf(collectionID);
            ResultSet rss = session.execute("SELECT * FROM particles.collections WHERE collectionID = " + collectionID);
            if (rss != null) {
                //rss = session.execute("Select Count(CollectionId) from particles.collections where collectionID =\'" + id + "\'");
                rss = session.execute("Select MAX(AtomID) from particles.collections where collectionID =" + collectionID );

                Row result = rss.one();
                int val = result.getInt(0);
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
        Row result = rss.one();
        int max = result.getInt(0);
        session.execute("INSERT INTO particles.collections (CollectionID , parent, Name , Comment , Description , Datatype, AtomID) Values(?,?,?,?,?,?,?)",
                max, parent, name, comment, description, datatype, 0);
        return (int) max;
    }

    @Override
    public void bulkInsertInit() throws Exception {
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
                    e.printStackTrace();
                }
            }
            bulkInsertFileWriter.close();
            Scanner myReader = new Scanner(bulkInsertFile);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                System.out.println(data);
                String[] line = data.split(",");
                int CID = Integer.parseInt(line[0]);
                int AID = Integer.parseInt(line[1]);
                ResultSet exists = session.execute("SELECT * FROM particles.collections WHERE CollectionID = "+CID+" AND AtomID = "
                        + AID );
                if(exists.one() == null) {
                    //ResultSet rs1 = session.execute("SELECT * from particles.collections");
                    //System.out.println(rs1.all());
                    session.execute("INSERT INTO particles.collections (CollectionID, AtomID) VALUES (?,?)", CID, AID);
                    //ResultSet rs = session.execute("SELECT MAX(AtomID) from particles.collections");
                    //System.out.println(rs.all());
                }
                else{
                    System.out.println("Association already exists");
                }
            }

            System.out.println("Time: " + (System.currentTimeMillis()-time));
            System.out.println("done with updating, time = " + (System.currentTimeMillis()-time));

        } catch (Exception e) {
            System.out.println("Error executing bulk insert");
            e.printStackTrace();
            return;
        }

    }

    @Override
    public void bulkDelete(StringBuilder atomIDsToDelete, Collection collection) throws Exception {

    }



    @Override
    public CollectionCursor getAtomInfoOnlyCursor(Collection collection) {
        return new AtomInfoOnlyCursor(collection);
    }

    //assuming subCollectionNum is CollectionID?
    @Override
    public String getCollectionDatatype(int subCollectionNum) {
        ResultSet rs = session.execute("SELECT datatype from particles.collections WHERE CollectionID = " + subCollectionNum );
        return rs.toString();
    }

    @Override
    public ArrayList<ArrayList<String>> getColNamesAndTypes(String datatype, DynamicTable table) {
        try {
            String dtable = null;
            if (table == DynamicTable.AtomInfoSparse) {
                dtable = "sparse";
            } else if (table == DynamicTable.AtomInfoDense) {
                dtable = "dense";
            }
            Metadata metadata = cluster.getMetadata();
            List<ColumnMetadata> columns = metadata.getKeyspace("particles").getTable(dtable).getColumns();
            ArrayList<ArrayList<String>> colNames = new ArrayList<ArrayList<String>>();
            ArrayList<String> temp;
            for(int i = 0; i<columns.size(); i++) {
                temp = new ArrayList<String>();
                temp.add(columns.get(i).getName());
                temp.add(columns.get(i).getType().toString());
                colNames.add(temp);
            }
        } catch(Exception e)
        {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Exception retrieving column names.");
            System.err.println("Exception retrieving column names.");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int insertParticle(String dense, ArrayList<String> sparse, Collection collection, int nextID) {
        int id = collection.getCollectionID();
        ResultSet rss = session.execute("SELECT MAX(AtomID) FROM particles.collections WHERE CollectionID = "+id);
        Row result = rss.one();
        int AtomId = result.getInt(0) + 1;
        String[] denseSplit = dense.split(",");
        session.executeAsync("INSERT INTO particles.dense (name, time, laserpower, size, scatdelay, specname) Values (?, ?, ?, ?, ?, ?, ?)",
                AtomId,  denseSplit[0], denseSplit[1], denseSplit[2], denseSplit[3], denseSplit[4]);
        for(int i =0; i <sparse.size(); i++){
            String[] sparseSplit = sparse.get(i).split(",");
            session.executeAsync("INSERT INTO particles.sparse (AtomID, area, relarea, masstocharge, height) Values (?, ?, ?, ?, ?, ?)",
                    AtomId, sparseSplit[0], sparseSplit[1], sparseSplit[2], sparseSplit[3]);
        }
        session.executeAsync("INSERT INTO particles.collections (CollectionID, AtomId) Values(?, ?, ?)", id, AtomId);

        return 0;
    }

    @Override
    public int getNextID() {

        //Note for if this fails later: CQL query using the MAX function returns resultset with rows.size=1 if data is not found. And Row has only null values.
        try {
            int nextID;
            ResultSet rs = session.execute("SELECT MAX(AtomID) FROM particles.collections");
            Row result =  rs.one();
            if(result.isNull(0)){
                nextID = 0;
            }
            else {
                int test = result.getInt(0);
                //int result = rs.getAvailableWithoutFetching();
                nextID = test + 1;
            }
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
        try {
            Number AtomID = (Number) centerAtomID;
            Number collID = (Number) centerCollID;
            session.execute("INSERT INTO particles.centeratoms (AtomID, CollectionID) VALUES (?, ?)", AtomID.toString(), collID.toString());
            return true;
        } catch (Exception e){
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(),"Error adding center atom");
            System.err.println("Error adding center atom");
            e.printStackTrace();
        }
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
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(),"Exception updating collection description.");
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
    private class AtomInfoOnlyCursor implements CollectionCursor {
        protected ResultSet rs;
        protected java.sql.ResultSet partInfRS = null;
        protected java.sql.Statement stmt = null;
        Collection collection;

        public AtomInfoOnlyCursor(Collection col) {
            super();
            assert (col.getDatatype().equals("ATOFMS")) : "Wrong datatype for cursor.";
            collection = col;

            ResultSet q = session.execute("SELECT \'" + getDynamicTableName(DynamicTable.AtomInfoDense, collection.getDatatype()) + "\'.AtomID, OrigFilename, ScatDelay," +
                    " LaserPower, [Time], Size FROM \'" + getDynamicTableName(DynamicTable.AtomInfoDense, collection.getDatatype()) + "\', InternalAtomOrder WHERE" +
                    " InternalAtomOrder.CollectionID = " + collection.getCollectionID() +
                    " AND \'" + getDynamicTableName(DynamicTable.AtomInfoDense, collection.getDatatype()) + "\'.AtomID = InternalAtomOrder.AtomID");
            System.out.println(q.toString());
            try {
                partInfRS = stmt.executeQuery(q.toString());
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
