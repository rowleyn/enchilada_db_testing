package edu.carleton.clusteringbenchmark.database;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Projections;
import edu.carleton.clusteringbenchmark.errorframework.ErrorLogger;
import edu.carleton.clusteringbenchmark.collection.Collection;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBResultMapper;
import org.joda.time.Instant;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class InfluxWarehouse implements InfoWarehouse{


    private File bulkInsertFile;
    private  PrintWriter bulkInsertFileWriter;
    private String dbName = "enchilada_benchmark";
    private InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "influxdbUser", "influxdbPsw" );

    private String rpName = "aRetentionPolicy";

    public InfluxWarehouse(){
        influxDB.setDatabase("enchilada_benchmark");
        influxDB.query(new Query("CREATE RETENTION POLICY " + rpName + " ON " + dbName + " DURATION 30h REPLICATION 2 SHARD DURATION 30m DEFAULT"));
        influxDB.setRetentionPolicy(rpName);
        influxDB.enableBatch(BatchOptions.DEFAULTS);
    }

    public void clear(){
        System.out.println("Clearing dense, sparse, and collections");
        influxDB.query(new Query("DROP MEASUREMENT dense", dbName));
        influxDB.query(new Query("DROP MEASUREMENT sparse", dbName));
        influxDB.query(new Query("DROP MEASUREMENT collections", dbName));
    }

    public Collection getCollection(int collectionID) {
        try {
            QueryResult queryResult = influxDB.query(new Query("SELECT colName FROM collections WHERE collectionID ='" + collectionID + "'", dbName));
            InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
            List<Collections> memoryPointList = resultMapper.toPOJO(queryResult, Collections.class);

            if (!memoryPointList.isEmpty()){
                return new Collection("ATOFMS", collectionID, this);
            }
            else {
                System.out.println("Collection not created yet!!!!!");
                return null;
            }
        }
        catch (InfluxDBException e){
            System.out.println("Something went wrong");
            return null;
        }
    }
    public String getCollectionName(int collectionID){
        try {
            QueryResult queryResult = influxDB.query(new Query("SELECT colName FROM collections WHERE collectionID ='" + collectionID + "'", dbName));
            InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
            List<Collections> result = resultMapper.toPOJO(queryResult, Collections.class);

            if (!result.isEmpty()){
               return result.get(0).getColName();
            }
            else {
                System.out.println("Collection not created yet!!");
                return "hardCoded";
            }
        }
        catch (InfluxDBException e){
        System.out.println("Something went wrong");

        }
        return null;
    }

    public int getCollectionSize(int collectionID){
        try{
            QueryResult queryResult = influxDB.query(new Query("SELECT COUNT(atomID) FROM internalAtomOrder where collectionID ='" + collectionID + "' AND deleted != 1", dbName));
            InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
            List<InternalAtomOrder> result = resultMapper.toPOJO(queryResult, InternalAtomOrder.class);

            if (!result.isEmpty()) {
                return result.get(0).getCount();
            }
        }
        catch (InfluxDBException e){
            System.out.println(e);
            System.out.println("Something went wrong");
        }
        return -1;
    }

    public int createEmptyCollection( String datatype, int parent, String name, String comment, String description){
        QueryResult queryResult = influxDB.query(new Query("SELECT MAX(collectionIDInt) FROM collections", dbName));
        InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
        List<Collections> result = resultMapper.toPOJO(queryResult, Collections.class);
        Integer ID = 0;
        try {
            if (!result.isEmpty()) {
                ID = result.get(0).getMax() + 1;
            }
            influxDB.write(Point.measurement("collections")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .tag("colName", name)
                    .tag("collectionID", ID.toString())
                    .addField("collectionIDInt" , ID)
                    .addField("datatype", datatype)
                    .addField("parent", parent)
                    .addField("comment", comment)
                    .addField("description", description)
                    .build());
            influxDB.close();
        }
        catch (InfluxDBException e){
            System.out.println(e);
            System.out.println("Something went wrong");
        }
        return ID;
    }

    public void bulkInsertInit() throws Exception {
        if (bulkInsertFile != null && bulkInsertFile.exists()) {
            bulkInsertFile.delete();
        }
        bulkInsertFile = null;
        bulkInsertFileWriter = null;
        try {
            bulkInsertFile = File.createTempFile("bulkfile", ".txt");
            bulkInsertFile.deleteOnExit();
            bulkInsertFileWriter = new PrintWriter(new FileWriter(bulkInsertFile));
        } catch (IOException e) {
            System.err.println("Trouble creating " + bulkInsertFile.getAbsolutePath() + "");
            e.printStackTrace();
        }
    }

    public void bulkInsertAtom(int newChildID, int newHostID) throws Exception {
        if(bulkInsertFileWriter==null || bulkInsertFile==null){
            throw new Exception("Must initialize bulk insert first!");
        }
        bulkInsertFileWriter.println(newHostID+","+newChildID);
    }

    public void bulkInsertExecute() throws Exception{
        if(bulkInsertFileWriter==null || bulkInsertFile == null){
            try {
                throw new Exception("Must initialize bulk insert first!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        bulkInsertFileWriter.close();
        Scanner reader = new Scanner(bulkInsertFile);
        while (reader.hasNextLine()) {
            String[] pair = reader.nextLine().split(",");
            QueryResult queryResult = influxDB.query(new Query("SELECT * FROM internalAtomOrder WHERE collectionID =" +
                    Integer.toString(Integer.parseInt(pair[0])) + " AND atomID =" +
                    Integer.toString(Integer.parseInt(pair[1])) + " AND deleted != 1", dbName));
            InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
            List<Collections> result = resultMapper.toPOJO(queryResult, Collections.class);
            if (! result.isEmpty()) { //Atom was not found
                influxDB.write(Point.measurement("internalAtomOrder")
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .tag("id", Integer.toString(Integer.parseInt(pair[0])))
                        .addField("atomID" , Integer.parseInt(pair[1]))
                        .addField("deleted" , 0)
                        .build());
            }
        }
    }

    public void bulkDelete(StringBuilder atomIDsToDelete, edu.carleton.clusteringbenchmark.collection.Collection collection) throws Exception{
        try {
            QueryResult queryResult = influxDB.query(new Query("SELECT * FROM collections WHERE collectionID ='" + collection.getCollectionID() + "'", dbName));
            InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
            List<Collections> result = resultMapper.toPOJO(queryResult, Collections.class);
            if (!result.isEmpty()) {
                Scanner deleteids = new Scanner(atomIDsToDelete.toString()).useDelimiter(",");
                while (deleteids.hasNext()) {
                    int nextid = Integer.parseInt(deleteids.next());
                    QueryResult atomQueryResult = influxDB.query(new Query("SELECT atomID FROM internalAtomOrder WHERE collectionID ='" + collection.getCollectionID() + "'" + " AND atomID =" + nextid, dbName));
                    InfluxDBResultMapper atomMapper = new InfluxDBResultMapper();
                    List<InternalAtomOrder> atoms = atomMapper.toPOJO(atomQueryResult, InternalAtomOrder.class);
                    Instant time = Instant.parse(atoms.get(0).getTime());
                    influxDB.write(Point.measurement("internalAtomOrder")
                            .time(time.getMillis(), TimeUnit.MILLISECONDS)
                            .tag("collectionID", Integer.toString(collection.getCollectionID()))
                            .addField("atomID", nextid)
                            .addField("deleted", 1)
                            .build());
                }
            }
        }
        catch (Exception e){
            System.out.println("problem in bulk delete");
            System.out.println(e);
        }
    }

    public CollectionCursor getAtomInfoOnlyCursor(edu.carleton.clusteringbenchmark.collection.Collection collection){
        try {
            return new InfluxCursor(collection);
        }
        catch (Exception e){
            System.out.println("Problem in getAtomInfoOnlyCursor");
        }
        return new InfluxCursor(collection);
    }

    public String getCollectionDatatype(int collectionID){
        try {
            QueryResult queryResult = influxDB.query(new Query("SELECT datatype FROM collections WHERE collectionID ='" + collectionID + "'" , dbName));
            InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
            List<Collections> result = resultMapper.toPOJO(queryResult, Collections.class);

            if (!result.isEmpty()){
                return result.get(0).getDatatype();
            }
            else {
                System.out.println("Collection not created yet!");
                return "ATOFMS";
            }
        }
        catch (InfluxDBException e){
            System.out.println("Something went wrong");
            System.out.println(e);

        }
        return null;
    }

    public ArrayList<ArrayList<String>> getColNamesAndTypes(String datatype, DynamicTable table) {
        try {
            if (table.ordinal() == 1) {
                String[] temp1 = {"masstocharge", "REAL", "area", "INT", "relarea", "REAL", "height", "INT"};
                ArrayList<ArrayList<String>> result = new ArrayList<>();
                ArrayList<String> temp2;
                for (int i = 0; i < temp1.length; i = i + 2) {
                    temp2 = new ArrayList<>();
                    temp2.add(temp1[i]);
                    temp2.add(temp1[i + 1]);
                    result.add(temp2);
                }
                return result;
            } else if (table.ordinal() == 2) {
                String[] temp1 = {"time", "DATETIME", "laserpower", "REAL", "size", "REAL", "scatdelay", "INT", "specname", "VARCHAR(8000)"};
                ArrayList<ArrayList<String>> result = new ArrayList<>();
                ArrayList<String> temp2;
                for (int i = 0; i < temp1.length; i = i + 2) {
                    temp2 = new ArrayList<>();
                    temp2.add(temp1[i]);
                    temp2.add(temp1[i + 1]);
                    result.add(temp2);
                }
                return result;
            }
        } catch(Exception e)
        {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Exception retrieving column names.");
            System.err.println("Exception retrieving column names.");
            e.printStackTrace();
        }
        return null;
    }

    public int insertParticle(String dense, ArrayList<String> sparse, edu.carleton.clusteringbenchmark.collection.Collection collection, int nextID){
        try {
            QueryResult queryResult = influxDB.query(new Query("SELECT * FROM collections WHERE collectionID ='" + collection.getCollectionID() + "'", dbName));
            InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
            List<Collections> result = resultMapper.toPOJO(queryResult, Collections.class);
            if (result.isEmpty()) {
                ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Error retrieving collection for collectionID " + collection.getCollectionID());
                System.err.println("collectionID not created yet!");
                return -1;
            } else {
                int atomId = nextID;
                String[] denseparams = dense.split(", ");
                influxDB.write(Point.measurement("dense")
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .tag("specname", denseparams[4])
//                        .tag("par_id", denseparams[])
                        .addField("_id", atomId)
                        .addField("scatdelay", denseparams[3])
                        .addField("size", denseparams[2])
                        .addField("laserpower", denseparams[1])
                        .addField("Date", denseparams[0])
                        .build());
                for (String sparsestr : sparse) {
                    String[] sparseparams = sparsestr.split(", ");
                    influxDB.write(Point.measurement("sparse")
                            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                            .tag("specname", denseparams[4])
                            .addField("_id", atomId)
                            .addField("Area", Integer.parseInt(sparseparams[1]))
                            .addField("relarea", Float.parseFloat(sparseparams[2]))
                            .addField("masstocharge", Double.parseDouble(sparseparams[0]))
                            .addField("height", Integer.parseInt(sparseparams[3]))
                            .build());
                }
                influxDB.write(Point.measurement("internalAtomOrder")
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .tag("collectionID", Integer.toString(collection.getCollectionID()))
                        .addField("atomID", atomId)
                        .addField("deleted", 0)
                        .build());
            }
            return nextID;
        }
        catch (Exception e){
            System.out.println("problem inserting particle");
            System.out.println(e);
        }
        return -1;
    }

    public int getNextID(){
        try {
            QueryResult queryResult = influxDB.query(new Query("SELECT MAX(_id) FROM dense", dbName));
            InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
            List<Dense> result = resultMapper.toPOJO(queryResult, Dense.class);
            Integer ID = -1;
            if (!result.isEmpty()) {
                ID = result.get(0).getMax();
            }
            return ID;
        }
        catch (InfluxDBException e){
            System.out.println("Something went wrong");
            System.out.println(e);
            return -1;
        }
    }

    public boolean addCenterAtom(int centerAtomID, int centerCollID){
        try {
            QueryResult queryResult = influxDB.query(new Query("SELECT * FROM collections WHERE collectionID ='" + centerCollID + "'", dbName));
            InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
            List<Collections> result = resultMapper.toPOJO(queryResult, Collections.class);
            if (!result.isEmpty()) {

                QueryResult atom = influxDB.query(new Query("SELECT * FROM internalAtomOrder WHERE collectionID ='" + centerCollID + "'" + "AND atomID =" + centerAtomID + " AND deleted != 1", dbName));
                InfluxDBResultMapper atomMapper = new InfluxDBResultMapper();
                List<InternalAtomOrder> atomResult = atomMapper.toPOJO(atom, InternalAtomOrder.class);
                if (atomResult.isEmpty()) {
                    influxDB.write(Point.measurement("internalAtomOrder")
                            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                            .tag("collectionID", Integer.toString(centerCollID))
                            .addField("atomID", centerAtomID)
                            .addField("deleted", 0)
                            .build());
                }
                return true;
            }
        }
        catch (Exception e){
            System.out.println("problem in addCenterAtom");
            System.out.println(e);
        }
        return false;
    }

    public boolean setCollectionDescription(edu.carleton.clusteringbenchmark.collection.Collection collection, String description){ //Not done
        QueryResult queryResult = influxDB.query(new Query("SELECT time, description FROM collections WHERE collectionID ='" + collection.getCollectionID() + "'", dbName));
        InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
        List<Collections> result = resultMapper.toPOJO(queryResult, Collections.class);
        if (!result.isEmpty()) {
            String time = result.get(0).getTime();
            influxDB.write(Point.measurement("collection")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .tag("collectionID", Integer.toString(collection.getCollectionID()))
                    .addField("datatype", "ATOFMS")
                    .addField("description" , description)
                    .build());
            return true;
        }
        else {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Error updating the collection description for collection " + collection.getCollectionID());
            System.err.println("Error updating Collection Description.");
            return false;
        }
    }

    public String dbname(){
        return "InfluxDB";
    }
}
