package edu.carleton.clusteringbenchmark.database;

import com.datastax.driver.core.*;

import com.mongodb.client.MongoCollection;
import edu.carleton.clusteringbenchmark.ATOFMS.ParticleInfo;
import edu.carleton.clusteringbenchmark.analysis.BinnedPeakList;
import edu.carleton.clusteringbenchmark.atom.ATOFMSAtomFromDB;
import edu.carleton.clusteringbenchmark.collection.Collection;
import edu.carleton.clusteringbenchmark.errorframework.ErrorLogger;
import org.apache.cassandra.exceptions.CassandraException;
import org.bson.Document;

import javax.xml.transform.Result;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.*;

import static com.mongodb.client.model.Filters.eq;
import static java.util.Arrays.asList;

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
        Row name = rss.one();
        if(name == null){
            return "";
        }
        return name.getString(0);
    }


    @Override
    public int getCollectionSize(int collectionID) {
        try {
            ResultSet rs = session.execute("SELECT * FROM particles.collections WHERE collectionID = " + collectionID);
            if (rs != null) {
                ResultSet rss = session.execute("Select COUNT(AtomID) from particles.collections where collectionID =" + collectionID );
                Row result = rss.one();
                long val = result.getLong(0);
                ResultSet one = session.execute("Select atomid from particles.collections where atomID = "+ -1+" AND collectionId = " +collectionID);
                if(one.one() != null){
                    return (int)val -1;
                }
                return (int)val;
            }
            else{
                return -1;
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
        ResultSet rs = session.execute("SELECT * FROM particles.collections LIMIT 1");
        int nextID;
        int max;
        if(rs.one() == null){
            nextID = 0;
        }
        else {
            ResultSet rss = session.execute(("SELECT MAX(CollectionId) from particles.collections"));
            Row result = rss.one();
            max = result.getInt(0);
            nextID = max +1;
        }
        session.execute("INSERT INTO particles.collections (CollectionID , parent, Name , Comment , Description , Datatype, AtomID) Values(?,?,?,?,?,?,?)",
                nextID, parent, name, comment, description, datatype, -1);
        return nextID;
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
                String[] line = data.split(",");
                int CID = Integer.parseInt(line[0]);
                int AID = Integer.parseInt(line[1]);
                ResultSet exists = session.execute("SELECT * FROM particles.collections WHERE CollectionID = "+CID+" AND AtomID = "
                        + AID );

                if(exists.one() == null) {
                    session.execute("INSERT INTO particles.collections (CollectionID, AtomID) VALUES (?,?)", CID, AID);
                    session.execute("DELETE FROM particles.collections WHERE AtomId = "+ -1 + " AND CollectionID = "+ CID);

                }
                else{
                    System.out.println("Association already exists");
                }
            }

            System.out.println("Time: " + (System.currentTimeMillis()-time));
            System.out.println("done with updating, time = " + (System.currentTimeMillis()-time));
            return;
        } catch (Exception e) {
            System.out.println("Error executing bulk insert");
            e.printStackTrace();
            return;
        }

    }

    @Override
    public void bulkDelete(StringBuilder atomIDsToDelete, Collection collection) throws Exception {
        Scanner deleteids = new Scanner(atomIDsToDelete.toString()).useDelimiter(",");
        int id = collection.getCollectionID();
        while(deleteids.hasNext()){
            int nextid = Integer.parseInt(deleteids.next());
            session.execute("DELETE FROM particles.collections WHERE AtomID = " + nextid + " AND CollectionId = " + id);
        }

    }


    @Override
    public CollectionCursor getAtomInfoOnlyCursor(Collection collection) {
        return new CassandraCursor(collection);
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
            if (table == DynamicTable.AtomInfoSparse) {
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
            } else if (table == DynamicTable.AtomInfoDense) {
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
            //Metadata version
            /**
            Metadata metadata = cluster.getMetadata();
            List<ColumnMetadata> columns = metadata.getKeyspace("particles").getTable(dtable).getColumns();
            ArrayList<ArrayList<String>> colNames = new ArrayList<ArrayList<String>>();

            ArrayList<String> temp;
            for(int i = 0; i<columns.size(); i++) {
                temp = new ArrayList<String>();
                temp.add(columns.get(i).getName());
                temp.add(columns.get(i).getType().toString());
                colNames.add(temp);
            }*/

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

        int AtomId = nextID;
        String[] denseSplit = dense.split(", ");
        SimpleDateFormat dateFormat = new SimpleDateFormat("''yyyy-MM-dd HH:mm:ss.S''");
        try {
            Date date5 = dateFormat.parse(denseSplit[0]);
            session.execute("INSERT INTO particles.dense (time, laserpower, size, scatdelay, specname, atomID) Values (?, ?, ?, ?, ?, ?)",
                date5, denseSplit[1], denseSplit[2], denseSplit[3], denseSplit[4], AtomId);
        }
        catch (ParseException e){
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Failed to parse datestring during particle insert.");
            System.err.println("Datestring parse failed.");
            return -1;
        }
        for (String sparsestr : sparse) {
            String[] sparseparams = sparsestr.split(", ");
            String mtc = sparseparams[0];
            String area = sparseparams[1];
            String relarea = sparseparams[2];
            String height = sparseparams[3];
            session.execute("INSERT INTO particles.sparse (area, relarea, masstocharge, height, AtomID) Values (?, ?, ?, ?, ?)",
                    area, relarea, mtc, height, String.valueOf(AtomId));
        }
        session.execute("INSERT INTO particles.collections (CollectionID, AtomId) Values(?, ?)", id, AtomId);
            session.execute("DELETE FROM particles.collections WHERE AtomId = "+ -1 + " AND CollectionID = "+ id);

        return AtomId;
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
            ResultSet exists = session.execute("SELECT * FROM particles.collections WHERE collectionID = " +centerCollID);
            if(exists.one() != null) {
                Number AtomID = (Number) centerAtomID;
                Number collID = (Number) centerCollID;

                session.execute("INSERT INTO particles.centeratoms (AtomID, CollectionID) VALUES (?, ?)", AtomID.toString(), collID.toString());
                session.execute("INSERT INTO particles.collections (AtomID, CollectionID) VALUES (?, ?)", (int)AtomID, (int)collID);
                return true;
            }
            else{
                return false;
            }
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
            ResultSet atomIDs = session.execute("SELECT AtomID FROM particles.collections WHERE collectionID =" + collection.getCollectionID());
            List<Row> atomlist = new ArrayList<Row>();
            atomlist = atomIDs.all();
            for(Row atom : atomlist) {
                session.execute("UPDATE particles.collections SET description = \'"
                        + description + "\' WHERE CollectionID = " + id + " AND AtomID = "+ atom.getInt(0));
            }
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

    @Override
    public void clear() {
        session.execute("TRUNCATE particles.collections");
        session.execute("TRUNCATE particles.dense");
        session.execute("TRUNCATE particles.sparse");
        session.execute("TRUNCATE particles.centeratoms");
        session.execute("TRUNCATE pars.par");

    }


    /**
     * AtomInfoOnly cursor.  Returns atom info.
     */
    private class CassandraCursor implements CollectionCursor {
        protected ResultSet rs;
        protected ResultSet partInfRS = null;
        protected java.sql.Statement stmt = null;
        Collection collection;
        ResultSet atomIDs = null;
        Stack<Row> atoms = new Stack<>();

        public CassandraCursor(Collection col) {
            collection = col;
            int id = col.getCollectionID();
            partInfRS = session.execute("SELECT * FROM particles.collections WHERE collectionID = " + id);
            reset();
        }

        @Override
        public boolean next() {
            /**
            ResultSet one = session.execute("SELECT * FROM particles.collections LIMIT 1");
            if(one.one() == null){
                System.out.println("applesauce");
                return false;
            }*/

            try {
                if(atoms.empty()){
                    return false;
                }
                else if(atoms.size() == 1){
                    Row thing = atoms.pop();
                    int zero = thing.getInt(0);
                    if(zero == -1){
                        return false;
                    }
                    else {
                        atoms.push(thing);
                        return true;
                    }
                }
                else{
                    return true;
                }
            } catch (Exception e) {
                System.err.println("Error checking the " +
                        "bounds of " +
                        "the ResultSet.");
                e.printStackTrace();
                return false;
            }
        }

        @Override
        public ParticleInfo getCurrent() {
            try{
                int atomID = atoms.pop().getInt(0);
                ResultSet dense = session.execute("SELECT * FROM particles.dense WHERE atomID = "+ atomID);
                Row densething = dense.one();
                if(densething!=null) {
                    String specname = densething.getString(5);
                    String scatdelay = densething.getString(3);
                    String laserpower = densething.getString(2);
                    Date time = densething.getTimestamp(6);
                    SimpleDateFormat formatter5 = new SimpleDateFormat("''yyyy-MM-dd hh:mm:ss.S''");
                    String date = formatter5.format(time);
                    SimpleDateFormat dateFormat = new SimpleDateFormat("''yyyy-MM-dd HH:mm:ss.S''");
                    Date date5 = dateFormat.parse(date);
                    String size = densething.getString(4);
                    ATOFMSAtomFromDB atominfo = new ATOFMSAtomFromDB(atomID,
                            specname,
                            Integer.parseInt(scatdelay),
                            (Float.parseFloat(laserpower)),
                            date5,
                            Float.valueOf(size));
                    atominfo.setDateString(date);
                    ParticleInfo particleInfo = new ParticleInfo();
                    particleInfo.setParticleInfo(atominfo);


                    ResultSet sparseMass = session.execute("SELECT masstocharge FROM particles.sparse WHERE atomID = \'" + atomID + "\'");
                    BinnedPeakList peaks = new BinnedPeakList();
                    Row curMass = sparseMass.one();
                    while (curMass != null) {
                        String mass = curMass.getString(0);
                        ResultSet sparseArea = session.execute("SELECT area FROM particles.sparse WHERE masstocharge = \'" + mass + "\' AND atomID = \'" + atomID + "\'");
                        String area = sparseArea.one().getString(0);
                        float fmass = Float.parseFloat(mass);
                        peaks.add((int)fmass, Integer.parseInt(area));
                        curMass = sparseMass.one();
                    }
                    particleInfo.setBinnedList(peaks);
                    particleInfo.setID(particleInfo.getATOFMSParticleInfo().getAtomID());
                    return particleInfo;
                }
                else{
                    System.out.println("Empty row");
                    return null;
                }
            } catch (Exception e) {
                System.err.println("Error retrieving the " +
                        "next row");
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public void close() {
            atoms.clear();
        }

        @Override
        public void reset() {
            atomIDs = session.execute("SELECT AtomID FROM particles.collections WHERE collectionID =" + collection.getCollectionID());
            List<Row> atomlist = new ArrayList<Row>();
            atomlist = atomIDs.all();
            atoms.addAll(atomlist);
        }
    }
}
