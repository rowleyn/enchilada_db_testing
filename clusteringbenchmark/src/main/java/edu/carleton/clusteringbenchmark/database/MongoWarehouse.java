package edu.carleton.clusteringbenchmark.database;

import com.mongodb.client.*;

import com.mongodb.client.model.Projections;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.*;

import edu.carleton.clusteringbenchmark.errorframework.ErrorLogger;
import org.bson.Document;

import edu.carleton.clusteringbenchmark.collection.Collection;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class MongoWarehouse implements InfoWarehouse {

    private MongoDatabase db;
    private File bulkInsertFile;
    private PrintWriter bulkInsertFileWriter;

    public MongoWarehouse() {
        MongoClient client = MongoClients.create();
        db = client.getDatabase("enchilada_benchmark");
    }

    public void clear() {
        MongoCollection<Document> collections = db.getCollection("collections");
        MongoCollection<Document> atoms = db.getCollection("atoms");
        collections.drop();
        atoms.drop();
    }

    public String dbname() {
        return "MongoDB";
    }

    public Collection getCollection(int collectionId) {
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collection = collections.find(eq("_id", collectionId)).projection(include("_id")).first();
        if (collection != null) {
            return new Collection("ATOFMS", collectionId, this);
        }
        else {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Error retrieving collection for collectionID "+collectionId);
            System.err.println("collectionID not created yet!!");
            return null;
        }
    }

    public String getCollectionName(int collectionId) {
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collection = collections.find(eq("_id", collectionId)).projection(include("name")).first();
        if (collection != null) {
            return collection.getString("name");
        }
        else {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Error retrieving the collection name for collection " + collectionId);
            System.err.println("Error retrieving Collection Name.");
        }

        return "";
    }

    public int getCollectionSize(int collectionID) {
        MongoCollection<Document> collections = db.getCollection("collections");
        MongoCollection<Document> atoms = db.getCollection("atoms");
        Document collection = collections.find(eq("_id", collectionID)).projection(include("atomids")).first();
        if (collection != null) {
            return (int)atoms.countDocuments(in("_id", collection.get("atomids")));
        }
        else {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(),"Error retrieving the collection size for collectionID "+collectionID);
            System.err.println("Error retrieving Collection Size.");
        }
        return -1;
    }

    public String getCollectionDatatype(int collectionId) {
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collection = collections.find(eq("_id", collectionId)).projection(include("datatype")).first();

        if (collection != null) {
            return collection.getString("datatype");
        }
        else {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Error retrieving the collection datatype for collection " + collectionId);
            System.err.println("Error retrieving Collection Datatype.");
        }

        return "";
    }

    public boolean setCollectionDescription(Collection collection, String description) {
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collectiondoc = collections.find(eq("_id", collection.getCollectionID())).projection(include("description")).first();

        if (collectiondoc != null) {
            collectiondoc.put("description", description);
            collections.replaceOne(eq("_id", collection.getCollectionID()), collectiondoc);
            return true;
        }
        else {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Error updating the collection description for collection " + collection.getCollectionID());
            System.err.println("Error updating Collection Description.");
            return false;
        }
    }

    public int createEmptyCollection(String datatype, int parent, String name, String comment, String description) {
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collection = collections.find().projection(include("_id")).sort(descending("_id")).first();

        int nextid;
        if (collection != null) {
            nextid = collection.getInteger("_id") + 1;
        }
        else {
            nextid = 0;
        }

        Document collectioninfo = new Document();
        collectioninfo.put("_id", nextid);
        collectioninfo.put("name", name);
        collectioninfo.put("datatype", datatype);
        collectioninfo.put("parent", parent);
        collectioninfo.put("comment", comment);
        collectioninfo.put("description", description);
        collectioninfo.put("atomids", new ArrayList<>());
        collections.insertOne(collectioninfo);

        return nextid;
    }

    public ArrayList<ArrayList<String>> getColNamesAndTypes(String datatype, DynamicTable table) {
        ArrayList<ArrayList<String>> colnamesandtypes = new ArrayList<>();
        ArrayList<String> nameandtype;

        String collection = "";
        if (table.ordinal() == 1) {
            collection = "dense";
        }
        if (table.ordinal() == 2) {
            collection = "sparse";
        }

        MongoCollection<Document> metadata = db.getCollection("metadata");
        Document mdatadoc = metadata.find().projection(Projections.elemMatch("ATOFMS")).first();
        Map mdatacollection = ((Map)((Map)mdatadoc.get("datatype")).get(collection));

        for (Object colname : mdatacollection.keySet()) {
            nameandtype = new ArrayList<>();
            nameandtype.add((String)colname);
            nameandtype.add((String)mdatacollection.get(colname));
            colnamesandtypes.add(nameandtype);
        }

        return colnamesandtypes;
    }

    public int getNextID() {
        MongoCollection<Document> atoms = db.getCollection("atoms");
        Document atom = atoms.find().projection(include("_id")).sort(descending("_id")).first();
        if (atom != null) {
            return atom.getInteger("_id") + 1;
        }
        else {
            return 0;
        }
    }

    public int insertParticle(String dense, ArrayList<String> sparse, Collection collection, int nextID) {
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collectiondoc = collections.find(eq("_id", collection.getCollectionID())).first();
        if (collectiondoc == null) {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Error retrieving collection for collectionID " + collection.getCollectionID());
            System.err.println("collectionID not created yet!");
            return -1;
        }

        MongoCollection<Document> atoms = db.getCollection("atoms");

        Document densedoc = new Document();
        String[] denseparams = dense.split(", ");
        densedoc.append("time", denseparams[0]);
        densedoc.append("laserpower", denseparams[1]);
        densedoc.append("size", denseparams[2]);
        densedoc.append("scatdelay", denseparams[3]);
        densedoc.append("specname", denseparams[4]);

        List<Document> sparsedoc = new ArrayList<>();
        for (String sparsestr : sparse) {
            String[] sparseparams = sparsestr.split(", ");
            Document sparseentry = new Document();
            sparseentry.append("masstocharge", sparseparams[0]);
            sparseentry.append("area", sparseparams[1]);
            sparseentry.append("relarea", sparseparams[2]);
            sparseentry.append("height", sparseparams[3]);
            sparsedoc.add(sparseentry);
        }

        Document newatomentry = new Document();
        newatomentry.append("_id", nextID);
        newatomentry.append("sparsedata", sparsedoc);
        newatomentry.append("densedata", densedoc);

        atoms.insertOne(newatomentry);

        List<Integer> atomids = collectiondoc.getList("atomids", Integer.class);
        atomids.add(nextID);
        collectiondoc.put("atomids", atomids);
        collections.replaceOne(eq("_id", collection.getCollectionID()), collectiondoc);

        return nextID;
    }

    public boolean addCenterAtom(int centerAtomID, int centerCollID) {
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collection = collections.find(eq("_id", centerCollID)).first();

        if (collection != null) {
            List<Integer> atomids = collection.getList("atomids", Integer.class);
            if (!atomids.contains(centerAtomID)) {
                atomids.add(centerAtomID);
                collection.put("atomids", atomids);
                collections.replaceOne(eq("_id", centerCollID), collection);
            }
            return true;
        }
        return false;
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

    public void bulkInsertExecute() throws Exception {
        if(bulkInsertFileWriter==null || bulkInsertFile == null){
            try {
                throw new Exception("Must initialize bulk insert first!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        bulkInsertFileWriter.close();

        MongoCollection<Document> collections = db.getCollection("collections");

        Scanner bulkinsert = new Scanner(bulkInsertFile);
        while (bulkinsert.hasNextLine()) {
            String[] pair = bulkinsert.nextLine().split(",");
            Document collection = collections.find(eq("_id", Integer.parseInt(pair[0]))).first();
            if (collection != null) {
                List<Integer> atomids = collection.getList("atomids", Integer.class);
                if (!atomids.contains(Integer.parseInt(pair[1]))) {
                    atomids.add(Integer.parseInt(pair[1]));
                    collection.put("atomids", atomids);
                    collections.replaceOne(eq("_id", Integer.parseInt(pair[0])), collection);
                }
            }
        }
    }

    public void bulkDelete(StringBuilder atomIDsToDelete, Collection collection) throws Exception {
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collectiondoc = collections.find(eq("_id", collection.getCollectionID())).first();
        if (collectiondoc != null) {
            List<Integer> atomids = collectiondoc.getList("atomids", Integer.class);
            Scanner deleteids = new Scanner(atomIDsToDelete.toString()).useDelimiter(",");
            while (deleteids.hasNext()) {
                int nextid = Integer.parseInt(deleteids.next());
                atomids.remove(nextid);
            }
            collectiondoc.put("atomids", atomids);
            collections.replaceOne(eq("_id", collection.getCollectionID()), collectiondoc);
        }
    }

    public CollectionCursor getAtomInfoOnlyCursor(Collection collection) {
        return new MongoCursor(collection);
    }
}
