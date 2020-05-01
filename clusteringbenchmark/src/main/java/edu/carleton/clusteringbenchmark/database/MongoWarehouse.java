package edu.carleton.clusteringbenchmark.database;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

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

    MongoClient client;
    MongoDatabase db;
    File bulkInsertFile;
    PrintWriter bulkInsertFileWriter;

    public MongoWarehouse() {
        client = MongoClients.create();
        db = client.getDatabase("enchilada_benchmark");
    }

    public String dbname() {
        return "MongoDB";
    }

    public Collection getCollection(int collectionId) {
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collection = collections.find(eq("_id", collectionId)).first();
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
        String name = "";
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collection = collections.find(eq("_id", collectionId)).first();
        if (collection != null) {
            name = (String)collection.get("name");
        }
        else {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Error retrieving the collection name for collection " + collectionId);
            System.err.println("Error retrieving Collection Name.");
        }

        return name;
    }

    public int getCollectionSize(int collectionID) {
        int size = -1;
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collection = collections.find(eq("_id", collectionID)).first();
        if (collection != null) {
            String name = (String)collection.get("name");
            MongoCollection atomcollection = db.getCollection(name);
            int count = (int)atomcollection.countDocuments();
            if (count != 0) {
                size = count;
            }
        }
        else {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(),"Error retrieving the collection size for collectionID "+collectionID);
            System.err.println("Error retrieving Collection Size.");
        }
        return size;
    }

    public String getCollectionDatatype(int collectionId) {
        String datatype = "";
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collection = collections.find(eq("_id", collectionId)).first();

        if (collection != null) {
            datatype = (String)collection.get("datatype");
        }
        else {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Error retrieving the collection datatype for collection " + collectionId);
            System.err.println("Error retrieving Collection Datatype.");
        }

        return datatype;
    }

    public boolean setCollectionDescription(Collection collection, String description) {
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collectiondoc = collections.find(eq("_id", collection.getCollectionID())).first();

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
        Document collection = collections.find().sort(descending("_id")).first();

        int nextid;
        if (collection != null) {
            nextid = (int)collection.get("_id") + 1;
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
        Document atom = atoms.find().sort(descending("_id")).first();
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
        String[] denseparams = dense.split(",");
        densedoc.append("time", denseparams[0]);
        densedoc.append("laserpower", denseparams[1]);
        densedoc.append("size", denseparams[2]);
        densedoc.append("scatdelay", denseparams[3]);
        densedoc.append("specname", denseparams[4]);

        List<Document> sparsedoc = new ArrayList<>();
        for (String sparsestr : sparse) {
            String[] sparseparams = sparsestr.split(",");
            Document sparseentry = new Document();
            sparseentry.append("masstocharge", sparseparams[0]);
            sparseentry.append("area", sparseparams[1]);
            sparseentry.append("relarea", sparseparams[2]);
            sparseentry.append("height", sparseparams[3]);
            sparsedoc.add(sparseentry);
        }

        Document newcollectionentry = new Document();
        newcollectionentry.append("_id", nextID);
        newcollectionentry.append("sparsedata", sparsedoc);
        newcollectionentry.append("densedata", densedoc);

        MongoCollection<Document> insertcollection = db.getCollection(collectiondoc.getString("name"));
        insertcollection.insertOne(newcollectionentry);

        Document newatomentry = new Document();
        List cs = new ArrayList();
        Document csentry = new Document();
        csentry.append("collectionid", collection.getCollectionID());
        csentry.append("collectionname", collectiondoc.getString("name"));
        cs.add(csentry);
        newatomentry.append("_id", nextID);
        newatomentry.append("collections", cs);
        atoms.insertOne(newatomentry);

        return nextID;
    }

    public boolean addCenterAtom(int centerAtomID, int centerCollID) {
        MongoCollection<Document> atoms = db.getCollection("atoms");
        Document atom = atoms.find(eq("_id", centerAtomID)).first();
        Collection collection = getCollection(centerCollID);
        Document csentry = new Document();
        csentry.append("collectionid", collection.getCollectionID());
        csentry.append("collectionname", collection.getName());
        if (atom == null) {
            Document newatomentry = new Document();
            List cs = new ArrayList();
            cs.add(csentry);
            newatomentry.append("_id", centerAtomID);
            newatomentry.append("collections", cs);
            atoms.insertOne(newatomentry);
        }
        else {
            if (!((List)atom.get("collections")).contains(csentry)) {
                ((List)atom.get("collections")).add(csentry);
                atoms.replaceOne(eq("_id", centerAtomID), atom);
            }
        }

        return true;
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

        MongoCollection<Document> atoms = db.getCollection("atoms");

        Scanner reader = new Scanner(bulkInsertFile);
        while (reader.hasNextLine()) {
            String[] pair = reader.nextLine().split(",");
            Document atom = atoms.find(eq("_id", Integer.parseInt(pair[1]))).first();
            Collection collection = getCollection(Integer.parseInt(pair[0]));
            Document csentry = new Document();
            csentry.append("collectionid", collection.getCollectionID());
            csentry.append("collectionname", collection.getName());
            if (atom == null) {
                Document newatomentry = new Document();
                List cs = new ArrayList();
                cs.add(csentry);
                newatomentry.append("_id", Integer.parseInt(pair[1]));
                newatomentry.append("collections", cs);
                atoms.insertOne(newatomentry);
            }
            else {
                ((List)atom.get("collections")).add(csentry);
                atoms.replaceOne(eq("_id", Integer.parseInt(pair[1])), atom);
            }
        }
    }

    public void bulkDelete(StringBuilder atomIDsToDelete, Collection collection) throws Exception {
        MongoCollection<Document> atoms = db.getCollection("atoms");
        Scanner atomids = new Scanner(atomIDsToDelete.toString()).useDelimiter(",");
        while (atomids.hasNext()) {
            Document atom = atoms.find(eq("_id", atomids.next())).first();
            if (atom != null) {
                if (((List)atom.get("collections")).contains()) {

                }
            }
        }
    }

    public CollectionCursor getAtomInfoOnlyCursor(Collection collection) {

    }
}
