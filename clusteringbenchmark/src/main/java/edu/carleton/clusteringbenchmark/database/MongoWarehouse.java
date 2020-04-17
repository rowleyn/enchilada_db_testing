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

import java.util.ArrayList;
import java.util.Map;

public class MongoWarehouse implements InfoWarehouse {

    MongoClient client;
    MongoDatabase db;

    public MongoWarehouse() {
        client = MongoClients.create();
        db = client.getDatabase("enchilada_benchmark");
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
        }
        else {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Error updating the collection description for collection " + collection.getCollectionID());
            System.err.println("Error updating Collection Description.");
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
}
