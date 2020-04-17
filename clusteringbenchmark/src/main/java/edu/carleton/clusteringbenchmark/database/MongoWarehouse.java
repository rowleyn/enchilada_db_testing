package edu.carleton.clusteringbenchmark.database;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Filters.*;

import edu.carleton.clusteringbenchmark.errorframework.ErrorLogger;
import org.bson.Document;

import edu.carleton.clusteringbenchmark.collection.Collection;

import java.util.HashMap;
import java.util.Map;

public class MongoWarehouse implements InfoWarehouse {

    MongoClient client;
    MongoDatabase db;

    public MongoWarehouse() {
        client = MongoClients.create();
        db = client.getDatabase("enchilada_benchmark");
    }

    public Collection getCollection(int collectionID) {
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collectiondoc = collections.find().first();
        if (collectiondoc.containsKey(String.valueOf(collectionID))) {
            return new Collection("ATOFMS", collectionID, this);
        }
        else {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(), "Error retrieving collection for collectionID "+collectionID);
            System.err.println("collectionID not created yet!!");
            return null;
        }
    }

    public int getCollectionSize(int collectionID) {
        int size = -1;
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collectiondoc = collections.find().first();
        if (collectiondoc.containsKey(String.valueOf(collectionID))) {
            String collectionname = (String)collectiondoc.get(String.valueOf(collectionID));
            MongoCollection collection = db.getCollection(collectionname);
            int count = (int)collection.countDocuments();
            if (count != 0) {
                size = count;
            }
        }
        else {
            ErrorLogger.writeExceptionToLogAndPrompt(dbname(),"Error retrieving the collection size for collectionID "+collectionID);
        }
        return size;
    }

    public int createEmptyCollection(String datatype, int parent, String name, String comment, String description) {
        MongoCollection<Document> collections = db.getCollection("collections");
        Document collectiondoc = collections.find().first();

        int maxid = 0;
        for (String id: collectiondoc.keySet()) {
            if (Integer.parseInt(id) > maxid) {
                maxid = Integer.parseInt(id);
            }
        }
        int nextid = maxid + 1;

        Map collectioninfo = new HashMap();
        collectioninfo.put("name", name);
        collectioninfo.put("parent", parent);
        collectioninfo.put("comment", comment);
        collectioninfo.put("description", description);
        collectiondoc.append(String.valueOf(nextid), collectioninfo);
        collections.updateOne(new Document(), collectiondoc);

        return nextid;
    }
}
