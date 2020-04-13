
package edu.carleton.dataloadbenchmark;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;

import java.util.*;

public class MongoDBBenchmark implements DatabaseLoad {

    public void clear() {
        MongoClient mongoClient = MongoClients.create();
        MongoDatabase database = mongoClient.getDatabase("enchilada_benchmark");
        MongoCollection<Document> collections = database.getCollection("collections");
        MongoCollection<Document> pars = database.getCollection("pars");

        Document par = pars.find().first();
        MongoCollection<Document> particles = database.getCollection(par.get("datasetname") + "_particles");
        MongoCollection<Document> clusters = database.getCollection(par.get("datasetname") + "_clusters");

        collections.drop();
        pars.drop();
        particles.drop();
        clusters.drop();
    }

    public boolean insert(DataRead reader) {
        // format data and insert into db
        // return true if successful and false if not

        MongoClient mongoClient = MongoClients.create();
        MongoDatabase database = mongoClient.getDatabase("enchilada_benchmark");
        MongoCollection<Document> pars = database.getCollection("pars");

        Map par = new HashMap();

        par.put("_id", 0);
        par.put("datasetname", reader.par.get("dbdatasetname"));
        par.put("starttime", reader.par.get("starttime"));
        par.put("startdate", reader.par.get("startdate"));
        par.put("inlettype", reader.par.get("inlettype"));
        par.put("comment", reader.par.get("comment"));

        Document pardoc = new Document(par);

        try {
            pars.insertOne(pardoc);
        }
        catch (MongoException e) {
            System.out.println("Something went wrong...");
            System.out.println("System: " + name());
            System.out.println(e);

            return false;
        }

        String particleCollectionName = reader.par.get("dbdatasetname") + "_particles";
        MongoCollection<Document> particles = database.getCollection(particleCollectionName);

        boolean moretoread = true;
        int setindex = 0;

        while (moretoread) {
            List data = reader.readNSpectraFrom(1, setindex);
            setindex = (int)data.get(1);
            Document spectrum;

            Map spectrumdata = new HashMap();
            spectrumdata.put("_id", ((Map)data.get(0)).get("name"));
            spectrumdata.put("sparsedata", ((Map)data.get(0)).get("sparse"));
            spectrumdata.put("densedata", ((Map)data.get(0)).get("dense"));
            spectrum = new Document(spectrumdata);

            try {
                particles.insertOne(spectrum);
            }
            catch (MongoException e) {
                System.out.println("Something went wrong...");
                System.out.println("System: " + name());
                System.out.println(e);

                return false;
            }

            if (setindex >= reader.set.size()) {
                moretoread = false;
            }
        }

        MongoCollection<Document> collections = database.getCollection("collections");
        Document collectionlist = new Document();
        Map collectioninfo = new HashMap();
        collectioninfo.put("name", particleCollectionName);
        collectionlist.append("0", collectioninfo);
        collections.insertOne(collectionlist);

        mongoClient.close();

        return true;
    }

    public String name() {
        // return a string that identifies this database and schema implementation

        return "MongoDB";
    }
}
