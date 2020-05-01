
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
        MongoCollection<Document> metadata = database.getCollection("metadata");
        MongoCollection<Document> collections = database.getCollection("collections");
        MongoCollection<Document> atoms = database.getCollection("atoms");
        MongoCollection<Document> pars = database.getCollection("pars");

        Document par = pars.find().first();
        if (par != null) {
            MongoCollection<Document> particles = database.getCollection(par.get("datasetname") + "_particles");
            MongoCollection<Document> clusters = database.getCollection(par.get("datasetname") + "_clusters");
            particles.drop();
            clusters.drop();
        }

        metadata.drop();
        collections.drop();
        atoms.drop();
        pars.drop();
    }

    public boolean insert(DataRead reader) {
        // format data and insert into db
        // return true if successful and false if not

        MongoClient mongoClient = MongoClients.create();
        MongoDatabase database = mongoClient.getDatabase("enchilada_benchmark");

        // build metadata document
        MongoCollection<Document> metadata = database.getCollection("metadata");
        // dense metadata
        Document densemdata = new Document();
        densemdata.put("time", "DATETIME");
        densemdata.put("laserpower", "REAL");
        densemdata.put("size", "REAL");
        densemdata.put("scatdelay", "INT");
        densemdata.put("specname", "VARCHAR(8000)");
        // sparse metadata
        Document sparsemdata = new Document();
        sparsemdata.put("masstocharge", "REAL");
        sparsemdata.put("area", "INT");
        sparsemdata.put("relarea", "REAL");
        sparsemdata.put("height", "INT");
        // both are a kind of ATOFMS metadata
        Document atofmsdata = new Document();
        atofmsdata.put("dense", densemdata);
        atofmsdata.put("sparse", sparsemdata);
        Document mdata = new Document();
        mdata.put("ATOFMS", atofmsdata);
        metadata.insertOne(mdata);

        // build par document
        MongoCollection<Document> pars = database.getCollection("pars");

        Document par = new Document();

        par.put("_id", 0);
        par.put("datasetname", reader.par.get("dbdatasetname"));
        par.put("starttime", reader.par.get("starttime"));
        par.put("startdate", reader.par.get("startdate"));
        par.put("inlettype", reader.par.get("inlettype"));
        par.put("comment", reader.par.get("comment"));

        try {
            pars.insertOne(par);
        }
        catch (MongoException e) {
            System.out.println("Something went wrong...");
            System.out.println("System: " + name());
            System.out.println(e);

            return false;
        }

        // build particle collection
        String particleCollectionName = reader.par.get("dbdatasetname") + "_particles";
        MongoCollection<Document> particles = database.getCollection(particleCollectionName);
        MongoCollection<Document> atoms = database.getCollection("atoms");

        boolean moretoread = true;
        int setindex = 0;
        int atomidcount = 0;

        while (moretoread) {
            List data = reader.readNSpectraFrom(1, setindex);
            setindex = (int)data.get(data.size()-1);

            Document spectrumdata = new Document();
            spectrumdata.put("_id", atomidcount);
            spectrumdata.put("name", ((Map)data.get(0)).get("name"));
            spectrumdata.put("sparsedata", ((Map)data.get(0)).get("sparse"));
            spectrumdata.put("densedata", ((Map)data.get(0)).get("dense"));

            Document newatomentry = new Document();
            Document csentry = new Document();
            List cs = new ArrayList();
            csentry.append("collectionid", 0);
            csentry.append("collectionname", particleCollectionName);
            cs.add(csentry);
            newatomentry.append("_id", atomidcount);
            newatomentry.append("collections", cs);
            atomidcount++;

            try {
                particles.insertOne(spectrumdata);
                atoms.insertOne(newatomentry);
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

        // build collection map
        MongoCollection<Document> collections = database.getCollection("collections");
        Document collectioninfo = new Document();
        collectioninfo.put("_id", 0);
        collectioninfo.put("name", particleCollectionName);
        collectioninfo.put("datatype", "ATOFMS");
        collections.insertOne(collectioninfo);

        mongoClient.close();

        return true;
    }

    public String name() {
        // return a string that identifies this database and schema implementation

        return "MongoDB";
    }
}
