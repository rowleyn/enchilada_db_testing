
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
        MongoCollection<Document> pars = database.getCollection("pars");
        MongoCollection<Document> particles = database.getCollection("particles");

        pars.drop();
        particles.drop();
    }

    public boolean insert(DataRead reader) {
        // format data and insert into db
        // return true if successful and false if not

        MongoClient mongoClient = MongoClients.create();
        MongoDatabase database = mongoClient.getDatabase("enchilada_benchmark");
        MongoCollection<Document> pars = database.getCollection("pars");
        MongoCollection<Document> particles = database.getCollection("particles");

        Map par = new HashMap();

        String dbdatasetname = (String)reader.par.get("dbdatasetname");
        par.put("_id", dbdatasetname);
        par.put("datasetname", reader.par.get("datasetname"));
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

        //List<Document> spectra = new ArrayList<>();
        boolean moretoread = true;
        int setindex = 0;

        while (moretoread) {
            List data = reader.readNSpectraFrom(1, setindex);
            setindex = (int)data.get(data.size() - 1);
            List<Document> spectra = new ArrayList<>();

            for (int i = 0; i < data.size() - 1; i++) {
                Map spectrum = new HashMap();
                spectrum.put("_id", ((Map)data.get(i)).get("name"));
                spectrum.put("par_id", dbdatasetname);
                spectrum.put("sparsedata", ((Map)data.get(i)).get("sparse"));
                spectrum.put("densedata", ((Map)data.get(i)).get("dense"));
                spectra.add(new Document(spectrum));
            }

            try {
                particles.insertOne(spectra.get(0));
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

        mongoClient.close();

        return true;
    }

    public String name() {
        // return a string that identifies this database and schema implementation

        return "MongoDB";
    }
}
