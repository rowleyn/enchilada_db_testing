
package edu.carleton.dataloadbenchmark;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;

import java.util.*;

public class MongoDBBenchmark implements DatabaseLoad {

    public boolean insert(Map par, List<String[]> set, List<String> names, List<List<Map>> sparse, List<Map> dense) {
        // format data and insert into db
        // return true if successful and false if not

        String dbdatasetname = (String)par.get("dbdatasetname");
        par.remove("dbdatasetname");
        par.put("_id", dbdatasetname);

        Document pardoc = new Document(par);
        List<Document> spectra = new ArrayList<>();

        for (int i = 0; i < set.size(); i++) {
            Map spectrum = new HashMap();
            spectrum.put("_id", names.get(i));
            spectrum.put("par_id", dbdatasetname);
            spectrum.put("sparsedata", sparse.get(i));
            spectrum.put("densedata", dense.get(i));

            spectra.add(new Document("spectrum", spectrum));
        }

        MongoClient mongoClient = MongoClients.create();
        MongoDatabase database = mongoClient.getDatabase("enchilada_benchmark");
        MongoCollection<Document> pars = database.getCollection("pars");
        MongoCollection<Document> particles = database.getCollection("particles");

        try {
            pars.insertOne(pardoc);
            particles.insertMany(spectra);
        }
        catch (MongoException e) {
            System.out.println("Something went wrong...");
            System.out.println("System: " + name());
            System.out.println(e);

            return false;
        }

        return true;
    }

    public String name() {
        // return a string that identifies this database and schema implementation

        return "MongoDB";
    }
}
