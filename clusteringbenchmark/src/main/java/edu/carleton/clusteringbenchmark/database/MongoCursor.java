package edu.carleton.clusteringbenchmark.database;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import edu.carleton.clusteringbenchmark.ATOFMS.ParticleInfo;
import edu.carleton.clusteringbenchmark.analysis.BinnedPeakList;
import edu.carleton.clusteringbenchmark.atom.ATOFMSAtomFromDB;
import edu.carleton.clusteringbenchmark.collection.Collection;
import org.bson.Document;

import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.*;

public class MongoCursor implements CollectionCursor {

    private MongoDatabase db;
    private Document collection;
    private com.mongodb.client.MongoCursor<Document> atoms;

    public MongoCursor(Collection collection) {
        MongoClient client = MongoClients.create();
        db = client.getDatabase("enchilada_benchmark");
        this.collection = db.getCollection("collections").find(eq("_id", collection.getCollectionID())).first();
        reset();
    }

    public boolean next() {
        return atoms.hasNext();
    }

    public ParticleInfo getCurrent() {
        Document atom = atoms.next();
        Document dense = (Document)atom.get("densedata");
        ParticleInfo particleInfo = new ParticleInfo();
        particleInfo.setParticleInfo(new ATOFMSAtomFromDB(
                atom.getInteger("_id"),
                dense.getString("specname"),
                dense.getInteger("scatdelay"),
                dense.getDouble("laserpower").floatValue(),
                dense.getDate("time"),
                dense.getDouble("size").floatValue()
        ));
        List<Document> sparse = atom.getList("sparsedata", Document.class);
        BinnedPeakList peaks = new BinnedPeakList();
        for (Document peak : sparse) {
            peaks.add(peak.getInteger("masstocharge"), peak.getInteger("area"));
        }
        particleInfo.setBinnedList(peaks);
        particleInfo.setID(atom.getInteger("_id"));
        return particleInfo;
    }

    public void close() {
        atoms.close();
    }

    public void reset() {
        List<Integer> atomids = collection.getList("atomids", Integer.class);
        atoms = db.getCollection("atoms")
                .find(in("_id", atomids))
                .projection(include("_id", "densedata"))
                .sort(ascending("_id"))
                .cursor();
    }
}
