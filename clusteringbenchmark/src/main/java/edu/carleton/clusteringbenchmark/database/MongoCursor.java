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

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.*;

public class MongoCursor implements CollectionCursor {

    private MongoDatabase db;
    private Document collection;
    private com.mongodb.client.MongoCursor<Document> atoms;
    private Document currentatom;

    public MongoCursor(Collection collection) {
        MongoClient client = MongoClients.create();
        db = client.getDatabase("enchilada_benchmark");
        this.collection = db.getCollection("collections").find(eq("_id", collection.getCollectionID())).first();
        reset();
    }

    public boolean next() {
        if (atoms.hasNext()) {
            currentatom = atoms.next();
            return true;
        }
        else {
            return false;
        }
    }

    public ParticleInfo getCurrent() {
        Document dense = (Document)currentatom.get("densedata");
        ParticleInfo particleInfo = new ParticleInfo();
        particleInfo.setParticleInfo(new ATOFMSAtomFromDB(
                currentatom.getInteger("_id"),
                dense.getString("specname"),
                dense.getInteger("scatdelay"),
                dense.getDouble("laserpower").floatValue(),
                dense.getDate("time"),
                dense.getDouble("size").floatValue()
        ));
        List<Document> sparse = currentatom.getList("sparsedata", Document.class);
        BinnedPeakList peaks = new BinnedPeakList();
        for (Document peak : sparse) {
            peaks.add(peak.getDouble("masstocharge").intValue(), peak.getInteger("area"));
        }
        particleInfo.setBinnedList(peaks);
        particleInfo.setID(currentatom.getInteger("_id"));
        return particleInfo;
    }

    public void close() {
        atoms.close();
    }

    public void reset() {
        List<Integer> atomids = collection.getList("atomids", Integer.class);
        atoms = db.getCollection("atoms")
                .find(in("_id", atomids))
                .sort(ascending("_id"))
                .cursor();
    }
}
