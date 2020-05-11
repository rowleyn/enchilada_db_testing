package edu.carleton.clusteringbenchmark;

import edu.carleton.clusteringbenchmark.analysis.clustering.KMeans;
import edu.carleton.clusteringbenchmark.database.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

public class ClusteringBenchmark {

    public static void main(String[] args) throws Exception {

        int collectionId = 0;
        int numClusters = 1;
        boolean normalize = true;

        BufferedWriter writer = new BufferedWriter(new FileWriter("results.txt"));

        List<InfoWarehouse> dbs = new ArrayList<>();

        // add InfoWarehouse instances
        dbs.add(new MongoWarehouse());

        // Perform benchmarks
        for (InfoWarehouse db : dbs) {
            NonZeroCursor cur = new NonZeroCursor(db.getAtomInfoOnlyCursor(db.getCollection(collectionId)));
            System.out.println("Beginning benchmark for system: " + db.dbname());
            long start = System.currentTimeMillis();
            KMeans kmeans = new KMeans(collectionId, db, numClusters, db.getCollectionName(collectionId), "comment", normalize);
            kmeans.setCurs(cur);
            kmeans.cluster();
            long end = System.currentTimeMillis();
            System.out.println("Benchmark finished for system: " + db.dbname());
            long timeelapsed = end - start;
            writer.write(db.dbname() + ": " + timeelapsed + " milliseconds");
            writer.newLine();
        }

        writer.close();
    }
}
