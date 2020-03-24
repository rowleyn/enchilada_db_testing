package edu.carleton.clusteringbenchmark;

import edu.carleton.clusteringbenchmark.analysis.clustering.KMeans;
import edu.carleton.clusteringbenchmark.database.InfoWarehouse;
import edu.carleton.clusteringbenchmark.database.NonZeroCursor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

public class ClusteringBenchmark {

    public static void main(String[] args) throws Exception {

        int collectionId;
        int numClusters;
        boolean normalize;

        BufferedWriter writer = new BufferedWriter(new FileWriter("results.txt"));

        List<InfoWarehouse> dbs = new ArrayList<>();
        List<NonZeroCursor> curs = new ArrayList<>();

        // add InfoWarehouse instances
        // add NonZeroCursor instances, each instantiated with a CollectionCursor implementation

        // Perform benchmarks
        for (int i = 0; i < dbs.size(); i++) {
            InfoWarehouse db = dbs.get(i);
            NonZeroCursor cur = curs.get(i);
            System.out.println("Beginning benchmark for system: " + db.dbname());
            long start = System.currentTimeMillis();
            KMeans kmeans = new KMeans(collectionId, db, numClusters, db.getCollectionName(collectionId), "", normalize);
            kmeans.setCurs(cur);
            int newCollection = kmeans.cluster();
            // need some kind of success test
            boolean success;
            long end = System.currentTimeMillis();
            if (success) {
                System.out.println("Benchmark finished for system: " + db.dbname());
                long timeelapsed = end - start;
                writer.write(db.dbname() + ": " + timeelapsed + " milliseconds");
                writer.newLine();
            }
            else {
                writer.write("Benchmark failed for system: " + db.dbname());
                writer.newLine();
            }
        }

        writer.close();
    }
}
