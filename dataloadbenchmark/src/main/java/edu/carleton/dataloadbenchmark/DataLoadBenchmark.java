
package edu.carleton.dataloadbenchmark;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

public class DataLoadBenchmark {

    public static void main(String[] args) throws Exception {

        // read in all dataset files
        DataRead data = new DataRead(
                "data/e/e.par",
                "data/e/e.set",
                "data/20100812_metals_00005_masscal.cal",
                "data/sizecal_20100701.noz");


//        System.out.println(data.par);
//        System.out.println(Arrays.toString(data.sem.get(0)));
//        System.out.println(Arrays.toString(data.sem.get(5000)));
//        System.out.println(Arrays.toString(data.set.get(0)));
//        System.out.println(Arrays.toString(data.set.get(5000)));
//        System.out.println(data.particlenames.get(0));
//        System.out.println(data.particlenames.get(5000));
//        System.out.println(data.sparsemaps.get(0));
//        System.out.println(data.sparsemaps.get(5000));
//        System.out.println(data.densemaps.get(0));
//        System.out.println(data.densemaps.get(5000));


        // Initialize variables
        List<DatabaseLoad> dbs = new ArrayList<>();
        BufferedWriter writer = new BufferedWriter(new FileWriter("results.txt"));

        // Instantiate database inserters here and add them to list
        DatabaseLoad mongo = new MongoDBBenchmark();

        dbs.add(mongo);

        // Perform benchmarks
        for (DatabaseLoad db : dbs) {
            db.clear();
            System.out.println("Beginning benchmark for system: " + db.name());
            long start = System.currentTimeMillis();
            boolean success = db.insert(data);
            long end = System.currentTimeMillis();
            if (success) {
                System.out.println("Benchmark finished for system: " + db.name());
                long timeelapsed = end - start;
                writer.write(db.name() + ": " + timeelapsed + " milliseconds");
                writer.newLine();
            }
            else {
                writer.write("Benchmark failed for system: " + db.name());
                writer.newLine();
            }
        }

        writer.close();
    }
}
