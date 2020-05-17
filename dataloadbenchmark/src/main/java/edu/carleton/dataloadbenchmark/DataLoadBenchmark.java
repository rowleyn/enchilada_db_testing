
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

        // Initialize variables
        List<DatabaseLoad> dbs = new ArrayList<>();
        BufferedWriter writer = new BufferedWriter(new FileWriter("results.txt"));

        // Instantiate database inserters here and add them to list
        //DatabaseLoad mongo = new MongoDBBenchmark();
        DatabaseLoad sql = new SQLBenchmark();
        DatabaseLoad postgres = new PostgreSQLBenchmark();


        DatabaseLoad cassandra = new CassandraBenchmark();

        //dbs.add(mongo);
        //dbs.add(cassandra);
        dbs.add(sql);
        dbs.add(postgres);

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
