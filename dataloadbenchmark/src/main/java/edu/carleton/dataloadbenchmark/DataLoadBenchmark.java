
package edu.carleton.dataloadbenchmark;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataLoadBenchmark {

    public static void main(String[] args) throws Exception {

        // read in all dataset files
        DataRead data = new DataRead(
                "data/e/e.par",
                "data/e/e.sem",
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

        // Perform benchmarks
        for (DatabaseLoad db : dbs) {
            long start = System.currentTimeMillis();
            db.insert(data.par, data.sem, data.set, data.particlenames, data.sparsemaps, data.densemaps);
            long end = System.currentTimeMillis();
            long timeelapsed = end - start;
            writer.write(db.name() + timeelapsed);
            writer.newLine();
        }

        writer.close();
    }
}
