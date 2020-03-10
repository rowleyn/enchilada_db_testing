package edu.carleton.clusteringbenchmark;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

public class ClusteringBenchmark {

    public static void main(String[] args) throws Exception {

        BufferedWriter writer = new BufferedWriter(new FileWriter("results.txt"));

        // Perform benchmarks
        for () {
            System.out.println("Beginning benchmark for system: " + );
            long start = System.currentTimeMillis();
            // do benchmark
            long end = System.currentTimeMillis();
            if (success) {
                System.out.println("Benchmark finished for system: " + );
                long timeelapsed = end - start;
                writer.write( + ": " + timeelapsed + " milliseconds");
                writer.newLine();
            }
            else {
                writer.write("Benchmark failed for system: " + );
                writer.newLine();
            }
        }

        writer.close();
    }
}
