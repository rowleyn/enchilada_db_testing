package edu.carleton.dataloadbenchmark;

import java.util.List;
import java.util.Map;

public interface DatabaseLoad {
    // format data and then insert it into the database.
    // returns true if the operation was successful, and false if not
    boolean insert(DataRead reader);

    // clear out data prior to starting benchmark
    void clear();

    // returns a string that identifies the database and schema
    String name();
}
