package edu.carleton.dataloadbenchmark;

import java.util.List;
import java.util.Map;

public interface DatabaseLoad {
    // format data and then insert it into the database.
    // returns true if the operation was successful, and false if not
    boolean insert(Map par, List<String[]> sem, List<String[]> set, List<List<Map>> sparse, List<Map> dense);

    // returns a string that identifies the database and schema
    String name();
}
