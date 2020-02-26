package edu.carleton.dataloadbenchmark;

import java.util.List;
import java.util.Map;

public class MongoDBBenchmark implements DatabaseLoad {

    public boolean insert(Map par, List<String[]> sem, List<String[]> set, List<String> names, List<List<Map>> sparse, List<Map> dense) {
        // format data and insert into db
        // return true if successful and false if not

        return true;
    }

    public String name() {
        // return a string that identifies this database and schema implementation

        return "";
    }
}
