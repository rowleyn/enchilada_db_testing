package edu.carleton.dataloadbenchmark;

import java.util.List;
import java.util.Map;

public interface DatabaseLoad {
    boolean insert(Map par, List<String[]> sem, List<String[]> set, List<List<Map>> sparse, List<Map> dense);
    String name();
}
