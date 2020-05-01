package edu.carleton.clusteringbenchmark.database;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class InfluxWarehouse implements InfoWarehouse{
    public InfluxWarehouse(){
        InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "influxdbUser", "influxdbPsw");
        String dbName = "enchilada_benchmark";
        influxDB.setDatabase(dbName);
    }

    public Collection getCollection(int collectionId) {

    }

}
