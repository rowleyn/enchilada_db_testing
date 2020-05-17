package edu.carleton.dataloadbenchmark;


import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBResultMapper;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class InfluxDBBenchmark implements DatabaseLoad {

    // format data and then insert it into the database.
    // returns true if the operation was successful, and false if not
    public boolean insert(DataRead reader){
        try {
            InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "influxdbUser", "influxdbPsw");
            String dbName = "enchilada_benchmark";
            influxDB.query(new Query("CREATE DATABASE " + dbName));
            influxDB.setDatabase(dbName);
            String rpName = "aRetentionPolicy";
            influxDB.query(new Query("CREATE RETENTION POLICY " + rpName + " ON " + dbName + " DURATION 30h REPLICATION 2 SHARD DURATION 30m DEFAULT"));
            influxDB.setRetentionPolicy(rpName);
            influxDB.enableBatch(BatchOptions.DEFAULTS);

            String dbdatasetname = (String) reader.par.get("dbdatasetname");
            influxDB.write(Point.measurement("metaData")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .addField("_id", dbdatasetname)
                    .tag("datasetname", (String)reader.par.get("datasetname"))
                    .tag("starttime", (String)reader.par.get("starttime"))
                    .addField("startdate", (String)reader.par.get("startdate"))
                    .addField("inlettype", (String)reader.par.get("inlettype"))
                    .addField("comment", (String)reader.par.get("comment"))
                    .build());

            influxDB.write(Point.measurement("collections")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .tag("colName", dbdatasetname + "_particles")
                    .tag("collectionID", "0"  )
                    .addField("collectionIDInt" , 0)
                    .addField("parent", -1)
                    .addField("colName", dbdatasetname + "_particles")
                    .addField("description" , "This is a desc")
                    .addField("datatype", "ATOFMS")
                    .build());

            boolean moretoread = true;
            int setindex = 0;
            int atomId = 0;
            while (moretoread) {
                List data = reader.readNSpectraFrom(1, setindex);
                setindex = (int) data.get(data.size() - 1);
                atomId++;
                for (int i = 0; i < data.size() - 1; i++) {
                    List spectrum = (List) ((Map) data.get(i)).get("sparse");
                    if (((Map) data.get(i)).get("name") != null) {
                        Map denseData = (Map) ((Map) data.get(i)).get("dense");
                        Integer scatdelay = (Integer) denseData.get("scatdelay");
                        Float size = (Float) denseData.get("size");
                        Float laserpower = (Float) denseData.get("laserpower");
                        String specname = (String) ((Map) data.get(i)).get("name");
                        Date date = (Date) denseData.get("time");
                        DateFormat dateFormat = new SimpleDateFormat("''yyyy-MM-dd HH:mm:ss.S''");
                        String strDate = dateFormat.format(date);
                        influxDB.write(Point.measurement("dense")
                                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                .tag("specname", specname)
                                .addField("_id", atomId)
                                .addField("scatdelay", scatdelay)
                                .addField("size", size)
                                .addField("laserpower", laserpower)
                                .addField("Date", strDate)
                                .build());
                        influxDB.write(Point.measurement("internalAtomOrder")
                                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                .tag("collectionID", "0")
                                .addField("atomID", atomId)
                                .addField("deleted", 0)
                                .build());
                        for (int j = 0; j < spectrum.size(); j++) {
                            Map sparseData = (Map) (((List) ((Map) data.get(i)).get("sparse")).get(j));
                            influxDB.write(Point.measurement("sparse")
                                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                    .tag("specname", (String) ((Map) data.get(i)).get("name"))
                                    .addField("_id", atomId)
                                    .addField("Area", (Integer) sparseData.get("area"))
                                    .addField("relarea", (Float) sparseData.get("relarea"))
                                    .addField("masstocharge", (Double) sparseData.get("masstocharge"))
                                    .addField("height", (Integer) sparseData.get("height"))
                                    .build());
                        }
                    }
                }
                if (setindex >= reader.set.size()) {
                    moretoread = false;
                }
            }
            influxDB.close();
            return true;
        }
        catch (InfluxDBException e){
                System.out.println("Something went wrong");
                System.out.println(e);
                return false;
        }
    }

    // clear out data prior to starting benchmark
    public void clear(){
        InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "influxdbUser", "influxdbPsw");
        influxDB.query(new Query("DROP DATABASE enchilada_benchmark"));
    }

    // returns a string that identifies the database and schema
    public String name(){
    	return "InfluxDB";
    }
}
