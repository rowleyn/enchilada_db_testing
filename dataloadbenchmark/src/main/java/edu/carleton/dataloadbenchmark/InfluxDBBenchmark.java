package edu.carleton.dataloadbenchmark;


import com.mongodb.MongoException;
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

            boolean moretoread = true;
            int setindex = 0;

            while (moretoread) {
                List data = reader.readNSpectraFrom(1, setindex);
                setindex = (int) data.get(data.size() - 1);
                for (int i = 0; i < data.size() - 1; i++) {
                    List spectrum = (List) ((Map) data.get(i)).get("sparse");
                    if (((Map) data.get(i)).get("name") != null) {

                        Date date = (Date) ((Map) ((Map) data.get(i)).get("dense")).get("time");
                        DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                        String strDate = dateFormat.format(date);

                        Integer scatdelay = (Integer) ((Map) ((Map) data.get(i)).get("dense")).get("scatdelay");
                        Float size = (Float) ((Map) ((Map) data.get(i)).get("dense")).get("size");
                        Float laserpower = (Float) ((Map) ((Map) data.get(i)).get("dense")).get("laserpower");
                        String specname = (String) ((Map) ((Map) data.get(i)).get("dense")).get("specname");

                        influxDB.write(Point.measurement("spectrum")
                                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                .addField("_id", (String) ((Map) data.get(i)).get("name"))
                                .tag("specname", specname)
                                .tag("par_id", dbdatasetname)
                                .addField("scatdelay", scatdelay)
                                .addField("size", size)
                                .addField("laserpower", laserpower)
                                .addField("Date", strDate)
                                .build());

                        for (int j = 0; j < spectrum.size(); j++) {
                            Map sparseData = (Map) (((List) ((Map) data.get(i)).get("sparse")).get(j));
                            influxDB.write(Point.measurement("sparse")
                                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                    .tag("specname", specname)
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
                return false;
        }
    }

    // clear out data prior to starting benchmark
    public void clear(){
        InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "influxdbUser", "influxdbPsw");
        influxDB.setDatabase("enchilada_benchmark");
        influxDB.query(new Query("DROP SERIES FROM pars"));
        influxDB.query(new Query("DROP SERIES FROM particles"));
    }

    // returns a string that identifies the database and schema
    public String name(){
    	return "InfluxDB";
    }
}
