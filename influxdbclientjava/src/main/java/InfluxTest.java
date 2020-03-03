

//import okhttp3.OkHttpClient;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
//import org.influxdb.InfluxDB.LogLevel;
//import org.influxdb.InfluxDB.ResponseFormat;
import org.influxdb.InfluxDBFactory;
//import org.influxdb.dto.BatchPoints;
//import org.influxdb.dto.BoundParameterQuery.QueryBuilder;
import org.influxdb.dto.Point;
//import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
//import org.influxdb.dto.QueryResult;
//import org.influxdb.dto.QueryResult.Series;
//import org.influxdb.impl.InfluxDBImpl;

import java.util.concurrent.TimeUnit;

public class InfluxTest {
    public static void main(String[] args){
//    InfluxDB influxDB = InfluxDBFactory.connect("http://172.17.0.2:8086", "root", "root");
    InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "influxdbUser", "influxdbPsw");
    String dbName = "aTimeSeries";
    influxDB.query(new Query("CREATE DATABASE " + dbName));
    influxDB.setDatabase(dbName);
    String rpName = "aRetentionPolicy";
    influxDB.query(new Query("CREATE RETENTION POLICY " + rpName + " ON " + dbName + " DURATION 30h REPLICATION 2 SHARD DURATION 30m DEFAULT"));
    influxDB.setRetentionPolicy(rpName);



    influxDB.enableBatch(BatchOptions.DEFAULTS);
    for (int i = 100; i>1; i--) {
        influxDB.write(Point.measurement("cpu")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .addField("idle", 90L)
                .addField("user", 9L)
                .addField("system", 1L)
                .build());
    }

    // Need to sleep otherwise data isn't done writing in yet as influx asynchronous
    try {
        Thread.sleep(3000);
    } catch (InterruptedException e){
        e.printStackTrace();
    }
    Query query = new Query("SELECT * FROM cpu", dbName);
    influxDB.query(query);
    System.out.println(influxDB.query(query));
    influxDB.query(new Query("DROP RETENTION POLICY " + rpName + " ON " + dbName));
    influxDB.query(new Query("DROP DATABASE " + dbName));
    influxDB.close();
    }
}
