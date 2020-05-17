package edu.carleton.clusteringbenchmark.database;

import jnr.ffi.annotations.In;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;

import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.joda.time.Instant;


@Measurement(name = "internalAtomOrder")
public class InternalAtomOrder {

    @Column(name = "collectionID")
    private String collectionID;

    @Column(name = "count")
    private Integer count;

    @Column(name = "atomID")
    private Integer atomID;

    @Column(name = "time")
    private String time;

    @Column(name = "deleted")
    private Integer deleted;

    public Integer getCount(){
        return count;
    }

    public Integer getDeleted(){
        return deleted;
    }

    public String getTime(){
        return time;
    }

    public Integer getAtomID(){
        return atomID;
    }

    public String getCollectionID(){
        return collectionID;
    }

}

