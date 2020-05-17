package edu.carleton.clusteringbenchmark.database;

import jnr.ffi.annotations.In;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;

import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;

import java.awt.geom.Area;
import java.text.DateFormat;


@Measurement(name = "collections")
public class Collections {

    @Column(name = "collectionID")
    private String collectionID;

    @Column(name = "time")
    private String time;

    @Column(name = "collectionIDInt")
    private Integer collectionIDInt;

    @Column(name = "max")
    private Integer max;

    @Column(name = "colName")
    private String colName;

    @Column(name = "description")
    private String description;

    @Column(name = "datatype")
    private String datatype;

    public String getCollectionID(){
        return collectionID;
    }

    public Integer getMax(){
        return max;
    }

    public Integer getCollectionIDInt(){
        return collectionIDInt;
    }

    public String getColName(){
        return colName;
    }

    public String getTime(){
        return time;
    }

    public String getDescription(){
        return description;
    }

    public String getDatatype(){
        return datatype;
    }
}
