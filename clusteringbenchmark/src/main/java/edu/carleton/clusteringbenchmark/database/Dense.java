package edu.carleton.clusteringbenchmark.database;

import jnr.ffi.annotations.In;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import java.text.SimpleDateFormat;
//import java.util.Date;

import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;

import java.awt.geom.Area;
import java.text.DateFormat;
import java.text.SimpleDateFormat;


@Measurement(name = "dense")
public class Dense {

    @Column(name = "specname")
    private String specname;

    @Column(name = "par_id")
    private String par_id;

    @Column(name = "time")
    private String time;

    @Column(name = "_id")
    private Integer _id;

    @Column(name = "max")
    private Integer max;

    @Column(name = "scatdelay")
    private Double scatdelay;

    @Column(name = "size")
    private Double size;

    @Column(name = "laserpower")
    private Double laserpower;

    @Column(name = "Date")
    private String Date;

    public String getSpecname(){
        return specname;
    }

    public String getPar_id(){
        return par_id;
    }

    public String getTime(){
        return time;
    }

    public Integer get_id(){
        return _id;
    }

    public Integer getMax(){
        return max;
    }

    public Double getScatdelay(){
        return scatdelay;
    }

    public Double getSize(){
        return size;
    }

    public Double getLaserpower(){
        return laserpower;
    }

    public String getDate(){
        return Date;
    }

}
