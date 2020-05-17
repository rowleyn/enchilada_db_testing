package edu.carleton.clusteringbenchmark.database;

import jnr.ffi.annotations.In;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;

import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;

import java.awt.geom.Area;
import java.math.BigDecimal;


@Measurement(name = "sparse")
public class Sparse {

    @Column(name = "specname")
    private String specname;

    @Column(name = "_id")
    private Integer _id;

    @Column(name = "Area")
    private Integer Area;

    @Column(name = "relarea")
    private Double relarea;

    @Column(name = "masstocharge")
    private Double masstocharge;

    @Column(name = "height")
    private Integer height;

    public String getSpecname(){
        return specname;
    }

    public Integer get_id(){
        return _id;
    }

    public Integer getArea(){
        return Area;
    }

    public Double getRelarea(){
        return relarea;
    }

    public Double getMasstocharge(){
        return masstocharge;
    }

    public Integer getHeight(){
        return height;
    }

}