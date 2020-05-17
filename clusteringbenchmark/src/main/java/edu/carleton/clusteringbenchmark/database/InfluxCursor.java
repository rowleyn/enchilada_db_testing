package edu.carleton.clusteringbenchmark.database;

import edu.carleton.clusteringbenchmark.ATOFMS.ParticleInfo;
import edu.carleton.clusteringbenchmark.analysis.BinnedPeakList;
import edu.carleton.clusteringbenchmark.atom.ATOFMSAtomFromDB;
import edu.carleton.clusteringbenchmark.collection.Collection;
import jnr.ffi.Struct;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBResultMapper;
import org.joda.time.Instant;

import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Date;

public class InfluxCursor implements CollectionCursor {
    Collection collection;

    InfluxDB influxDB;
    private String dbName;
    private QueryResult queryResult;
    private InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
    private List<InternalAtomOrder> atoms;
    private Integer currentIDIndex;

    public InfluxCursor(Collection collection) {
        influxDB = InfluxDBFactory.connect("http://localhost:8086", "influxdbUser", "influxdbPsw");
        dbName = "enchilada_benchmark";
        QueryResult queryResult = influxDB.query(new Query("SELECT atomID FROM internalAtomOrder", dbName));
        InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
        List<InternalAtomOrder> atoms = resultMapper.toPOJO(queryResult, InternalAtomOrder.class);
        this.queryResult = influxDB.query(new Query("SELECT atomID FROM internalAtomOrder WHERE collectionID ='" + collection.getCollectionID() + "' AND deleted =0", dbName));
        this.atoms = resultMapper.toPOJO(this.queryResult, InternalAtomOrder.class);
        this.currentIDIndex = 0;
        reset();
    }

    public boolean next() {
        try {
            if (atoms.get(currentIDIndex + 1) != null) {
                currentIDIndex++;
                return true;
            }
        } catch (Exception e) {
            System.out.println("Error checking the " +
                    "bounds of " +
                    "the ResultSet.");
            System.out.println(e);
        }
        return false;
    }

    /*
	calls get method.
    */
    public ParticleInfo getCurrent(){
        try {
            ParticleInfo particleInfo = new ParticleInfo();

            Integer currentID = atoms.get(currentIDIndex).getAtomID();

            QueryResult denseQuery = influxDB.query(new Query("SELECT * FROM dense WHERE _id = " + currentID, dbName));
            InfluxDBResultMapper denseMapper = new InfluxDBResultMapper();
            List<Dense> Denseresults = denseMapper.toPOJO(denseQuery, Dense.class);

            String time = Denseresults.get(0).getDate();

            SimpleDateFormat dateFormat = new SimpleDateFormat("''yyyy-MM-dd HH:mm:ss.S''");
            Date date5 = dateFormat.parse(time);

            ATOFMSAtomFromDB atominfo = new ATOFMSAtomFromDB(
                    currentID,
                    Denseresults.get(0).getSpecname(),
                    Denseresults.get(0).getScatdelay().intValue(),
                    Denseresults.get(0).getLaserpower().floatValue(),
                    date5,
                    Denseresults.get(0).getSize().floatValue());
            particleInfo.setParticleInfo(atominfo);

            BinnedPeakList peaks = new BinnedPeakList();
            QueryResult sparseQuery = influxDB.query(new Query("SELECT * FROM sparse WHERE _id = " + currentID, dbName));
            InfluxDBResultMapper sparseMapper = new InfluxDBResultMapper();
            List<Sparse> sparseResults = sparseMapper.toPOJO(sparseQuery, Sparse.class);
            for (int entry = 0; entry < sparseResults.size(); entry++) {
                peaks.add(sparseResults.get(entry).getMasstocharge().intValue(), sparseResults.get(entry).getArea());
            }
            particleInfo.setBinnedList(peaks);
            particleInfo.setID(currentID);
            return particleInfo;
        }
        catch(Exception e){
            System.out.println(e);
        }
        return null;
    }

    /*
    drops temprand table and calls super.close
    */
    public void close(){
        System.out.println("closing cursor");
        influxDB.close();
    }

    /*
	calls rs.close
	and then rs = getAllAtomsRS(collection)
	this selects every atomID from InternalAtomOrder where
	collectionID = collection.getcollectionid()
    */
    public void reset(){
        try {
            this.atoms = resultMapper.toPOJO(this.queryResult, InternalAtomOrder.class);
            this.currentIDIndex = 0;
        }
        catch (Exception e){
            System.out.println(e);
        }
    }
}
