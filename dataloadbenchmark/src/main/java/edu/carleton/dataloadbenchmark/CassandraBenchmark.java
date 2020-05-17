package edu.carleton.dataloadbenchmark;

import com.datastax.driver.core.*;

import static java.lang.System.out;
import com.datastax.driver.core.Cluster;
import org.apache.cassandra.exceptions.CassandraException;
import java.text.DateFormat;

import javax.xml.transform.Result;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;

public class CassandraBenchmark implements DatabaseLoad {
    public void clear(){
        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                    .addContactPoint("127.0.0.1")
                    .withProtocolVersion(ProtocolVersion.V3)
                    .withoutJMXReporting()
                    .build();
            Session session = cluster.connect();
            String drop = "DROP TABLE IF EXISTS pars.par";
            session.execute(drop);
            drop = "DROP TABLE IF EXISTS particles.collections";
            session.execute(drop);
            drop = "DROP TABLE IF EXISTS particles.dense";
            session.execute(drop);
            drop = "DROP TABLE IF EXISTS particles.sparse";
            session.execute(drop);
            drop = "DROP TABLE IF EXISTS particles.centeratoms";
            session.execute(drop);
        } finally {
            if (cluster != null) cluster.close();
        }
    }

    public boolean insert(DataRead reader) {
        // format data and insert into db
        // return true if successful and false if not
        this.clear();
        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                    .addContactPoint("127.0.0.1")
                    .withoutJMXReporting()
                    .build();
            Session session = cluster.connect();

            ResultSet rs = session.execute("select release_version from system.local");
            String createKeySpace =
                    "CREATE KEYSPACE IF NOT EXISTS pars WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};";
            session.execute(createKeySpace);
            String createParCql =
                    " CREATE TABLE pars.par (dbdatasetname varchar, datasetname varchar, starttime varchar, "
                            + "startdate varchar, inlettype varchar, comment varchar, PRIMARY KEY (dbdatasetname))";
            session.execute(createParCql);

            Object dbdatasetname = reader.par.get("dbdatasetname");

            session.execute(
                    "INSERT INTO pars.par (dbdatasetname, datasetname, starttime, startdate, inlettype, comment) VALUES (?, ?, ?, ?, ?, ?)",
                    dbdatasetname, reader.par.get("datasetname"), reader.par.get("starttime"),
                    reader.par.get("startdate"), reader.par.get("inlettype"), reader.par.get("comment"));
            createKeySpace =
                    "CREATE KEYSPACE IF NOT EXISTS particles WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};";
            session.execute(createKeySpace);


            session.execute("CREATE TABLE particles.dense (dbdatasetname varchar, time timestamp, laserpower varchar," +
                                    " size varchar, scatdelay varchar, specname varchar, AtomID int, PRIMARY KEY (AtomID))");

            //Assuming masstocharge represents location
            session.execute("CREATE TABlE particles.sparse (dbdatasetname varchar, area varchar, relarea varchar, " +
                    "masstocharge varchar, height varchar, AtomID varchar, PRIMARY KEY (AtomID, masstocharge))");

            session.execute("CREATE TABLE particles.collections (CollectionID int, Parent int, Name varchar, Comment varchar, Description varchar, Datatype varchar, AtomID int, PRIMARY KEY (CollectionId, AtomID))");

            session.execute("CREATE TABLE particles.centeratoms (AtomID varchar, CollectionID varchar, PRIMARY KEY(CollectionID, AtomID))");

            boolean moretoread = true;
            int setindex = 0;
            int v = 0;
            String particleCollectionName = reader.par.get("dbdatasetname") + "_particles";
            PreparedStatement query = session.prepare("INSERT INTO particles.sparse (dbdatasetname, area, relarea, masstocharge, height, AtomID) Values (?, ?, ?, ?, ?, ?)");
            while(moretoread) {
                List data = reader.readNSpectraFrom(1, setindex);
                setindex = (int)data.get(data.size() - 1);
                for (int i = 0; i < data.size() - 1; i++) {
                    if(((Map)data.get(i)).get("name") == null){ break;}
                    List<Map> sparse = (List<Map>)((Map)data.get(i)).get("sparse");
                    Date date = (Date) ((Map) ((Map) data.get(i)).get("dense")).get("time");
                    String laserpower= ((Map)((Map)data.get(i)).get("dense")).get("laserpower").toString();
                    String size =  ((Map)((Map)data.get(i)).get("dense")).get("size").toString();
                    String scatdelay =  ((Map)((Map)data.get(i)).get("dense")).get("scatdelay").toString();
                    String specname = (String) ((Map)((Map)data.get(i)).get("dense")).get("specname");
                    session.executeAsync("INSERT INTO particles.dense (dbdatasetname, time, laserpower, size, scatdelay, specname, AtomID) Values (?, ?, ?, ?, ?, ?, ?)",
                            dbdatasetname, date, laserpower, size, scatdelay, specname, v);
                    int k = 0;

                    for(int j = 0; j <sparse.size(); j++){
                        int area = (int) sparse.get(j).get("area");
                        String relarea = sparse.get(j).get("relarea").toString();
                        String masstocharge = sparse.get(j).get("masstocharge").toString();
                        String height =  sparse.get(j).get("masstocharge").toString();

                        BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED).add(query.bind(dbdatasetname, String.valueOf(area), relarea, masstocharge, height, String.valueOf(v)));
                        if(k >=500){
                            session.execute(batch);

                            k =0;
                        }
                        else{k++;}

                    }
                    session.executeAsync("INSERT INTO particles.collections (CollectionID, name, AtomId) Values(?, ?, ?)", 0, particleCollectionName, v);
                    v++;

                }

                if (setindex >= reader.set.size()) {
                    moretoread = false;
                }
            }

            Row row = rs.one();
            System.out.println(row.getString("release_version"));



        } catch (CassandraException ce) {
            if (cluster != null) cluster.close();
            out.println("Something went wrong...");
            out.print(ce);
            return false;
        }
        return true;

    }

    public String name() {
        // return a string that identifies this database and schema implementation
        return "Cassandra";
    }
}