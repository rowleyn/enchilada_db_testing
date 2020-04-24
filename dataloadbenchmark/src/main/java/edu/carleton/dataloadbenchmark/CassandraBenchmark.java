package edu.carleton.dataloadbenchmark;

import com.datastax.driver.core.*;

import static java.lang.System.out;
import com.datastax.driver.core.Cluster;
import org.apache.cassandra.exceptions.CassandraException;

import javax.xml.transform.Result;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class CassandraBenchmark implements DatabaseLoad {
    public void clear(){
        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                    .addContactPoint("127.0.0.1")
                    .withProtocolVersion(ProtocolVersion.V3)
                    //.addContactPoints(InetAddress.getLocalHost())
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
        //} catch (UnknownHostException e) {
          //  e.printStackTrace();
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


           // String createParticleCql =
            //        " CREATE TABLE particles.particle (name varchar, dbdatasetname varchar, sparse list<frozen<map<text, int>>>, dense list<text>, PRIMARY KEY (name))";
            //session.execute(createParticleCql);

            session.execute("CREATE TABLE particles.dense (name varchar, dbdatasetname varchar, time int, laserpower decimal," +
                                    " size decimal, scatdelay int, specname varchar, PRIMARY KEY (name))");

            //Assuming masstocharge represents location
            session.execute("CREATE TABlE particles.sparse (name varchar, dbdatasetname varchar, area int, relarea decimal," +
                    "masstocharge double, height double, PRIMARY KEY (name, masstocharge))");

            session.execute("CREATE TABLE particles.collections (CollectionID int, Parent int, Name varchar, Comment varchar, Description varchar, Datatype varchar, AtomID int, PRIMARY KEY (CollectionId, Name))");
            //session.execute("CREATE TABLE particles.collections (parID varchar, collectionID varchar, PRIMARY KEY (parID, collectionID))");


            boolean moretoread = true;
            int setindex = 0;
            int v = 0;
            String particleCollectionName = reader.par.get("dbdatasetname") + "_particles";
            while(moretoread) {
                List data = reader.readNSpectraFrom(1, setindex);
                setindex = (int)data.get(data.size() - 1);

                for (int i = 0; i < data.size() - 1; i++) {
                    if(((Map)data.get(i)).get("name") == null){ break;}
                    List<Map> sparse = (List<Map>)((Map)data.get(i)).get("sparse");
                    Map<String, Integer> dense = (Map)((Map)data.get(i)).get("dense");
                    Date time = (Date)((Map)((Map)data.get(i)).get("dense")).get("time");
                    Float laserpower= (Float)((Map)((Map)data.get(i)).get("dense")).get("laserpower");
                    Float size = (Float) ((Map)((Map)data.get(i)).get("dense")).get("size");
                    int scatdelay = (int) ((Map)((Map)data.get(i)).get("dense")).get("scatdelay");
                    String specname = (String) ((Map)((Map)data.get(i)).get("dense")).get("specname");
                    session.executeAsync("INSERT INTO particles.dense (name, dbdatasetname, time, laserpower, size, scatdelay, specname) Values (?, ?, ?, ?, ?, ?, ?)",
                            ((Map)data.get(i)).get("name"), dbdatasetname, LocalDate.fromMillisSinceEpoch(time.getTime()), laserpower, size, scatdelay, specname);
                    /**
                    for(int j = 0; j <sparse.size(); j++){
                        int area = (int) sparse.get(j).get("area");
                        Float relarea = (Float) sparse.get(j).get("relarea");
                        Double masstocharge = (Double) sparse.get(j).get("masstocharge");
                        Double height = (Double) sparse.get(j).get("masstocharge");
                        session.executeAsync("INSERT INTO particles.sparse (name, dbdatasetname, area, relarea, masstocharge, height) Values (?, ?, ?, ?, ?, ?)",
                               ((Map)data.get(i)).get("name"), dbdatasetname, area, relarea, masstocharge, height);
                        System.out.println(masstocharge);
                    }*/

                    session.executeAsync("INSERT INTO particles.collections (CollectionID, name, AtomId) Values(?, ?, ?)", "0", particleCollectionName, v);
                    v++;

                }

                if (setindex >= reader.set.size()) {
                    moretoread = false;
                }
            }

            Row row = rs.one();
            System.out.println(row.getString("release_version"));
            String collectionID = "particles.dense";

            //ResultSet r1 = session.execute("SELECT name FROM particles.collections WHERE CollectionID = 0");

            //session.execute("UPDATE particles.collections SET description = \'"
             //       + "hello" + "\' WHERE CollectionID =  0  AND name = \'"+ r1+"\'");

            //ResultSet rss = session.execute("SELECT description FROM particles.collections WHERE CollectionId = 0");

            String id = particleCollectionName;

            ResultSet rss = session.execute("SELECT * FROM particles.collections WHERE collectionID = \'" + id+"\'");

            System.out.println(rss.all());



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