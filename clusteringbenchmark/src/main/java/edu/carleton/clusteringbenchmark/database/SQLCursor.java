package edu.carleton.clusteringbenchmark.database;

import edu.carleton.clusteringbenchmark.ATOFMS.ParticleInfo;
import edu.carleton.clusteringbenchmark.collection.Collection;
import edu.carleton.clusteringbenchmark.analysis.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import edu.carleton.clusteringbenchmark.analysis.BinnedPeakList;
import edu.carleton.clusteringbenchmark.atom.ATOFMSAtomFromDB;

public class SQLCursor implements CollectionCursor{
    String url = "jdbc:jtds:sqlserver://127.0.0.1;instance=SQLEXPRESS01;DatabaseName=enchilada_benchmark";
    String driver = "net.sourceforge.jtds.jdbc.Driver";
    String userName = "SpASMS";
    String password = "finally";

    protected ResultSet partInfRS = null;
    protected Statement stmt = null;
    Connection conn = null;
    Collection collection;

    public SQLCursor(Collection col) {
        //super();
        assert (col.getDatatype().equals("ATOFMS")) : "Wrong datatype for cursor.";
        collection = col;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            stmt = conn.createStatement();
            String q = "SELECT "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID, OrigFilename, ScatDelay," +
                    " LaserPower, [Time], Size FROM "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+", AtomMembership WHERE" +
                    " AtomMembership.CollectionID = "+collection.getCollectionID() +
                    " AND "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID = AtomMembership.AtomID";

            //System.out.println(q);
            partInfRS = stmt.executeQuery(q);
            //stmt.close();
        }  catch (Exception e) {
            e.printStackTrace();
        }
        /*finally {
            try {
                //conn.close();

            } catch (SQLException e) {
                System.err.println("Exception creating cursor");
                e.printStackTrace();
            }

        }
*/
    }

    /*
    if first pass, goes through and keeps calling getnext from super
    and saves them into an array. otherwise, return position < storedInfo.size().
	This returns a boolean to show if it is successful in going to the next thing
	*/
    public boolean next(){
        try {
            return partInfRS.next();
        } catch (SQLException e) {
            System.err.println("Error checking the " +
                    "bounds of " +
                    "the ResultSet.");
            e.printStackTrace();
            return false;
        }
    }

    /*
	calls get method.
    */
    public ParticleInfo getCurrent(){
        try {
            ParticleInfo particleInfo = new ParticleInfo();
            ATOFMSAtomFromDB aInfo =  new ATOFMSAtomFromDB(
                    partInfRS.getInt(1),
                    partInfRS.getString(2),
                    partInfRS.getInt(3),
                    partInfRS.getFloat(4),
                    new Date(partInfRS.getTimestamp(5).
                            getTime()),
                    partInfRS.getFloat(6));
            SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
            aInfo.setDateString(dateformat.format(partInfRS.getTimestamp(5).getTime()));
            particleInfo.setParticleInfo(aInfo);
            particleInfo.setID(particleInfo.getATOFMSParticleInfo().getAtomID());
            particleInfo.setBinnedList(getPeakListfromAtomID(partInfRS.getInt(1)));
            return particleInfo;
        } catch (SQLException e) {
            System.err.println("Error retrieving the " +
                    "next row");
            e.printStackTrace();
            return null;
        }
    }

    public BinnedPeakList getPeakListfromAtomID(int atomID) {
        BinnedPeakList peakList = new BinnedPeakList(new Normalizer());
        //Connection con = null;
        try {
            ResultSet rs =
                    conn.createStatement().executeQuery(
                            "SELECT PeakLocation,PeakArea\n" +
                                    "FROM " + getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype()) + "\n" +
                                    "WHERE AtomID = " + atomID);
            while(rs.next()) {
                peakList.add(rs.getInt(1), rs.getFloat(2)
                        //rs.getInt(2),
                        //rs.getFloat(1)
                        );
                //System.out.println("Printing information about the peak list");
                //System.out.println(rs.getInt(2));
                //System.out.println(rs.getFloat(1));
            }
            rs.close();
            return peakList;
        } catch (SQLException e) {
            System.err.println("Error retrieving peak " +
                    "list.");
            e.printStackTrace();
            return null;
        }
    }
    /*
    drops temprand table and calls super.close
    */
    public void close(){
        try {
            stmt.close();
            partInfRS.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /*
	calls rs.close
	and then rs = getAllAtomsRS(collection)
	this selects every atomID from InternalAtomOrder where
	collectionID = collection.getcollectionid()
    */
    public void reset(){
        try {
            partInfRS.close();
            partInfRS = stmt.executeQuery("SELECT "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID, OrigFilename, ScatDelay," +
                    " LaserPower, [Time], Size FROM "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+", AtomMembership WHERE" +
                    " AtomMembership.CollectionID = "+collection.getCollectionID() +
                    " AND "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID = AtomMembership.AtomID");
        } catch (SQLException e) {
            System.err.println("SQL Error resetting " +
                    "cursor: ");
            e.printStackTrace();
        }
    }
    /**
     * gets the dynamic table name according to the datatype and the table
     * type.
     *
     * @param table
     * @param datatype
     * @return table name.
     */
    public String getDynamicTableName(DynamicTable table, String datatype) {
        assert (!datatype.equals("root")) : "root isn't a datatype.";

        if (table == DynamicTable.DataSetInfo)
            return datatype + "DataSetInfo";
        if (table == DynamicTable.AtomInfoDense)
            return datatype + "AtomInfoDense";
        if (table == DynamicTable.AtomInfoSparse)
            return datatype + "AtomInfoSparse";
        else return null;
    }

}
