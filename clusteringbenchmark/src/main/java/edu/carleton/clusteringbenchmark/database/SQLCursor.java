package edu.carleton.clusteringbenchmark.database;

import edu.carleton.clusteringbenchmark.ATOFMS.ParticleInfo;
import edu.carleton.clusteringbenchmark.collection.Collection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import edu.carleton.clusteringbenchmark.analysis.BinnedPeakList;
import edu.carleton.clusteringbenchmark.atom.ATOFMSAtomFromDB;

public class SQLCursor implements CollectionCursor{
    protected ResultSet partInfRS = null;
    protected Statement stmt = null;
    Collection collection;
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
            particleInfo.setParticleInfo(
                    new ATOFMSAtomFromDB(
                            partInfRS.getInt(1),
                            partInfRS.getString(2),
                            partInfRS.getInt(3),
                            partInfRS.getFloat(4),
                            new Date(partInfRS.getTimestamp(5).
                                    getTime()),
                            partInfRS.getFloat(6)));
            particleInfo.setID(particleInfo.getATOFMSParticleInfo().getAtomID());
            return particleInfo;
        } catch (SQLException e) {
            System.err.println("Error retrieving the " +
                    "next row");
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
                    " LaserPower, [Time], Size FROM "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+", InternalAtomOrder WHERE" +
                    " InternalAtomOrder.CollectionID = "+collection.getCollectionID() +
                    " AND "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID = InternalAtomOrder.AtomID");
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
