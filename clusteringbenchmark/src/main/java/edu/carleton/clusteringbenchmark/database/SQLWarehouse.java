package edu.carleton.clusteringbenchmark.database;

import edu.carleton.clusteringbenchmark.collection.Collection;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class SQLWarehouse implements InfoWarehouse {

    String url = "jdbc:jtds:sqlserver://127.0.0.1;instance=SQLEXPRESS01;DatabaseName=enchilada_benchmark";
    String driver = "net.sourceforge.jtds.jdbc.Driver";
    String userName = "SpASMS";
    String password = "finally";

    private ArrayList<Integer> alteredCollections;
    private PrintWriter bulkInsertFileWriter;
    private File bulkInsertFile;

    //TODO
    //Unsure what atoms would need to be deleted??
    private StringBuilder atomIDsToDelete;
    //atomIDsToDelete = new StringBuilder("");
    //atomIDsToDelete.append(atomID + ",");
    /**
     * The collection you are dividing
     */
    protected Collection collection;

    //TODO
    //Move this to dataload? And apply to any atoms inserted
    /**
     * Adds a new atom to InternalAtomOrder, assuming the atom is the next one
     * in its collection.
     * @param	atomID	- the atom to add
     * @param	collectionID - the collection to which the atom is added
     */
    private void addInternalAtom(int atomID, int collectionID){
        //IF OBJECT_ID('InternalAtomOrder', 'U') IS NULL CREATE TABLE InternalAtomOrder (atomID INT, collectionID INT);
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            Statement stmt = conn.createStatement();

            String query = "INSERT INTO InternalAtomOrder " +
                    "VALUES (" + atomID +", "+ collectionID+ ")";
            //System.out.println(query);//debugging
            stmt.execute(query);
            stmt.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                System.err.println("Exception creating empty collection:");
                e.printStackTrace();
            }
        }

    }

    //InfoWarehouse methods

    // simple, gets a collection (dataset) from db with associated metadata
    public Collection getCollection(int collectionID){
        Connection conn = null;

        Collection collection;
        boolean isPresent = false;
        String datatype = "";
        Statement stmt;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT CollectionID \nFROM Collections \nWHERE CollectionID = "+collectionID);
            while (rs.next()) {
                if (rs.getInt(1) == collectionID) {
                    isPresent = true;
                    break;
                }
            }

            if (isPresent) {
                rs = stmt.executeQuery("SELECT Datatype FROM Collections WHERE CollectionID = " + collectionID);
                rs.next();
                datatype = rs.getString(1);
            }
            else {
                //ErrorLogger.writeExceptionToLogAndPrompt(getName(),"Error retrieving collection for collectionID "+collectionID);
                System.err.println("collectionID not created yet!!");
                return null;
            }
            stmt.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                System.err.println("Error creating collection");
                e.printStackTrace();

            }
        }
        return new Collection(datatype,collectionID,this);
    }

    //simple, gets the count of atoms in DB using SQL command
    public int getCollectionSize(int collectionID){
        Connection conn = null;
        //PreparedStatement pst;

        int returnThis = -1;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(AtomID) FROM InternalAtomOrder WHERE CollectionID = " + collectionID);
            boolean test = rs.next();
            assert (test): "error getting atomID count.";
            returnThis = rs.getInt(1);
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                System.err.println("Error selecting the size of the table");
                e.printStackTrace();
                return -1;
            }
        }
        return returnThis;
    }

    // simple, creates a new empty collection in the db
    public int createEmptyCollection( String datatype,
                                      int parent,
                                      String name,
                                      String comment,
                                      String description){
        Connection conn = null;

        //TODO
        //IF OBJECT_ID('Metadata', 'U') IS NULL CREATE TABLE Metadata (DataType VARCHAR(8000), ColumnName VARCHAR(8000), ColumnType VARCHAR(8000), PrimaryKey BIT, TableID INT, ColumnOrder INT);
        //IF OBJECT_ID('Collections', 'U') IS NULL CREATE TABLE Collections (CollectionID INT, Name VARCHAR(8000), Comment VARCHAR(8000), Description VARCHAR(8000), Datatype VARCHAR(8000));
        //IF OBJJECT_ID('CollectionRelationships', 'U') IS NULL CREATE TABLE CollectionRelationships (ParentID INT, ChildID INT);
        //INSERT INTO Metadata VALUES

        if (description.length() == 0)
            description = "Name: " + name + " Comment: " + comment;

        int nextID = -1;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            Statement stmt = conn.createStatement();
            /*
            //Assert datatype is valid.  (Only valid options given in GUI, but
            //still want to double-check.)
            ResultSet rs = stmt.executeQuery("SELECT DISTINCT Datatype \n" +
                    "FROM Metadata \n" +
                    "WHERE Datatype = '" + datatype + "'");
            assert(rs.next()) : "The datatype of the new collection doesn't exist.";
            */

            // Get next CollectionID:
            ResultSet rs = stmt.executeQuery("SELECT MAX(CollectionID)\n" +
                    "FROM Collections\n");
            rs.next();
            nextID = rs.getInt(1) + 1;

            stmt.executeUpdate("INSERT INTO Collections\n" +
                    "(CollectionID, Name, Comment, Description, Datatype)\n" +
                    "VALUES (" +
                    Integer.toString(nextID) +
                    ", '" + removeReservedCharacters(name) + "', '"
                    + removeReservedCharacters(comment) + "', '" +
                    removeReservedCharacters(description) + "', '" + datatype + "')");
            stmt.executeUpdate("INSERT INTO CollectionRelationships\n" +
                    "(ParentID, ChildID)\n" +
                    "VALUES (" + Integer.toString(parent) +
                    ", " + Integer.toString(nextID) + ")");


            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
                //rs.close();
            } catch (SQLException e) {
                System.err.println("Exception creating empty collection:");
                e.printStackTrace();
                return -1;
            }
        }

        return nextID;
    }

    /**
     * Replaces characters which would interrupt SQL Server's
     * parsing of a string with their escape equivalents
     * @param s String to modify
     * @return The same string except in an acceptable string for
     * SQL Server
     */
    private String removeReservedCharacters(String s)
    {
        //Replace additional characters as necessary
        s = s.replace("'","''");
        //s = s.replace('"', ' ');
        return s;
    }

    // initalizes a bulk insert using temporary files
    public void bulkInsertInit() throws Exception{
        if (bulkInsertFile != null && bulkInsertFile.exists()) {
            bulkInsertFile.delete();
        }
        bulkInsertFile = null;
        bulkInsertFileWriter = null;
        alteredCollections = new ArrayList<Integer>();
        try {
            //bulkInsertFile = File.createTempFile("bulkfile", ".txt");
            bulkInsertFile =  new File("Temp"+File.separator+"bulkfile"+".txt");
            bulkInsertFile.deleteOnExit();
            bulkInsertFileWriter = new PrintWriter(new FileWriter(bulkInsertFile));
        } catch (IOException e) {
            System.err.println("Trouble creating " + bulkInsertFile.getAbsolutePath() + "");
            e.printStackTrace();
        }
    }

    // writes an atom to bulk-insert to the temp file
    public void bulkInsertAtom(int newChildID, int newHostID) throws Exception {
            if (bulkInsertFileWriter == null || bulkInsertFile == null) {
                throw new Exception("Must initialize bulk insert first!");
            }
            if (!alteredCollections.contains(new Integer(newHostID)))
                alteredCollections.add(new Integer(newHostID));

            //alteredCollections.add(parentID);
            bulkInsertFileWriter.println(newHostID+ "," + newChildID);

    }

    // executes a bulk insert, reading from temp files
    public void bulkInsertExecute() throws Exception{
        //IF OBJECT_ID('AtomMembership', 'U') IS NULL CREATE TABLE AtomMembership (atomID INT, collectionID INT);
        Connection conn = null;
        try {
            long time = System.currentTimeMillis();

            if(bulkInsertFileWriter==null || bulkInsertFile == null){
                try {
                    throw new Exception("Must initialize bulk insert first!");
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            bulkInsertFileWriter.close();
            dropTempTable();
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            Statement stmt = conn.createStatement();
            StringBuilder sql = new StringBuilder();
            File tempFile = null;
            sql.append("CREATE TABLE #temp (CollectionID INT, AtomID INT);\n");
            sql.append("BULK INSERT #temp" +
                    " FROM '"+bulkInsertFile.getAbsolutePath()+"' " +
                    "WITH (FIELDTERMINATOR=',');\n");
            sql.append("INSERT INTO AtomMembership (CollectionID, AtomID)" +
                    " SELECT CollectionID, AtomID " +
                    "FROM #temp;\n");
            sql.append("INSERT INTO InternalAtomOrder (CollectionID, AtomID)" +
                    " SELECT CollectionID, AtomID " +
                    "FROM #temp;\n");
            System.out.println(sql.toString());
            stmt.execute(sql.toString());
            stmt.close();

            System.out.println("Time: " + (System.currentTimeMillis()-time));
            System.out.println("done inserting now time for altering collections");

            /*
            time = System.currentTimeMillis();
            System.out.println("alteredCollections.size() " + alteredCollections.size());
            for (int i = 0; i < alteredCollections.size(); i++)
                updateInternalAtomOrder(getCollection(alteredCollections.get(i)));

            //when the parents of all the altered collections are the same
            //don't need to update the parent FOR EACH subcollection, just at end
            ArrayList<Collection> parents = new ArrayList<Collection>();
            Collection temp;
            for (int i = 0; i < alteredCollections.size(); i++){
                temp = getCollection(alteredCollections.get(i)).getParentCollection();
                if (! parents.contains(temp))
                    parents.add(temp);
            }
            //only update each distinct parent once
            for (int i=0; i<parents.size(); i++)
                updateAncestors(parents.get(i));
            if (batchStatement != null) {
                batchStatement.close();
            }
            System.out.println("done with updating, time = " + (System.currentTimeMillis()-time));
            */
        } catch (SQLException e) {
            //ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception executing batch atom adds and inserts.");
            System.out.println("Exception executing batch atom adds " +
                    "and inserts");
            e.printStackTrace();
        }
    }

    /**
     * @author steinbel
     * @author turetske
     * Drops the temporary table #temp from the database SpASMSdb.
     */
    private void dropTempTable(){
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("IF (OBJECT_ID('tempdb..#TEMP') " +
                    "IS NOT NULL)\n" +
                    " DROP TABLE #TEMP\n");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    // not from enchilada, this is meant to replace the putInSubCollectionBulkExecute method in CollectionDivider so that it doesn't contain SQL
    public void bulkDelete() throws Exception{
        Connection conn = null;
        System.out.println("Done with INSERTS, about to do DELETE");

        //build a table for deletes
        //drop the table in case it already (mistakenly) exists
        System.out.println("Creating deletion table...");
        //Connection dbCon = db.getCon();
        Statement delStmt = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            delStmt = conn.createStatement();
            delStmt.executeUpdate("IF (OBJECT_ID('stuffToDelete') IS NOT NULL)\n" +
                    "DROP TABLE stuffToDelete");
            delStmt.executeUpdate("CREATE TABLE stuffToDelete(atoms int)");
        }
        catch (Exception e) {
            System.out.println("Error creating table stuffToDelete in database.");
        }

        //create tempfile used to bulk insert to stuffToDelete
        //tempfile goes in same place as other temp files
        System.out.println("Creating tempdelete.data file...");
//		String tempdir = "";
        File temp = null;
        PrintWriter pw = null;
        try {
            temp = File.createTempFile("tempdelete", "data");
            pw = new PrintWriter(temp);
        }
        catch (Exception e) {
            System.out.println("Error Creating tempDelete file.");
        }

        //put stuff in the tempfile
        System.out.println("Putting stuff in tempdelete.data...");
        String atomIDsToDel = atomIDsToDelete.toString();
        Scanner atomIDs = new Scanner(atomIDsToDel).useDelimiter(",");
        while (atomIDs.hasNext()) {
            pw.println(atomIDs.next());
        }
        pw.println();
        pw.close();

        //execute a statement to bulk insert into stuffToDelete
        System.out.println("Putting stuff in deletion table...");
        try {
            delStmt.executeUpdate("BULK INSERT stuffToDelete\n" +
                    "FROM '" + temp.getAbsoluteFile() + "'");
        }
        catch (Exception e) {
            System.out.println("Error inserting into stuffToDelete.");
        }

        //delete the temp file
        System.out.println("Deleting tempdelete.data...");
        temp.delete();

        //finally, delete what's in stuffToDelete from AtomMembership
        //and drop the stuffToDelete table
        System.out.println("Finally, deleting from AtomMembership...");
        String deletionquery = "DELETE FROM AtomMembership\n" +
                "WHERE CollectionID = " + collection.getCollectionID() +
                "\n" + "AND AtomID IN \n" +
                "(SELECT atoms FROM stuffToDelete)";
        System.out.println("Query:");
        System.out.println(deletionquery);
        try {
            delStmt.executeUpdate(deletionquery);
            delStmt.executeUpdate("DROP TABLE stuffToDelete");
        }
        catch (Exception e) {
            System.out.println("Error deleting from AtomMembership");
        }
        System.out.println("...and dropping deletion table.");

        System.out.println("Done with DELETEs.");

    }

    //TODO
    //What is this
    // simple, just returns a new AtomInfoOnlyCursor object on collection
    public CollectionCursor getAtomInfoOnlyCursor(Collection collection){
        // return new AtomInfoOnlyCursor(collection);
    }


    //AtomInfoOnly cursor.  Returns atom info.
    /*
    private class AtomInfoOnlyCursor
            implements CollectionCursor {
        protected InstancedResultSet irs;
        protected ResultSet partInfRS = null;
        protected Statement stmt = null;
        Collection collection;

        public AtomInfoOnlyCursor(Collection col) {
            super();
            assert(col.getDatatype().equals("ATOFMS")) : "Wrong datatype for cursor.";
            collection = col;
            try {
                stmt = con.createStatement();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            String q = "SELECT "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID, OrigFilename, ScatDelay," +
                    " LaserPower, [Time], Size FROM "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+", InternalAtomOrder WHERE" +
                    " InternalAtomOrder.CollectionID = "+collection.getCollectionID() +
                    " AND "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID = InternalAtomOrder.AtomID";
            System.out.println(q);
            try {
                partInfRS = stmt.executeQuery(q);

            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        public void reset()
        {
            try {
                partInfRS.close();
                partInfRS = stmt.executeQuery("SELECT "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID, OrigFilename, ScatDelay," +
                        " LaserPower, [Time], Size FROM "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+", InternalAtomOrder WHERE" +
                        " InternalAtomOrder.CollectionID = "+collection.getCollectionID() +
                        " AND "+getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype())+".AtomID = InternalAtomOrder.AtomID");
            } catch (SQLException e) {
                //ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a AtomInfoOnly cursor.");
                System.err.println("SQL Error resetting " +
                        "cursor: ");
                e.printStackTrace();
            }
        }

        public boolean next() {
            try {
                return partInfRS.next();
            } catch (SQLException e) {
                //ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a AtomInfoOnly cursor.");
                System.err.println("Error checking the " +
                        "bounds of " +
                        "the ResultSet.");
                e.printStackTrace();
                return false;
            }
        }

        public ParticleInfo getCurrent() {
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
                ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a AtomInfoOnly cursor.");
                System.err.println("Error retrieving the " +
                        "next row");
                e.printStackTrace();
                return null;
            }
        }

        public void close() {
            try {
                stmt.close();
                partInfRS.close();
            } catch (SQLException e) {
                //ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a AtomInfoOnly cursor.");
                e.printStackTrace();
            }
        }

        public ParticleInfo get(int i)
                throws NoSuchMethodException {
            throw new NoSuchMethodException("Not implemented in disk based cursors.");
        }

        public BinnedPeakList getPeakListfromAtomID(int atomID) {
            BinnedPeakList peakList = new BinnedPeakList(new Normalizer());
            try {
                ResultSet rs =
                        con.createStatement().executeQuery(
                                "SELECT PeakLocation,PeakArea\n" +
                                        "FROM " + getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype()) + "\n" +
                                        "WHERE AtomID = " + atomID);
                while(rs.next()) {
                    peakList.add(
                            rs.getFloat(1),
                            rs.getInt(2));
                }
                rs.close();
                return peakList;
            } catch (SQLException e) {
                //ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception retrieving data through a AtomInfoOnly cursor.");
                System.err.println("Error retrieving peak " +
                        "list.");
                e.printStackTrace();
                return null;
            }
        }
    }
    */
    /**
     * gets the dynamic table name according to the datatype and the table
     * type.
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



    // simple, just gets the datatype (ATOFMS, AMs, etc.) of a collection
    public String getCollectionDatatype(int collectionId){
        Connection conn = null;
        String datatype = "";
        try{
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT Datatype \n" +
                            "FROM Collections \n" +
                            "WHERE CollectionID = " + collectionId);
            rs.next();
            datatype = rs.getString("Datatype");
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                System.err.println("Error retrieving collection datatype");
                e.printStackTrace();
            }
        }
        return datatype;
    }

    // gets the names of columns for a given datatype, this is metadata
    public ArrayList<ArrayList<String>> getColNamesAndTypes(String datatype, DynamicTable table){
        Connection conn = null;
        ArrayList<ArrayList<String>> colNames = new ArrayList<ArrayList<String>>();
        ArrayList<String> temp;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ColumnName, ColumnType FROM MetaData " +
                    "WHERE Datatype = '" + datatype + "' " +
                    "AND TableID = " + table.ordinal() + " ORDER BY ColumnOrder");

            while (rs.next()) {
                temp = new ArrayList<String>();
                temp.add(rs.getString(1).substring(1,rs.getString(1).length()-1));
                temp.add(rs.getString(2));
                colNames.add(temp);
            }
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                System.err.println("Error retrieving column names");
                e.printStackTrace();
            }
        }
        return colNames;
    }

    //TODO
    //What is this
    // inserts a particle's sparse (peaks) and dense info into the db
    public int insertParticle(String dense, ArrayList<String> sparse,Collection collection, int nextID){
        /*
        try {
            Statement stmt = con.createStatement();
            //System.out.println("Adding batches");
            BatchExecuter sql = getBatchExecuter(stmt);
            sql.append("INSERT INTO " + getDynamicTableName(DynamicTable.AtomInfoDense,collection.getDatatype()) + " VALUES (" +
                    nextID + ", " + dense + ")");
            sql.append("INSERT INTO AtomMembership " +
                    "(CollectionID, AtomID) " +
                    "VALUES (" +
                    collection.getCollectionID() + ", " +
                    nextID + ")\n");
            sql.append("INSERT INTO DataSetMembers " +
                    "(OrigDataSetID, AtomID) " +
                    "VALUES (" +
                    datasetID + ", " +
                    nextID + ")\n");

            String tableName = getDynamicTableName(DynamicTable.AtomInfoSparse,collection.getDatatype());

            Inserter bi = getBulkInserter(sql, tableName);
            for (int j = 0; j < sparse.size(); ++j) {
                bi.append(nextID + "," + sparse.get(j));
            }
            bi.close();

            sql.execute();

            stmt.close();
            bi.cleanUp();
        } catch (SQLException e) {
            //ErrorLogger.writeExceptionToLogAndPrompt(getName(),"SQL Exception inserting atom.  Please check incoming data for correct format.");
            System.err.println("Exception inserting particle.");
            e.printStackTrace();

            return -1;
        }
        if (!importing)
            updateInternalAtomOrder(collection);
        else
            addInternalAtom(nextID, collection.getCollectionID());
        return nextID;
        */
    }

    // gets the max id of any particle, this is the next id I guess?
    public int getNextID(){
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery("SELECT MAX(AtomID) FROM AtomMembership");

            int nextID;
            if(rs.next())
                nextID = rs.getInt(1) + 1;
            else
                nextID = 0;
            stmt.close();
            return nextID;

        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                System.err.println("Exception finding max atom id.");
                e.printStackTrace();
            }
        }

        return -1;
    }

    // simple, adds a particle to the Centeratoms table
    public boolean addCenterAtom(int centerAtomID, int centerCollID){
        Connection conn = null;
        boolean success = false;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            Statement stmt = conn.createStatement();
            String query = "INSERT INTO CenterAtoms \n" +
                    "VALUES ("+ centerAtomID + ", " + centerCollID + ")";
            stmt.execute(query);
            stmt.close();
            success = true;
        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                System.err.println("Exception finding max atom id.");
                e.printStackTrace();
            }
        }
        return success;
    }

    // simple, updates a collection's description
    public boolean setCollectionDescription(Collection collection,
                                            String description){
        Connection conn = null;
        description = removeReservedCharacters(description);
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(
                    "UPDATE Collections\n" +
                            "SET Description = '" + description + "'\n" +
                            "WHERE CollectionID = " + collection.getCollectionID());
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                System.err.println("Error updating collection. ");
                e.printStackTrace();
            }
        }
        return true;
    }

    // simple, gets the name of a collection
    public String getCollectionName(int collectionID){
        String name = "";
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT Name\n" +
                    "FROM Collections\n" +
                    "WHERE CollectionID = " +
                    collectionID);
            rs.next();
            name = rs.getString("Name");
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                System.err.println("Error grabbing the collection name. ");
                e.printStackTrace();
            }
        }
        return name;
    }

    // returns a string naming the database system implementing this interface
    public String dbname(){
        return "SQLExpress";
    }
}
