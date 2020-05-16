package edu.carleton.clusteringbenchmark.database;

import edu.carleton.clusteringbenchmark.ATOFMS.ParticleInfo;
import edu.carleton.clusteringbenchmark.collection.Collection;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;

public class PostgreSQLWarehouse implements InfoWarehouse{
    String url = "jdbc:postgresql://localhost/enchilada_benchmark";
    //String driver = "net.sourceforge.jtds.jdbc.Driver";
    String userName = "postgres";
    String password = "finally";

    private ArrayList<Integer> alteredCollections;
    private PrintWriter bulkInsertFileWriter;
    private File bulkInsertFile;

    private StringBuilder atomIDsToDelete;

    /**
     * The collection you are dividing
     */
    protected Collection collection;

    public Connection connect(String url) {
        Connection conn = null;

        try {
            conn = DriverManager.getConnection(url, userName, password);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return conn;
    }

    /**
     * Adds a new atom to InternalAtomOrder, assuming the atom is the next one
     * in its collection.
     * @param	atomID	- the atom to add
     * @param	collectionID - the collection to which the atom is added
     */
    private void addInternalAtom(int atomID, int collectionID){
        //Connection conn = null;
        try (Connection conn = connect(url)) {
            Statement stmt = conn.createStatement();
            String query = "INSERT INTO AtomMembership " +
                    "VALUES (" + atomID +", "+ collectionID+ ")";
            stmt.execute(query);
            stmt.close();
        }
        catch (SQLException ex){
            System.out.println(ex.getMessage());
        }
    }

    //InfoWarehouse methods

    // simple, gets a collection (dataset) from db with associated metadata
    public Collection getCollection(int collectionID){
        //Connection conn = null;
        Collection collection;
        boolean isPresent = false;
        String datatype = "";
        Statement stmt;

        try (Connection conn = connect(url)) {
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT CollectionID FROM Collections WHERE CollectionID = "+collectionID);
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
                System.err.println("collectionID not created yet!!");
                return null;
            }
            stmt.close();
        }
        catch (SQLException ex){
            System.out.println(ex.getMessage());
        }
        return new Collection(datatype,collectionID,this);
    }

    //simple, gets the count of atoms in DB using SQL command
    public int getCollectionSize(int collectionID){
        //Connection conn = null;
        //PreparedStatement pst;
        int returnThis = -1;
        try (Connection conn = connect(url)) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(AtomID) FROM AtomMembership WHERE CollectionID = " + collectionID);
            boolean test = rs.next();
            assert (test): "error getting atomID count.";
            returnThis = rs.getInt(1);
            stmt.close();
        }
        catch (SQLException ex){
            System.out.println(ex.getMessage());
            return -1;
        }
        return returnThis;
    }

    // simple, creates a new empty collection in the db
    public int createEmptyCollection( String datatype,
                                      int parent,
                                      String name,
                                      String comment,
                                      String description){
        //Connection conn = null;
        if (description.length() == 0)
            description = "Name: " + name + " Comment: " + comment;

        int nextID = -1;
        try (Connection conn = connect(url)) {
            Statement stmt = conn.createStatement();
            // Get next CollectionID:
            ResultSet rs = stmt.executeQuery("SELECT MAX(CollectionID)\n" +
                    "FROM Collections\n");
            rs.next();
            nextID = rs.getInt(1) + 1;
            //If there are no collections, the nextID is 0
            ResultSet frs = stmt.executeQuery("SELECT * FROM Collections LIMIT 1");
            if(!frs.next()){
                nextID = 0;
            }

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
        }
        catch (SQLException ex){
            System.out.println(ex.getMessage());
            return -1;
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

        bulkInsertFileWriter.println(newHostID+ "," + newChildID);

    }

    // executes a bulk insert, reading from temp files
    public void bulkInsertExecute() throws Exception{
        //IF OBJECT_ID('AtomMembership', 'U') IS NULL CREATE TABLE AtomMembership (atomID INT, collectionID INT);
        //Connection conn = null;

        try (Connection conn = connect(url)) {
            Statement stmt = conn.createStatement();
            long time = System.currentTimeMillis();

            if(bulkInsertFileWriter==null || bulkInsertFile == null){
                try {
                    throw new Exception("Must initialize bulk insert first!");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            bulkInsertFileWriter.close();
            dropTempTable();
            StringBuilder sql = new StringBuilder();
            sql.append("CREATE TABLE \"#temp\" (CollectionID INT, AtomID INT);\n");
            /*
            sql.append("BULK INSERT \"#temp\"" +
                    " FROM '"+bulkInsertFile.getAbsolutePath()+"' " +
                    "WITH (FIELDTERMINATOR=',');\n");
            */
            sql.append("COPY \"#temp\" FROM '" + bulkInsertFile.getAbsolutePath() + "' WITH (DELIMITER ',');\n");
            sql.append("INSERT INTO AtomMembership (CollectionID, AtomID)" +
                    " SELECT CollectionID, AtomID " +
                    "FROM \"#temp\";\n");

            //System.out.println(sql.toString());
            stmt.execute(sql.toString());
            stmt.close();

            //System.out.println("Time: " + (System.currentTimeMillis()-time));
            //System.out.println("done inserting now time for altering collections");
        }
        catch (SQLException ex){
            System.out.println(ex.getMessage());
        }
    }

    /**
     * @author steinbel
     * @author turetske
     * Drops the temporary table #temp from the database SpASMSdb.
     */
    private void dropTempTable(){
        //Connection conn = null;
        try (Connection conn = connect(url)) {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("DROP TABLE IF EXISTS \"#TEMP\"");
            stmt = conn.createStatement();
            stmt.executeUpdate("DROP TABLE IF EXISTS \"#temp\"");
        }
        catch (SQLException ex){
            System.out.println(ex.getMessage());
        }

    }

    // not from enchilada, this is meant to replace the putInSubCollectionBulkExecute method in CollectionDivider so that it doesn't contain SQL
    public void bulkDelete(StringBuilder atomIDsToDelete, Collection collection) throws Exception{
        Connection conn = null;
        //System.out.println("Done with INSERTS, about to do DELETE");

        //build a table for deletes
        //drop the table in case it already (mistakenly) exists
        //System.out.println("Creating deletion table...");
        Statement stmt = null;

        try  {
            conn = connect(url);
            stmt = conn.createStatement();
            stmt.executeUpdate("DROP TABLE IF EXISTS stuffToDelete");
            stmt = conn.createStatement();
            stmt.executeUpdate("CREATE TABLE stuffToDelete(atoms int)");
            stmt = conn.createStatement();
        }
        catch (SQLException ex){
            System.out.println("Error creating table stuffToDelete in database.");
            System.out.println(ex.getMessage());
        }

        //create tempfile used to bulk insert to stuffToDelete
        //tempfile goes in same place as other temp files
        //System.out.println("Creating tempdelete.data file...");
        File temp = null;
        PrintWriter pw = null;
        try {
            temp = new File("Temp"+File.separator+"tempDelete"+".txt");
            temp.deleteOnExit();
            pw = new PrintWriter(temp);
        }
        catch (Exception e) {
            System.out.println("Error Creating tempDelete file.");
        }

        //put stuff in the tempfile
        //System.out.println("Putting stuff in tempdelete.data...");
        String atomIDsToDel = atomIDsToDelete.toString();
        Scanner atomIDs = new Scanner(atomIDsToDel).useDelimiter(",");
        while (atomIDs.hasNext()) {
            pw.println(atomIDs.next());
        }
        //pw.println();
        pw.close();

        //execute a statement to bulk insert into stuffToDelete
        //System.out.println("Putting stuff in deletion table...");
        try {
            stmt.executeUpdate("COPY stuffToDelete FROM '" + temp.getAbsolutePath() + "' WITH (DELIMITER ',')");
        }
        catch (Exception e) {
            System.out.println("Error inserting into stuffToDelete.");
            System.out.println(e);
        }
        //ResultSet rs = stmt.executeQuery("SELECT COUNT (*) FROM stuffToDelete");

        //System.out.println("Are there atoms in stuff to delete?");
        //System.out.println(rs.next());

        //delete the temp file
        //System.out.println("Deleting tempdelete.data...");
        //temp.delete();

        //finally, delete what's in stuffToDelete from AtomMembership
        //and drop the stuffToDelete table
        //System.out.println("Finally, deleting from AtomMembership...");
        String deletionquery = "DELETE FROM AtomMembership\n" +
                "WHERE CollectionID = " + collection.getCollectionID() +
                "\n" + "AND AtomID IN \n" +
                "(SELECT atoms FROM stuffToDelete)";
        //System.out.println("Query:");
        //System.out.println(deletionquery);
        try {
            stmt.executeUpdate(deletionquery);
            stmt.executeUpdate("DROP TABLE stuffToDelete");
        }
        catch (Exception e) {
            System.out.println("Error deleting from AtomMembership");
        }
        //System.out.println("...and dropping deletion table.");

        //System.out.println("Done with DELETEs.");

    }


    // simple, just returns a new AtomInfoOnlyCursor object on collection
    public CollectionCursor getAtomInfoOnlyCursor(Collection collection){
        return new PostgreSQLCursor(collection);
    }


    // simple, just gets the datatype (ATOFMS, AMs, etc.) of a collection
    public String getCollectionDatatype(int collectionId){
        //Connection conn = null;
        String datatype = "";
        try (Connection conn = connect(url)) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT Datatype \n" +
                            "FROM Collections \n" +
                            "WHERE CollectionID = " + collectionId);
            rs.next();
            datatype = rs.getString("Datatype");
            stmt.close();
        }
        catch (SQLException ex){
            System.out.println(ex.getMessage());
        }
        return datatype;
    }

    // gets the names of columns for a given datatype, this is metadata
    public ArrayList<ArrayList<String>> getColNamesAndTypes(String datatype, DynamicTable table){
        //Connection conn = null;
        ArrayList<ArrayList<String>> colNames = new ArrayList<ArrayList<String>>();
        ArrayList<String> temp;

        try (Connection conn = connect(url)) {
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

        }
        catch (SQLException ex){
            System.out.println("Error creating table stuffToDelete in database.");
            System.out.println(ex.getMessage());
        }
        return colNames;
    }

    // inserts a particle's sparse (peaks) and dense info into the db
    public int insertParticle(String dense, ArrayList<String> sparse,Collection collection, int nextID){
        //Connection conn = null;
        PreparedStatement pst;
        try (Connection conn = connect(url)) {
            //Statement stmt = conn.createStatement();
            int currentAtomID = nextID;

            String[] denseparams = dense.split(", ");
            //Put data in tables
            //Put data into ATOFMSAtomInfoDense

            String strDate = denseparams[0];
            String laserpower = denseparams[1];
            String size = denseparams[2];
            String scatDelay = denseparams[3];
            String name = denseparams[4];

            //Check for extra quotes
            if(name.contains("\'")){
                name = name.replace("\'","");
            }
            //Check that date exists
            if(strDate.length() < 10){
                strDate = "2000-01-01 00:00:00";
            }

            //System.out.println("INSERT INTO ATOFMSAtomInfoDense VALUES ("+ Integer.toString(currentAtomID) + ", '" + strDate + "', " + laserpower + ", " + size + ", " + scatDelay + ", '" + name +"')");
            pst = conn.prepareStatement("INSERT INTO ATOFMSAtomInfoDense VALUES ("+ Integer.toString(currentAtomID) + ", '" + strDate + "', " + laserpower + ", " + size + ", " + scatDelay + ", '" + name +"')");
            pst.execute();

            //Put data into ATOFMSAtomInfoSparse
            for (String sparsestr : sparse) {
                String[] sparseparams = sparsestr.split(", ");
                String mtc = sparseparams[0];
                String area = sparseparams[1];
                String relarea = sparseparams[2];
                String height = sparseparams[3];
                pst = conn.prepareStatement("INSERT INTO ATOFMSAtomInfoSparse VALUES (" + Integer.toString(currentAtomID) + ", " + mtc + ", " + area + ", " + relarea + ", " + height +")");
                pst.execute();
            }

            pst = conn.prepareStatement("INSERT INTO AtomMembership " +
                    "(CollectionID, AtomID) " +
                    "VALUES (" +
                    collection.getCollectionID() + ", " +
                    nextID + ")");
            pst.execute();
        }
        catch (SQLException ex){
            System.out.println("Error creating table stuffToDelete in database.");
            System.out.println(ex.getMessage());
        }
        return nextID;

    }

    // gets the max id of any particle, this is the next id I guess?
    public int getNextID(){
        //Connection conn = null;

        try (Connection conn = connect(url)) {
            Statement stmt = conn.createStatement();
            int nextID = 0;

            ResultSet frs = stmt.executeQuery("SELECT * FROM AtomMembership LIMIT 1");
            if(!frs.next()){
                nextID = 0;
            }
            else{
                ResultSet rs = stmt.executeQuery("SELECT MAX(AtomID) FROM AtomMembership");
                if(rs.next())
                    nextID = rs.getInt(1) + 1;
            }
            stmt.close();
            return nextID;
        }
        catch (SQLException ex){
            System.out.println(ex.getMessage());
        }
        return -1;
    }

    // simple, adds a particle to the Centeratoms table
    public boolean addCenterAtom(int centerAtomID, int centerCollID){
        //Connection conn = null;
        boolean success = false;

        try (Connection conn = connect(url)) {
            Statement stmt = conn.createStatement();
            String query = "INSERT INTO CenterAtoms \n" +
                    "VALUES ("+ centerAtomID + ", " + centerCollID + ")";
            stmt.execute(query);
            stmt.close();
            success = true;
        }
        catch (SQLException ex){
            System.out.println(ex.getMessage());
        }
        return success;
    }

    // simple, updates a collection's description
    public boolean setCollectionDescription(Collection collection,
                                            String description){
        //Connection conn = null;
        description = removeReservedCharacters(description);

        try (Connection conn = connect(url)) {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(
                    "UPDATE Collections\n" +
                            "SET Description = '" + description + "'\n" +
                            "WHERE CollectionID = " + collection.getCollectionID());
            stmt.close();
        }
        catch (SQLException ex){
            System.out.println(ex.getMessage());
        }
        return true;
    }

    // simple, gets the name of a collection
    public String getCollectionName(int collectionID){
        String name = "";
        //Connection conn = null;
        try (Connection conn = connect(url)) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT Name\n" +
                    "FROM Collections\n" +
                    "WHERE CollectionID = " +
                    collectionID);
            rs.next();
            name = rs.getString("Name");
            stmt.close();

        }
        catch (SQLException ex){
            System.out.println(ex.getMessage());
        }

        return name;
    }

    // returns a string naming the database system implementing this interface
    public String dbname(){
        return "PostgreSQL";
    }

    // clear out the database so it is fresh for running tests (not used in the benchmark)
    public void clear(){
        //Connection conn = null;

        try (Connection conn = connect(url)) {
            PreparedStatement stmt = conn.prepareStatement("DELETE FROM Collections");
            stmt.execute();
            stmt = conn.prepareStatement("DELETE FROM CollectionRelationships");
            stmt.execute();
            stmt = conn.prepareStatement("DELETE FROM AtomMembership");
            stmt.execute();
            stmt = conn.prepareStatement("DELETE FROM ATOFMSAtomInfoDense");
            stmt.execute();
            stmt = conn.prepareStatement("DELETE FROM ATOFMSAtomInfoSparse");
            stmt.execute();
            stmt = conn.prepareStatement("DELETE FROM CenterAtoms");
            stmt.execute();
            stmt = conn.prepareStatement("DROP TABLE IF EXISTS stuffToDelete");
            stmt.execute();
            stmt.close();
        }
        catch (SQLException ex){
            System.out.println(ex.getMessage());
        }

    }
}
