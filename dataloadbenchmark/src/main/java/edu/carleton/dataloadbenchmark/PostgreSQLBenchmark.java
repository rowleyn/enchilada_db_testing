package edu.carleton.dataloadbenchmark;

import jdk.nashorn.internal.runtime.Debug;
import org.bson.Document;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class PostgreSQLBenchmark implements DatabaseLoad {
    String user = "postgres";
    String password = "finally";
    String url = "jdbc:postgresql://localhost/";

    private ArrayList<Integer> alteredCollections;
    private PrintWriter bulkInsertFileWriterSparse;
    private PrintWriter bulkInsertFileWriterDense;
    private PrintWriter bulkInsertFileWriterAtomMembership;
    private File bulkInsertFileSparse;
    private File bulkInsertFileDense;
    private File bulkInsertFileAtomMembership;

    public Connection connect(String url) {
        Connection conn = null;

        try {
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return conn;
    }

    public void clear(){
        //Drop the db if it exists and create a new one
        String SQL = "DROP DATABASE IF EXISTS enchilada_benchmark";
        try (Connection conn = connect(url)) {
            Statement stmt = conn.createStatement();
            stmt.execute(SQL);
            //System.out.println("Drop");
        }
        catch (SQLException ex){
            System.out.println(ex.getMessage());
        }
    }

    public boolean insert(DataRead reader){
        createDatabase();
        //String T1 = "CREATE TABLE ATOFMSAtomInfoSparse (AtomID INT, PeakLocation FLOAT, PeakArea INT, RealPeakArea FLOAT, PeakHeight INT, PRIMARY KEY (AtomID, PeakLocation));";
        //String T2 = "CREATE TABLE ATOFMSAtomInfoDense (AtomID INT, Time TIMESTAMP, LaserPower FLOAT, Size FLOAT, ScatDelay INT, OrigFileName VARCHAR(8000), PRIMARY KEY (AtomID));";
        bulkInsertInit();

        try (Connection conn = connect(url + "enchilada_benchmark")) {
            Statement stmt = conn.createStatement();
            //stmt.executeUpdate(T1);
            //stmt = conn.createStatement();
            //stmt.executeUpdate(T2);

            boolean moretoread = true;
            int setindex = 0;
            int currentAtomID = 0;

            stmt.executeUpdate("INSERT INTO Collections VALUES (0, 'Name', 'comment','description', 'ATOFMS')");
            stmt = conn.createStatement();
            stmt.executeUpdate("INSERT INTO CollectionRelationships VALUES (0, -1)");

            while(moretoread){
                //Get n rows of data (n particles)
                List data = reader.readNSpectraFrom(1, setindex);
                setindex = (int)data.get(data.size() - 1);

                //Put data into bulk file
                for (int i = 0; i < data.size() - 1; i++) {
                    //Put data into ATOFMSAtomInfoDense
                    Map dense = (Map)((Map)data.get(i)).get("dense");
                    Date time = (Date)dense.get("time");
                    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                    String strDate = dateFormat.format(time);

                    float laserpower = (float)dense.get("laserpower");
                    float size = (float)dense.get("size");
                    int scatDelay = (int)dense.get("scatdelay");
                    String name = (String)dense.get("specname");
                    //System.out.println(name);
                    bulkInsertFileWriterDense.println(Integer.toString(currentAtomID)+","+strDate+","+Float.toString(laserpower) +","+Float.toString(size) +"," + Integer.toString(scatDelay) + "," + name);
                    bulkInsertFileWriterAtomMembership.println("0," + Integer.toString(currentAtomID));

                    //Put data into ATOFMSAtomInfoSparse
                    List<Map> sparse = (List<Map>)((Map)data.get(i)).get("sparse");
                    for (Map m: sparse) {
                        double mtc = (double)m.get("masstocharge");
                        int area = (int)m.get("area");
                        float relarea = (float)m.get("relarea");
                        int height = (int)m.get("height");
                        bulkInsertFileWriterSparse.println(Integer.toString(currentAtomID) +"," + Double.toString(mtc) + "," + Integer.toString(area) + "," + Float.toString(relarea) + "," + Integer.toString(height));
                    }
                    currentAtomID++;
                }
                if (setindex >= reader.set.size()) {
                    moretoread = false;
                }
            }
            bulkInsertFileWriterSparse.close();
            bulkInsertFileWriterDense.close();
            bulkInsertFileWriterAtomMembership.close();

            stmt = conn.createStatement();
            stmt.execute("COPY ATOFMSAtomInfoSparse FROM '" + bulkInsertFileSparse.getAbsolutePath() + "' WITH (DELIMITER ',')");
            stmt = conn.createStatement();
            stmt.execute("COPY ATOFMSAtomInfoDense FROM '" + bulkInsertFileDense.getAbsolutePath() + "' WITH (DELIMITER ',')");
            stmt = conn.createStatement();
            stmt.execute("COPY AtomMembership FROM '" + bulkInsertFileAtomMembership.getAbsolutePath() + "' WITH (DELIMITER ',')");
            //pst = conn.prepareStatement("BULK INSERT ATOFMSAtomInfoSparse FROM '" + bulkInsertFileSparse.getAbsolutePath() + "' WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');");
            //pst.execute();
            //pst = conn.prepareStatement("BULK INSERT ATOFMSAtomInfoDense FROM '" + bulkInsertFileDense.getAbsolutePath() + "' WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');");
            //pst.execute();
        }
        catch (SQLException ex){
            System.out.println(ex.getMessage());
        }
        return true;
    }

    //Create temporary bulk files
    private void bulkInsertInit(){
        if (bulkInsertFileSparse != null && bulkInsertFileSparse.exists()) {
            bulkInsertFileSparse.delete();
        }

        if (bulkInsertFileDense != null && bulkInsertFileDense.exists()) {
            bulkInsertFileDense.delete();
        }

        if (bulkInsertFileAtomMembership != null && bulkInsertFileAtomMembership.exists()) {
            bulkInsertFileAtomMembership.delete();
        }

        bulkInsertFileSparse = null;
        bulkInsertFileWriterSparse = null;
        alteredCollections = new ArrayList<Integer>();

        bulkInsertFileDense = null;
        bulkInsertFileWriterDense = null;
        bulkInsertFileAtomMembership = null;

        try {
            bulkInsertFileSparse = new File("Temp"+File.separator+"bulkfileSparse"+".txt");
            bulkInsertFileSparse.deleteOnExit();
            bulkInsertFileWriterSparse = new PrintWriter(new FileWriter(bulkInsertFileSparse));
        } catch (IOException e) {
            System.err.println("Trouble creating " + bulkInsertFileSparse.getAbsolutePath() + "");
            e.printStackTrace();
        }

        try {
            bulkInsertFileDense = new File("Temp"+File.separator+"bulkfileDense"+".txt");
            bulkInsertFileDense.deleteOnExit();
            bulkInsertFileWriterDense = new PrintWriter(new FileWriter(bulkInsertFileDense));
        } catch (IOException e) {
            System.err.println("Trouble creating " + bulkInsertFileDense.getAbsolutePath() + "");
            e.printStackTrace();
        }

        try {
            bulkInsertFileAtomMembership = new File("Temp"+File.separator+"bulkfileCollections"+".txt");
            bulkInsertFileAtomMembership.deleteOnExit();
            bulkInsertFileWriterAtomMembership = new PrintWriter(new FileWriter(bulkInsertFileAtomMembership));
        } catch (IOException e) {
            System.err.println("Trouble creating " + bulkInsertFileAtomMembership.getAbsolutePath() + "");
            e.printStackTrace();
        }
    }


    public void createDatabase(){
        //Connection conn;
        Scanner in = null;
        String SQL = "CREATE DATABASE enchilada_benchmark";
        try (Connection conn = connect(url)) {
            //conn = connect();
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(SQL);
            //rs.next();
            //System.out.println(rs.getInt(1));
        }
        catch (SQLException ex){
            System.out.println(ex.getMessage());
        }

        try (Connection conn = connect(url + "enchilada_benchmark")) {
            in = new Scanner(new File("PostgreSQLRebuildDatabase.txt"));
            String query = "";
            StringTokenizer token;
            // loop through license block
            while (in.hasNext()) {
                query = in.nextLine();
                token = new StringTokenizer(query);
                if (token.hasMoreTokens()) {
                    String s = token.nextToken();
                    if (s.equals("CREATE"))
                        break;
                }
            }
            // Update the database according to the stmts.
            conn.createStatement().executeUpdate(query);

            while (in.hasNext()) {
                query = in.nextLine();
                conn.createStatement().executeUpdate(query);
            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    public String name() {
        return "postgresSQL";
    }
}
