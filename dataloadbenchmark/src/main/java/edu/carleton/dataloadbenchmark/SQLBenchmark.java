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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Date;

public class SQLBenchmark implements DatabaseLoad {

    private Statement batchStatement;
    private ArrayList<Integer> alteredCollections;
    private PrintWriter bulkInsertFileWriterSparse;
    private PrintWriter bulkInsertFileWriterDense;
    private File bulkInsertFileSparse;
    private File bulkInsertFileDense;

    public void clear(){
        //Drop the db if it exists and create a new one
        Connection conn = null;
        ResultSet rs = null;
        String url = "jdbc:jtds:sqlserver://127.0.0.1;instance=SQLEXPRESS01;";
        String driver = "net.sourceforge.jtds.jdbc.Driver";
        String userName = "SpASMS";
        String password = "finally";
        PreparedStatement pst;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            pst = conn.prepareStatement("IF DB_ID('enchilada_benchmark') IS NOT NULL " +
                    "DROP DATABASE enchilada_benchmark");
            pst.execute();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
                //rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    //Single insert
    //@Override
    public boolean insertSingle(DataRead reader) {
        createDatabase();
        //Create table
        Connection conn = null;
        ResultSet rs = null;
        String url = "jdbc:jtds:sqlserver://127.0.0.1;instance=SQLEXPRESS01;DatabaseName=enchilada_benchmark";
        String driver = "net.sourceforge.jtds.jdbc.Driver";
        String userName = "SpASMS";
        String password = "finally";
        PreparedStatement pst;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            //Create tables
            pst = conn.prepareStatement("CREATE TABLE ATOFMSAtomInfoSparse (AtomID INT, PeakLocation FLOAT, PeakArea INT, RealPeakArea FLOAT, PeakHeight INT, PRIMARY KEY (AtomID, PeakLocation));");
            //pst = conn.prepareStatement("CREATE TABLE ATOFMSAtomInfoSparse ( AtomID INT, PeakLocation INT, PeakArea INT)");
            pst.executeUpdate();
            pst = conn.prepareStatement("CREATE TABLE ATOFMSAtomInfoDense (AtomID INT, Time SMALLDATETIME, LaserPower FLOAT, Size FLOAT, ScatDelay INT, OrigFileName VARCHAR(8000), PRIMARY KEY (AtomID));");
            pst.executeUpdate();

            String dbdatasetname = (String)reader.par.get("dbdatasetname");
            boolean moretoread = true;
            int setindex = 0;
            int currentAtomID = 0;

            while(moretoread){
                //Get n rows of data (n particles)
                List data = reader.readNSpectraFrom(1, setindex);
                setindex = (int)data.get(data.size() - 1);
                //List<Document> spectra = new ArrayList<>();

                //Put data in tables
                for (int i = 0; i < data.size() - 1; i++) {
                    //Map spectrum = new HashMap();
                    //spectrum.put("_id", ((Map)data.get(i)).get("name"));
                    //spectrum.put("par_id", dbdatasetname);
                    //spectrum.put("sparsedata", ((Map)data.get(i)).get("sparse"));
                    //spectrum.put("densedata", ((Map)data.get(i)).get("dense"));

                    //Put data into ATOFMSAtomInfoDense
                    Map dense = (Map)((Map)data.get(i)).get("dense");
                    Date time = (Date)dense.get("time");
                    //System.out.println(time);

                    //DateFormat originalDateFormat = new SimpleDateFormat("MMM dd hh:mm:ss yyyy");
                    //String tempDate = originalDateFormat.format(time);
                    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                    /*
                    try{
                        Date d = originalDateFormat.parse(tempDate);
                        System.out.println("Check:");
                        System.out.println(dateFormat.format(d));
                    }
                    catch (ParseException e){
                        e.printStackTrace();
                    }
                    */
                    String strDate = dateFormat.format(time);
                    float laserpower = (float)dense.get("laserpower");
                    float size = (float)dense.get("size");
                    int scatDelay = (int)dense.get("scatdelay");
                    String name = (String)dense.get("specname");
                    //spectra.add(new Document(spectrum));
                    System.out.println(name);
                    //System.out.println(strDate);
                    pst = conn.prepareStatement("INSERT INTO ATOFMSAtomInfoDense VALUES ("+ Integer.toString(currentAtomID) + ", '" + strDate + "', " + Float.toString(laserpower) + ", " + Float.toString(size) + ", " + Integer.toString(scatDelay) + ", '" + name +"')");
                    pst.execute();

                    //Put data into ATOFMSAtomInfoSparse
                    List<Map> sparse = (List<Map>)((Map)data.get(i)).get("sparse");
                    for (Map m: sparse) {
                        double mtc = (double)m.get("masstocharge");
                        int area = (int)m.get("area");
                        float relarea = (float)m.get("relarea");
                        int height = (int)m.get("height");
                        pst = conn.prepareStatement("INSERT INTO ATOFMSAtomInfoSparse VALUES (" + Integer.toString(currentAtomID) + ", " + Double.toString(mtc) + ", " + Integer.toString(area) + ", " + Float.toString(relarea) + ", " + Integer.toString(height) +")");
                        //pst = conn.prepareStatement("INSERT INTO ATOFMSAtomInfoSparse VALUES (" + Integer.toString(currentAtomID)+ ", 5)");
                        pst.execute();
                    }
                    currentAtomID++;
                }
                if (setindex >= reader.set.size()) {
                    moretoread = false;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                conn.close();
                //rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    //Bulk insert
    public boolean insert(DataRead reader){
        createDatabase();
        //Create table
        Connection conn = null;
        String url = "jdbc:jtds:sqlserver://127.0.0.1;instance=SQLEXPRESS01;DatabaseName=enchilada_benchmark";
        String driver = "net.sourceforge.jtds.jdbc.Driver";
        String userName = "SpASMS";
        String password = "finally";
        PreparedStatement pst;
        bulkInsertInit();

        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            //Create tables
            pst = conn.prepareStatement("CREATE TABLE ATOFMSAtomInfoSparse (AtomID INT, PeakLocation FLOAT, PeakArea INT, RealPeakArea FLOAT, PeakHeight INT, PRIMARY KEY (AtomID, PeakLocation));");
            pst.executeUpdate();
            pst = conn.prepareStatement("CREATE TABLE ATOFMSAtomInfoDense (AtomID INT, Time SMALLDATETIME, LaserPower FLOAT, Size FLOAT, ScatDelay INT, OrigFileName VARCHAR(8000), PRIMARY KEY (AtomID));");
            pst.executeUpdate();

            String dbdatasetname = (String)reader.par.get("dbdatasetname");
            boolean moretoread = true;
            int setindex = 0;
            int currentAtomID = 0;
            //bulkInsertFileWriter.println(parentID+","+atomID);

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
                    System.out.println(name);
                    bulkInsertFileWriterDense.println(Integer.toString(currentAtomID)+","+strDate+","+Float.toString(laserpower) +","+Float.toString(size) +"," + Integer.toString(scatDelay) + "," + name);

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
            pst = conn.prepareStatement("BULK INSERT ATOFMSAtomInfoSparse FROM '" + bulkInsertFileSparse.getAbsolutePath() + "' WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');");
            pst.execute();
            pst = conn.prepareStatement("BULK INSERT ATOFMSAtomInfoDense FROM '" + bulkInsertFileDense.getAbsolutePath() + "' WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');");
            pst.execute();

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
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

        bulkInsertFileSparse = null;
        bulkInsertFileWriterSparse = null;
        alteredCollections = new ArrayList<Integer>();

        bulkInsertFileDense = null;
        bulkInsertFileWriterDense = null;

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
    }


    @Override
    public String name() {
        return "SQLExpress";
    }
    public void createDatabase(){
        //Create database
        Connection conn = null;
        ResultSet rs = null;
        String url = "jdbc:jtds:sqlserver://127.0.0.1;instance=SQLEXPRESS01;";
        String driver = "net.sourceforge.jtds.jdbc.Driver";
        String userName = "SpASMS";
        String password = "finally";
        PreparedStatement pst;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, userName, password);
            pst = conn.prepareStatement("CREATE DATABASE enchilada_benchmark");
            pst.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
                //rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    /*


    */
}
