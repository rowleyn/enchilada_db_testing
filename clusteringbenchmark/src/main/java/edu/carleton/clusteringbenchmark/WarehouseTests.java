package edu.carleton.clusteringbenchmark;

import edu.carleton.clusteringbenchmark.ATOFMS.ParticleInfo;
import edu.carleton.clusteringbenchmark.analysis.BinnedPeakList;
import edu.carleton.clusteringbenchmark.atom.ATOFMSAtomFromDB;
import edu.carleton.clusteringbenchmark.collection.Collection;
import edu.carleton.clusteringbenchmark.database.CassandraWarehouse;
import edu.carleton.clusteringbenchmark.database.CollectionCursor;
import edu.carleton.clusteringbenchmark.database.DynamicTable;
import edu.carleton.clusteringbenchmark.database.InfoWarehouse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class WarehouseTests {
    private static InfoWarehouse db;

    private static int NUM_PARTICLES = 5;

    private static String filename = "'ThisFile'";
    private static String dateString = "'1983-01-19 05:05:00.0'";
    private static float laserPower = (float)0.01191983;
    private static float size = (float)0.5;
    private static int scatterDelay = 10;

    private static int posPeakLocation1 = 19;
    private static int negPeakLocation1 = -20;
    private static int peak1Height = 80;
    private static int posPeakLocation2 = 100;
    private static int negPeakLocation2 = -101;
    private static int peak2Height = 100;

    public static void main (String[]args) throws Exception {
            db = new CassandraWarehouse();
            createCollectionTest();
            List<Integer> atomids = insertParticlesTest();
            bulkInsertTest(atomids);
            bulkDeleteTest(atomids);
            centerAtomsTest();
            colNamesTest();
            cursorTest();


    }
    public static Collection createCollectionTest() {
        String dataType = db.getCollectionDatatype(0);
        int collectionid = db.createEmptyCollection(dataType, 0, "name", "comment", "description");

        assert db.getCollectionName(collectionid).equals("name");

        Collection test = db.getCollection(collectionid);
        assert test.getName().equals("name");
        assert test.getCollectionID() == 0;
        assert test.getDatatype().equals(dataType);

        assert db.getCollectionSize(collectionid) == 1;

        return db.getCollection(collectionid);
    }

    public static List<Integer> insertParticlesTest() {
        ArrayList<String> sparseData = new ArrayList<>();
        sparseData.add(posPeakLocation1 + ", " +  peak1Height + ", 0.1, " + peak1Height);
        sparseData.add(negPeakLocation1 + ", " +  peak1Height + ", 0.1, " + peak1Height);
        sparseData.add(posPeakLocation2 + ", " +  peak2Height + ", 0.1, " + peak2Height);
        sparseData.add(negPeakLocation2 + ", " +  peak2Height + ", 0.1, " + peak2Height);

        String denseData = dateString + "," + laserPower + "," + size + "," + scatterDelay + ", " + filename;

        List<Integer> atomids = new ArrayList<>();
        int lastID = -1;

        for (int i = 0; i < NUM_PARTICLES; i++) {
            int nextID = db.getNextID();
            assert nextID - 1 == lastID;
            lastID = nextID;
            int particleID = db.insertParticle(denseData, sparseData, db.getCollection(0), nextID);
            assert nextID == particleID;
            atomids.add(particleID);
        }

        return atomids;
    }

    public static void bulkInsertTest(List<Integer> atomids) throws Exception {
        try {
            db.bulkInsertInit();
        } catch (IOException  e){
            System.err.println("Error in bulk insert init");
            e.printStackTrace();
            System.exit(1);
        }
        int newcollectionid = db.createEmptyCollection("ATOFMS", -1, "name", "comment", "description");
        assert newcollectionid == 1;
        for (int atomid : atomids) {
            db.bulkInsertAtom(atomid, newcollectionid);
        }
        try{
            db.bulkInsertExecute();
        } catch (Exception e){
            System.out.println("Error in bulk insert execute");
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void bulkDeleteTest(List<Integer> atomids) throws Exception {
        int atomID = atomids.get(0);
        int atomID2 = atomids.get(1);
        StringBuilder atomIDsToDelete= new StringBuilder();
        atomIDsToDelete.append(atomID);
        atomIDsToDelete.append(",");
        atomIDsToDelete.append(atomID2);
        db.bulkDelete(atomIDsToDelete, db.getCollection(1));
    }

    public static void centerAtomsTest() {
        int collectionid = db.createEmptyCollection("ATOFMS", -1, "name", "comment", "description");
        boolean result = db.addCenterAtom(0, collectionid);
        assert result;
        result = db.addCenterAtom(0, 100);
        assert !result;
    }

    public static void colNamesTest() {
        List<ArrayList<String>> dense = db.getColNamesAndTypes("ATOFMS", DynamicTable.AtomInfoDense);
        List<ArrayList<String>> sparse = db.getColNamesAndTypes("ATOFMS", DynamicTable.AtomInfoSparse);
        assert dense.get(0).get(0).equals("time") && dense.get(0).get(1).equals("DATETIME");
        assert dense.get(1).get(0).equals("laserpower") && dense.get(1).get(1).equals("REAL");
        assert dense.get(2).get(0).equals("size") && dense.get(2).get(1).equals("REAL");
        assert dense.get(3).get(0).equals("scatdelay") && dense.get(3).get(1).equals("INT");
        assert dense.get(4).get(0).equals("specname") && dense.get(4).get(1).equals("VARCHAR(8000)");
        assert sparse.get(0).get(0).equals("masstocharge") && sparse.get(0).get(1).equals("REAL");
        assert sparse.get(1).get(0).equals("area") && sparse.get(1).get(1).equals("INT");
        assert sparse.get(2).get(0).equals("relarea") && sparse.get(2).get(1).equals("REAL");
        assert sparse.get(3).get(0).equals("height") && sparse.get(3).get(1).equals("INT");
    }

    public static void cursorTest() {
        int count;
        CollectionCursor cursor = db.getAtomInfoOnlyCursor(db.getCollection(0));
        for (int i = 0; i < 2; i++) {
            count = 0;
            while (cursor.next()) {
                count++;
                ParticleInfo info = cursor.getCurrent();
                ATOFMSAtomFromDB dense = info.getATOFMSParticleInfo();
                BinnedPeakList sparse = info.getBinnedList();
                assert filename.equals(dense.getFilename());
                assert dateString.equals(dense.getDateString());
                assert laserPower == dense.getLaserPower();
                assert size == dense.getSize();
                assert scatterDelay == dense.getScatDelay();
                assert peak1Height == sparse.getAreaAt(posPeakLocation1);
                assert peak1Height == sparse.getAreaAt(negPeakLocation1);
                assert peak2Height == sparse.getAreaAt(posPeakLocation2);
                assert peak2Height == sparse.getAreaAt(negPeakLocation2);
            }
            assert count == NUM_PARTICLES;
            cursor.reset();
        }
        count = 0;
        cursor = db.getAtomInfoOnlyCursor(db.getCollection(1));
        while (cursor.next()) {
            count++;
            ParticleInfo info = cursor.getCurrent();
            ATOFMSAtomFromDB dense = info.getATOFMSParticleInfo();
            BinnedPeakList sparse = info.getBinnedList();
            assert filename.equals(dense.getFilename());
            assert dateString.equals(dense.getDateString());
            assert laserPower == dense.getLaserPower();
            assert size == dense.getSize();
            assert scatterDelay == dense.getScatDelay();
            assert peak1Height == sparse.getAreaAt(posPeakLocation1);
            assert peak1Height == sparse.getAreaAt(negPeakLocation1);
            assert peak2Height == sparse.getAreaAt(posPeakLocation2);
            assert peak2Height == sparse.getAreaAt(negPeakLocation2);
        }
        assert count == NUM_PARTICLES - 2;
        cursor.reset();
        cursor.close();
    }
}
