
/*
* Created by Nathan Rowley on 2/13/2020.
* Much code borrowed and adapted from the ReadSpec class in Enchilada's ATOFMS package.
*/

package edu.carleton.dataloadbenchmark;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.opencsv.*;

public class DataRead {
    public Map par;
    public List<String[]> sem;
    public List<String[]> set;
    public List<List<Map>> sparsemaps;
    public List<Map> densemaps;

    public DataRead(String parfile, String semfile, String setfile, String masscal, String sizecal) throws Exception {
        readParFile(parfile);
        readSemFile(semfile);
        readSetFile(setfile);
        readAllSpectra(masscal, sizecal);
    }

    private void readParFile(String parfile) throws IOException {
        CSVReader reader = new CSVReader(new FileReader(parfile));
        List<String[]> parlines = reader.readAll();
        reader.close();

        ListIterator<String[]> pariterator = parlines.listIterator();
        pariterator.next();

        par = new HashMap<>();

        while (pariterator.hasNext()) {
            String[] line = pariterator.next();

            if (line.length > 0) {
                String[] keyvalue = line[0].split("=");
                par.put(keyvalue[0], keyvalue[1]);
            }
        }
    }

    private void readSemFile(String semfile) throws IOException {
        CSVReader reader = new CSVReader(new FileReader(semfile));
        sem = reader.readAll();
        reader.close();
    }

    private void readSetFile(String setfile) throws IOException {
        CSVReader reader = new CSVReader(new FileReader(setfile));
        set = reader.readAll();
        reader.close();
    }

    private void readAllSpectra(String masscal, String sizecal) throws IOException, ParseException {
        sparsemaps = new ArrayList<>();
        densemaps = new ArrayList<>();
        DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        Pattern p = Pattern.compile("e~");
        CalInfo calinfo = new CalInfo(masscal, sizecal, false);
        PeakParams peakparams = new PeakParams(0,0,0,0);
        ATOFMSParticle.currCalInfo = calinfo;
        ATOFMSParticle.currPeakParams = peakparams;
        for (String[] spectrum : set) {
            if (spectrum.length > 0) {
                Matcher m = p.matcher(spectrum[1]);
                String filename;
                if (m.matches()) {
                    filename = "data/" + spectrum[1].substring(0,6) + "/" + spectrum[1].substring(6);
                }
                else {
                    filename = "data/" + spectrum[1].substring(0,1) + "/" + spectrum[1].substring(1);
                }
                String datetime = spectrum[5];
                Date date = df.parse(datetime);
                ReadSpec read = new ReadSpec(filename, date);
                ATOFMSParticle particle = read.getParticle();
                if (particle != null) {
                    sparsemaps.add(particle.particleInfoSparseMap());
                    densemaps.add(particle.particleInfoDenseMap());
                }
            }
        }
    }
}
