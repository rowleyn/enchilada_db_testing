
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
    CalInfo calinfo;
    public Map par;
    public List<String[]> set;
    public List<String> particlenames;
    public List<List<Map>> sparsemaps;
    public List<Map> densemaps;

    public DataRead(String parfile, String setfile, String masscal, String sizecal) throws Exception {
        readParFile(parfile);
        readSetFile(setfile);
        try {
            calinfo = new CalInfo(masscal, sizecal, false);
        }
        catch (IOException e){
            System.out.println("Failed to read calibration files.");
            System.out.println(e);
            System.exit(1);
        }
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
                par.put(keyvalue[0].toLowerCase(), keyvalue[1]);
            }
        }
    }

    private void readSetFile(String setfile) throws IOException {
        CSVReader reader = new CSVReader(new FileReader(setfile));
        set = reader.readAll();
        reader.close();
    }

    public void readAllSpectra() {
        particlenames = new ArrayList<>();
        sparsemaps = new ArrayList<>();
        densemaps = new ArrayList<>();
        DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        Pattern p = Pattern.compile("e~.*");
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
                Date date;
                try {
                    date = df.parse(datetime);
                }
                catch (ParseException e) {
                    System.out.println("Invalid date formatting, skipping...");
                    date = null;
                }
                ATOFMSParticle particle;
                if (date != null) {
                    try {
                        ReadSpec read = new ReadSpec(filename, date);
                        particle = read.getParticle();
                    } catch (IOException e) {
                        particle = null;
                    }
                    if (particle != null) {
                        if (m.matches()) {
                            particlenames.add(spectrum[1].substring(6).split("\\.")[0]);
                        } else {
                            particlenames.add(spectrum[1].substring(1).split("\\.")[0]);
                        }
                        sparsemaps.add(particle.particleInfoSparseMap());
                        densemaps.add(particle.particleInfoDenseMap());
                    }
                }
            }
        }
    }

    /*
    Data structure returned by this method:
    List[Map{name, sparse, dense}] Last item is set index
    name --> string
    sparse --> List[Map{masstocharge, area, relarea, height}]
    dense --> Map{time, laserpower, size, scatdelay, specname}
     */
    public List readNSpectraFrom(int n, int start) {
        List particles = new ArrayList<>();
        int last = 0;
        DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        Pattern p = Pattern.compile("e~.*");
        PeakParams peakparams = new PeakParams(0,0,0,0);
        ATOFMSParticle.currCalInfo = calinfo;
        ATOFMSParticle.currPeakParams = peakparams;
        for (int i = start; i < start + n; i++) {
            String[] spectrum;
            last = i + 1;
            try {
                spectrum = set.get(i);
            }
            catch (IndexOutOfBoundsException e) {
                particles.add(last);
                return particles;
            }
            if (spectrum.length > 0) {
                Map data = new HashMap();
                Matcher m = p.matcher(spectrum[1]);
                String filename;
                String particlename;
                if (m.matches()) {
                    filename = "data/" + spectrum[1].substring(0,6) + "/" + spectrum[1].substring(6);
                    particlename = spectrum[1].substring(6).split("\\.")[0];
                }
                else {
                    filename = "data/" + spectrum[1].substring(0,1) + "/" + spectrum[1].substring(1);
                    particlename = spectrum[1].substring(1).split("\\.")[0];
                }
                String datetime = spectrum[5];
                Date date;
                try {
                    date = df.parse(datetime);
                }
                catch (ParseException e) {
                    System.out.println("Invalid date formatting, skipping...");
                    date = null;
                }
                ATOFMSParticle particle;
                if (date != null) {
                    try {
                        ReadSpec read = new ReadSpec(filename, date);
                        particle = read.getParticle();
                    } catch (IOException e) {
                        particle = null;
                        n++;
                    }
                    if (particle != null) {
                        data.put("name", particlename);
                        data.put("sparse", particle.particleInfoSparseMap());
                        data.put("dense", particle.particleInfoDenseMap());
                    }

                    particles.add(data);
                }
                else {
                    n++;
                }
            }
        }

        particles.add(last);

        return particles;
    }
}
