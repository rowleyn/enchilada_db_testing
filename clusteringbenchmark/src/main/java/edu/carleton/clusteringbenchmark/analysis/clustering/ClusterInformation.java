package edu.carleton.clusteringbenchmark.analysis.clustering;

import java.util.ArrayList;

public class ClusterInformation {
	// names have 'ATOFMSAtomInfoDense.' , etc. before it. - AR
	public ArrayList<String> valueColumns;
	public String keyColumn;
	public String weightColumn;
	public boolean automatic;
	public boolean normalize;
	
	public ClusterInformation(ArrayList<String> v, String k, String w, boolean a, boolean n) {
		valueColumns = v;
		keyColumn = k;
		weightColumn = w;
		automatic = a;
		normalize = n;
		
	}
	
	//dummy constructor used for testing method(s) in Cluster
	public ClusterInformation() {
	}
	
	public void printSelf() {
		System.out.println("value columns: " + valueColumns);
		System.out.println("key col " + keyColumn);
		System.out.println("weightcol " + weightColumn);
		System.out.println("auto " + automatic);
		System.out.println("normalize " + normalize);
	}
}
