package edu.carleton.clusteringbenchmark;

public interface CollectionCursor {
    public boolean next();
    public ParticleInfo getCurrent();
    public void close();
    public void reset();
}
