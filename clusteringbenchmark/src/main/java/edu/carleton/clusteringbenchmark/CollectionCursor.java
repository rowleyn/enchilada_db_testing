package edu.carleton.clusteringbenchmark;

public interface CollectionCursor {
    /*
    if first pass, goes through and keeps calling getnext from super
    and saves them into an array. otherwise, return position < storedInfo.size().
	This returns a boolean to show if it is successful in going to the next thing 
	*/
    public boolean next();

    /*
	calls get method.
    */
    public ParticleInfo getCurrent();

    /*
    drops temprand table and calls super.close
    */
    public void close();
	
    /*
	calls rs.close
	and then rs = getAllAtomsRS(collection)
	this selects every atomID from InternalAtomOrder where 
	collectionID = collection.getcollectionid()
    */
    public void reset();
}
