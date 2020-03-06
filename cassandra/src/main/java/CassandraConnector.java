//package com.marxmart.persistence;


import com.datastax.driver.core.*;

import static java.lang.System.out;
/**
 * Class used for connecting to Cassandra database.
 */
public class CassandraConnector
{
    /** Cassandra Cluster. */
    private Cluster cluster;
    /** Cassandra Session. */
    private Session session;
    /**
     * Connect to Cassandra Cluster specified by provided node IP
     * address and port number.
     *
     * @param node Cluster node IP address.
     * @param port Port of cluster host.
     */
    public void connect(final String node, final int port)
    {
        this.cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
        final Metadata metadata = cluster.getMetadata();
        out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (final Host host : metadata.getAllHosts())
        {
            out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }
    /**
     * Provide my Session.
     *
     * @return My session.
     */
    public Session getSession()
    {
        return this.session;
    }
    /** Close cluster. */
    public void close()
    {
        cluster.close();
    }

    public static void main(final String[] args)
    {
        final CassandraConnector client = new CassandraConnector();
        final String ipAddress = args.length > 0 ? args[0] : "localhost";
        final int port = args.length > 1 ? Integer.parseInt(args[1]) : 9042;
        out.println("Connecting to IP Address " + ipAddress + ":" + port + "...");
        client.connect(ipAddress, port);

        final String createKeySpace =
                "CREATE KEYSPACE IF NOT EXISTS movies_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};";
        client.getSession().execute(createKeySpace);

        final String createMovieCql =
                "CREATE TABLE movies_keyspace.movies (title varchar, year int, description varchar, "
                        + "mmpa_rating varchar, dustin_rating varchar, PRIMARY KEY (title, year))";
        client.getSession().execute(createMovieCql);
        MoviePersistence mp = new MoviePersistence(ipAddress, port);
        mp.persistMovie("Title", 1997, "description", "rating", "rating");

        final ResultSet movieResults = client.getSession().execute(
                "SELECT * from movies_keyspace.movies WHERE title = ? AND year = ?", "title", 1997);
        System.out.println(movieResults);
        System.out.println("HELLO KELSEY");
        client.close();
    }

}