//package cassandra;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.Optional;
import static java.lang.System.out;
/**
 * Handles movie persistence access.
 */
public class MoviePersistence
{
    private final CassandraConnector client = new CassandraConnector();
    public MoviePersistence(final String newHost, final int newPort)
    {
        out.println("Connecting to IP Address " + newHost + ":" + newPort + "...");
        client.connect(newHost, newPort);
    }
    /**
     * Persist provided movie information.
     *
     * @param title Title of movie to be persisted.
     * @param year Year of movie to be persisted.
     * @param description Description of movie to be persisted.
     * @param mmpaRating MMPA rating.
     * @param dustinRating Dustin's rating.
     */
    public void persistMovie(
            final String title, final int year, final String description,
            final String mmpaRating, final String dustinRating)
    {
        client.getSession().execute(
                "INSERT INTO movies_keyspace.movies (title, year, description, mmpa_rating, dustin_rating) VALUES (?, ?, ?, ?, ?)",
                title, year, description, mmpaRating, dustinRating);
    }

    /**
     * Close my underlying Cassandra connection.
     */
    private void close()
    {
        client.close();
    }
}