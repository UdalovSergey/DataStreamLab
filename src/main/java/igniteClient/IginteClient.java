package igniteClient;

import org.apache.ignite.Ignition;
import org.apache.ignite.examples.model.Address;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.client.ClientException;

/**
 * TODO create a cache with TTL
 * see http://apache-ignite-users.70518.x6.nabble.com/Key-Value-Store-control-TTL-refresh-td19652.html
 */
public class IginteClient {
    private final ClientConfiguration cfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");
    private final String CACHE_NAME = "bot-cache";
    private final ClientCache<Integer, Object> cache;

    public IginteClient() {
        try (IgniteClient igniteClient = Ignition.startClient(cfg)) {
            cache = igniteClient.getOrCreateCache(CACHE_NAME);


        } catch (ClientException e) {
            System.err.println(e.getMessage());
        } catch (Exception e) {
            System.err.format("Unexpected failure: %s\n", e);
        }
    }

    //TODO: put IP as key, timeStamp of detecting bot as val
    public void putBot(Object val){
        cache.put(val, val);
    }
}
