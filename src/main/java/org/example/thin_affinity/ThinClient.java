package org.example.thin_affinity;

import lombok.Value;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ThinClient {

    public static void main(String[] args) {
        Ignite server = Ignition.start(new IgniteConfiguration());

        IgniteClient client = Ignition.startClient(
                new ClientConfiguration()
                        .setAddresses("localhost:10800")
                        .setBinaryConfiguration(
                                new BinaryConfiguration()
                                        .setCompactFooter(true)
                        )
        );

        IgniteCache<K, V> kv = server.getOrCreateCache(
                new CacheConfiguration<K, V>("kv")
        );

        kv.put(K.of("s", 0), V.of("s", 0));

        System.out.println(
                client.cache("kv").get(K.of("s", 0))
        );


        Map<K, V> result1 = client.<K, V>cache("kv").getAll(
                IntStream.range(0, 10)
                        .mapToObj(i -> K.of("k" + i, i))
                        .collect(Collectors.toSet())
        );

        IgniteCache<K2, V> kv2 = server.getOrCreateCache("kv2");
        IntStream.range(0, 10)
                .forEach(i -> kv2.put(K2.of("k" + i, i), V.of("v" + i, i)));

        Map<K2, V> result2 = client.<K2, V>cache("kv2").getAll(
                IntStream.range(0, 10)
                        .mapToObj(i -> K2.of("k" + i, i))
                        .collect(Collectors.toSet())
        );

        System.out.println();
    }

    @Value(staticConstructor = "of")
    public static class K {
        String stringField;
        @AffinityKeyMapped
        Integer intField;
    }

    @Value(staticConstructor = "of")
    public static class K2 {
        String stringField;
        Integer intField;
    }

    @Value(staticConstructor = "of")
    public static class V {
        String stringField;
        Integer intField;
    }

}
