package org.example.pds_expiration;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.util.UUID;

public class Client1 {

    private static final int N = 10;

    public static void main(String[] args) throws InterruptedException {
        Ignite ignite = Cluster.ignite("Client1", true);
        IgniteCluster cluster = ignite.cluster();

        if (!cluster.active()) {
            cluster.active(true);
        }

//        ignite.compute(ignite.cluster().forServers()).run(new Run());

        Thread.sleep(3_000L);
        IgniteCache<Integer, UUID> cache = ignite.cache(Cluster.CACHE);
        for (int i = 0; i < N; i++) {
            cache.put(i, UUID.randomUUID());
        }
        System.out.format("%d records inserted%n", N);

        Thread.sleep(3_000L);
        for (int i = N; i < N * 2; i++) {
            cache.put(i, UUID.randomUUID());
        }
        System.out.format("%d records inserted%n", N);
    }

    public static class Run implements IgniteRunnable {

        @IgniteInstanceResource
        private Ignite ignite;

        @Override
        public void run() {
            ignite.events().remoteListen(
                    (UUID uuid, CacheEvent event) -> {
                        try {
                            System.out.println(
                                    event.eventNode().consistentId() + " :: " + event.key() + " : " + event.newValue()
                            );

                            return true;
                        } catch (Exception e) {
                            return false;
                        }
                    },
                    (CacheEvent event) -> event.cacheName().equals(Cluster.CACHE),
                    EventType.EVT_CACHE_OBJECT_PUT
            );
        }
    }

}
