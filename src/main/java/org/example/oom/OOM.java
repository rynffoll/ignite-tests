package org.example.oom;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.ExponentialBackoffTimeoutStrategy;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;


public class OOM {

    public static void main(String[] args) {

        Ignite server1 = G.start(config("server1"));

        CommunicationSpi commSpi = server1.configuration().getCommunicationSpi();

        long totalTimeout = 0;

        if (commSpi instanceof TcpCommunicationSpi) {
            TcpCommunicationSpi cfg0 = (TcpCommunicationSpi) commSpi;

            totalTimeout =
                    cfg0.failureDetectionTimeoutEnabled()
                            ? cfg0.failureDetectionTimeout()
                            : ExponentialBackoffTimeoutStrategy.totalBackoffTimeout(
                            cfg0.getConnectTimeout(),
                            cfg0.getMaxConnectTimeout(),
                            cfg0.getReconnectCount()
                    );
        }

//        long timeout = Math.max((long) (totalTimeout * 1.5), DEFAULT_MIN_TOMBSTONE_TTL);
//        Ignite server2 = G.start(config("server2"));

//        Ignite client = G.start(
//                new IgniteConfiguration()
//                        .setIgniteInstanceName("client")
//                        .setConsistentId("client")
//                        .setClientMode(true)
//        );

        IgniteCache<Key, String> cache = server1.getOrCreateCache(
                new CacheConfiguration<Key, String>("m")
                        .setDataRegionName("memory")
                        .setAffinity(
                                new RendezvousAffinityFunction()
//                                        .setPartitions(10)
                        )
                        .setStatisticsEnabled(true)
                        .setManagementEnabled(true)
        );

        AtomicInteger integer = new AtomicInteger();
//        new Thread(() -> {
        try {
            while (true) {
                System.out.println(">>> i = " + integer.getAndIncrement());

                IntStream.range(0, 1_000).forEach(i -> cache.put(new Key(UUID.randomUUID().toString(), i), ""));

                cache.query(new ScanQuery<>((k, v) -> {
                    server1.cache("m").remove(k);
                    return false;
                })).getAll();

                System.out.println(">>> sleep");
//                TimeUnit.SECONDS.sleep(1);
            }
//        } catch (InterruptedException e) {
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
//        }).start();

//        System.out.println();
    }

    public static IgniteConfiguration config(String name) {
        return new IgniteConfiguration()
                .setIgniteInstanceName(name)
                .setConsistentId(name)
                .setCommunicationSpi(
                        new TcpCommunicationSpi()
                                .setUsePairedConnections(false)
                                .setConnectionsPerNode(10)
                )
                .setDataStorageConfiguration(
                        new DataStorageConfiguration()
                                .setDataRegionConfigurations(
                                        new DataRegionConfiguration()
                                                .setName("memory")
                                                .setMaxSize(110L * 1024 * 1024)
                                                .setMetricsEnabled(true)
//                                        ,
//                                        new DataRegionConfiguration()
//                                                .setName("persistence")
//                                                .setPersistenceEnabled(true)
                                )
                )
                .setMetricExporterSpi(
                        new JmxMetricExporterSpi()
                )
                ;
    }

    @Data
    @AllArgsConstructor
    public static class Key {

        private String s;

        @AffinityKeyMapped
        private int i;
    }

}
