package org.example.pure_in_memory;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;

import java.util.Objects;
import java.util.UUID;
import java.util.stream.IntStream;

public class PureInMemory {

    public static void main(String[] args) {
//        Ignite server = G.start("pure_in_memory.xml");
        IgniteConfiguration base = new IgniteConfiguration()
                .setDataStorageConfiguration(
                        new DataStorageConfiguration()
                                .setDefaultDataRegionConfiguration(
                                        new DataRegionConfiguration()
                                                .setName("Memory Region")
                                )
                                .setMetricsEnabled(true)
                )
                ;

        Ignite server = G.start(
                new IgniteConfiguration(base)
                        .setConsistentId("server")
                        .setIgniteInstanceName("server")
                        .setMetricExporterSpi(
                                new JmxMetricExporterSpi()
                        )
        );

        Ignite server2 = G.start(
                new IgniteConfiguration(base)
                        .setIgniteInstanceName("server2")
                        .setConsistentId("server2")
                        .setMetricExporterSpi(
                                new JmxMetricExporterSpi()
                        )
        );

        Ignite server3 = G.start(
                new IgniteConfiguration(base)
                        .setIgniteInstanceName("server3")
                        .setConsistentId("server3")
                        .setMetricExporterSpi(
                                new JmxMetricExporterSpi()
                        )
        );

        Ignite client = G.start(
                new IgniteConfiguration(base)
                        .setIgniteInstanceName("client")
                        .setConsistentId("client")
                        .setMetricExporterSpi(
                                new JmxMetricExporterSpi()
                        )
        );

        server.cluster().baselineAutoAdjustEnabled(false);

        IgniteCache<Integer, UUID> test = client.getOrCreateCache(new CacheConfiguration<>("test"));

        IntStream.range(0, 1000).forEach(i -> test.put(i, UUID.randomUUID()));

        System.out.println(
                IntStream.range(0, 1000)
                        .mapToObj(test::get)
                        .filter(Objects::nonNull)
                        .count()
        );

        System.out.println();

        G.stop("server3", true);

        System.out.println(
                IntStream.range(0, 1000)
                        .mapToObj(test::get)
                        .filter(Objects::nonNull)
                        .count()
        );

        System.out.println();
    }

}
