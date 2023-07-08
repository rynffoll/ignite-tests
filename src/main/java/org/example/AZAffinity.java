package org.example;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeAffinityBackupFilter;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AZAffinity {

    public static void main(String[] args) throws InterruptedException {

//        System.setProperty("IGNITE_MBEAN_APPEND_JVM_ID", "true");
        System.setProperty("IGNITE_MBEAN_APPEND_CLASS_LOADER_ID", "false");

        Ignite ds_1 = G.start(
                config("ds-1", "ds")
                        .setMetricExporterSpi(
                                new JmxMetricExporterSpi()
                        )
        );
        Ignite ds_2 = G.start(config("ds-2", "ds"));
        Ignite ds_3 = G.start(config("ds-3", "ds"));

        Ignite m1_1 = G.start(config("m1-1", "m1"));
        Ignite m1_2 = G.start(config("m1-2", "m1"));
        Ignite m1_3 = G.start(config("m1-3", "m1"));

        Ignite client = G.start(
                new IgniteConfiguration()
                        .setIgniteInstanceName("client")
                        .setConsistentId("client")
                        .setClientMode(true)
        );

        client.cluster().state(ClusterState.ACTIVE);
        client.cluster().baselineAutoAdjustEnabled(false);

        IgniteCache<Integer, Integer> cache = client.getOrCreateCache(cache());

        Affinity<Object> affinity = client.affinity(cache.getName());

        printPartitionMap(client);

        G.stop("m1-1", false);

        TimeUnit.SECONDS.sleep(5);

        System.out.println("----- STOP -----");

        printPartitionMap(client);

        System.exit(1);
    }

    public static void printPartitionMap(Ignite client) {
        Affinity<Object> affinity = client.affinity("cache");
        List<String> partitionMap = client.cluster()
                .forServers()
                .nodes()
                .stream()
                .map(n -> String.format("%s :: %s :: P=%-50s : B=%-50s",
                        n.attribute("dc"),
                        n.consistentId(),
                        Arrays.toString(affinity.primaryPartitions(n)),
                        Arrays.toString(affinity.backupPartitions(n))
                ))
                .sorted()
                .collect(Collectors.toList());

        partitionMap.forEach(System.out::println);
    }

    public static CacheConfiguration<Integer, Integer> cache() {
        return new CacheConfiguration<Integer, Integer>("cache")
                .setBackups(1)
                .setAffinity(
                        new RendezvousAffinityFunction()
                                .setPartitions(16)
                                .setAffinityBackupFilter(new ClusterNodeAttributeAffinityBackupFilter("dc"))
                );
    }

    public static IgniteConfiguration config(String name, String cell) {
        return new IgniteConfiguration()
                .setIgniteInstanceName(name)
                .setConsistentId(name)
                .setUserAttributes(Collections.singletonMap("dc", cell))
                ;
    }

}
