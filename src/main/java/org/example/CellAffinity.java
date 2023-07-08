package org.example;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CellAffinity {

    public static void main(String[] args) throws InterruptedException {
        Ignite ds_1 = G.start(config("ds-1", "cell1"));
        Ignite ds_2 = G.start(config("ds-2", "cell2"));
        Ignite ds_3 = G.start(config("ds-3", "cell3"));

        Ignite m1_1 = G.start(config("m1-1", "cell1"));
        Ignite m1_2 = G.start(config("m1-2", "cell2"));
        Ignite m1_3 = G.start(config("m1-3", "cell3"));

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
                        n.attribute("cell"),
                        n.consistentId(),
                        Arrays.toString(affinity.primaryPartitions(n)),
                        Arrays.toString(affinity.backupPartitions(n))
                ))
//                .map(n -> {
//                    return String.format("%s :: %s :: %s",
//                            n.attribute("cell"),
//                            n.consistentId(),
//                            Arrays.toString(affinity.allPartitions(n))
//                    );
//                })
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
                                .setAffinityBackupFilter((ClusterNode candidate, List<ClusterNode> previouslySelected) -> {
                                    String primaryAttrVal = previouslySelected.get(0).attribute("cell");
                                    String candidateAttrVal = candidate.attribute("cell");

                                    if (primaryAttrVal == null || candidateAttrVal == null)
                                        throw new IllegalStateException("Empty co-location attribute value");

                                    return primaryAttrVal.equals(candidateAttrVal);
                                })
                );
    }

    public static IgniteConfiguration config(String name, String cell) {
        return new IgniteConfiguration()
                .setIgniteInstanceName(name)
                .setConsistentId(name)
                .setUserAttributes(Collections.singletonMap("cell", cell))
                .setPeerClassLoadingEnabled(true)
                .setDiscoverySpi(
                        new ZookeeperDiscoverySpi()
                                .setZkConnectionString("localhost:2181")
                )
                .setDataStorageConfiguration(
                        new DataStorageConfiguration()
                                .setDataRegionConfigurations(
                                        new DataRegionConfiguration()
                                                .setName("Memory Region")
                                )
                )
                ;
    }

}
