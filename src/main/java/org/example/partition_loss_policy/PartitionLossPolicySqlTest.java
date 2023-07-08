package org.example.partition_loss_policy;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.G;

import javax.cache.Cache;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PartitionLossPolicySqlTest {

    public static void main(String[] args) throws InterruptedException {
        Ignite server1 = G.start(config("server1"));
        Ignite server2 = G.start(config("server2"));

        Ignite client = G.start(
                new IgniteConfiguration()
                        .setIgniteInstanceName("client")
                        .setConsistentId("client")
                        .setClientMode(true)
                        .setIncludeEventTypes(
                                EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST
                        )
        );

        server2.cluster().state(ClusterState.ACTIVE);
        server2.cluster().baselineAutoAdjustEnabled(false);
//        server2.cluster().baselineAutoAdjustTimeout(TimeUnit.MINUTES.toMillis(5));

        System.out.println(">>>>>>>>> BLT: " + server1.cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId).collect(Collectors.toList()));

        server1.destroyCache("p");
        server1.destroyCache("m");

        IgniteCache<Integer, Value> cache = client.getOrCreateCache(
                new CacheConfiguration<Integer, Value>("m")
                        .setDataRegionName("memory")
                        .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
                        .setQueryEntities(Collections.singletonList(
                                new QueryEntity(Integer.class, Value.class)
                                        .setTableName("m")
                        ))
                        .setAffinity(
                                new RendezvousAffinityFunction()
                                        .setPartitions(32)
                        )
        );

        int endExclusive = 100;
        IntStream.range(0, endExclusive).forEach(i -> cache.put(i, new Value(Integer.toString(i), i)));
        IntStream.range(0, endExclusive).forEach(i -> cache.put(endExclusive + i, new Value(Integer.toString(endExclusive + i), i)));

        IntStream.range(0, endExclusive).forEach(i -> {
            try {
                List<List<?>> result = cache.query(new SqlFieldsQuery("select _val from m where i = ?").setArgs(i)).getAll();
                System.out.printf(">>>>>>>>>> SQL(%d): %s%n", i, result);
            } catch (Exception e) {
                System.out.printf(">>>>>>>>>> SQL(%d): %s%n", i, e.getMessage());
            }
        });

        G.stop("server2", true);

        TimeUnit.SECONDS.sleep(5L);

        Map<String, Collection<Integer>> lostPartitions = client.cacheNames().stream()
                .map(client::cache)
                .collect(Collectors.toMap(Cache::getName, IgniteCache::lostPartitions));
        System.out.println(">>>>>>>>> Lost partitions: " + lostPartitions);

        IntStream.range(0, endExclusive).forEach(i -> {
            try {
                List<List<?>> result = cache.query(new SqlFieldsQuery("select _val from m where s = ?").setArgs(i)).getAll();
                System.out.printf(">>>>>>>>>> SQL(%d): %s%n", i, result);
            } catch (Exception e) {
                System.out.printf(">>>>>>>>>> SQL(%d): %s%n", i, e.getMessage());
            }
        });

        server2 = G.start(config("server2"));

        client.resetLostPartitions(Collections.singletonList("m"));

        lostPartitions = client.cacheNames().stream()
                .map(client::cache)
                .collect(Collectors.toMap(Cache::getName, IgniteCache::lostPartitions));
        System.out.println(">>>>>>>>> Lost partitions: " + lostPartitions);

        IntStream.range(0, endExclusive).forEach(i -> {
            try {
                List<List<?>> result = cache.query(new SqlFieldsQuery("select _val from m where s = ?").setArgs(i)).getAll();
                System.out.printf(">>>>>>>>>> SQL(%d): %s%n", i, result);
            } catch (Exception e) {
                System.out.printf(">>>>>>>>>> SQL(%d): %s%n", i, e.getMessage());
            }
        });
    }

    public static IgniteConfiguration config(String name) {
        return new IgniteConfiguration()
                .setIgniteInstanceName(name)
                .setConsistentId(name)
                .setDataStorageConfiguration(
                        new DataStorageConfiguration()
                                .setDataRegionConfigurations(
                                        new DataRegionConfiguration()
                                                .setName("memory")
//                                        ,
//                                        new DataRegionConfiguration()
//                                                .setName("persistence")
//                                                .setPersistenceEnabled(true)
                                )
                )
                ;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Value {
        @QuerySqlField
        private String s;
        @QuerySqlField(index = true)
        private int i;
    }

}
