package org.example.partition_loss_policy;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class PartitionLossPolicyTest {

    public static void main(String[] args) {
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
        server2.cluster().baselineAutoAdjustEnabled(true);
        server2.cluster().baselineAutoAdjustTimeout(TimeUnit.MINUTES.toMillis(5));

        System.out.println(">>>>>>>>> BLT: " + server1.cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId).collect(Collectors.toList()));

        server1.destroyCache("p");
        server1.destroyCache("m");

        IgniteCache<Integer, Integer> pCache = client.getOrCreateCache(
                new CacheConfiguration<Integer, Integer>("p")
                        .setDataRegionName("persistence")
                        .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
                        .setQueryEntities(Collections.singletonList(
                                new QueryEntity(Integer.class, Integer.class)
                                        .setTableName("p")
                        ))
                        .setAffinity(
                                new RendezvousAffinityFunction()
                                        .setPartitions(32)
                        )
        );

        IgniteCache<Integer, Integer> mCache = client.getOrCreateCache(
                new CacheConfiguration<Integer, Integer>("m")
                        .setDataRegionName("memory")
                        .setPartitionLossPolicy(PartitionLossPolicy.IGNORE)
                        .setQueryEntities(Collections.singletonList(
                                new QueryEntity(Integer.class, Integer.class)
                                        .setTableName("m")
                        ))
                        .setAffinity(
                                new RendezvousAffinityFunction()
                                        .setPartitions(32)
                        )
        );

        range(i -> {
            pCache.put(i, i);
            mCache.put(i, i);
        });

        range(pCache::get);
        range(mCache::get);

        G.stop("server2", true);

        kv(pCache);
        kv(mCache);

        sql(pCache);
        sql(mCache);

        server2 = G.start(config("server2"));

        System.out.println(">>>>>>>>> BLT: " + server2.cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId).collect(Collectors.toList()));

        kv(pCache);
        kv(mCache);

        sql(pCache);
        sql(mCache);

        Map<String, Collection<Integer>> lostPartitions = client.cacheNames().stream()
                .map(client::cache)
                .collect(Collectors.toMap(Cache::getName, IgniteCache::lostPartitions));
        System.out.println(">>>>>>>>> Lost partitions: " + lostPartitions);
        client.resetLostPartitions(Stream.of("m", "p").collect(Collectors.toList()));
        System.out.println(">>>>>>>>> Reset lost partitions: " + server2.cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId).collect(Collectors.toList()));

        kv(pCache);
        kv(mCache);

        sql(pCache);
        sql(mCache);

        System.exit(1);
    }

    public static void range(IntConsumer consumer) {
        IntStream.range(0, 1_000).forEach(consumer);
    }
    
    public static void kv(IgniteCache<Integer, Integer> cache) {
        String name = cache.getName();
        try {
            range(cache::get);
            System.out.printf(">>>>>>>>>> GET: %s :: ok%n", name);
        } catch (Exception e) {
            System.out.printf(">>>>>>>>>> GET: %s :: %s%n", name, e.getMessage());
        }
    }

    public static void sql(IgniteCache<Integer, Integer> cache) {
        String name = cache.getName();
        try {
            cache.query(new SqlFieldsQuery("select * from " + name)).getAll();
            System.out.printf(">>>>>>>>>> SQL: %s :: ok%n", name);
        } catch (Exception e) {
            System.out.printf(">>>>>>>>>> SQL: %s :: %s%n", name, e.getMessage());
        }
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
                                        ,
                                        new DataRegionConfiguration()
                                                .setName("persistence")
                                                .setPersistenceEnabled(true)
                                )
                )
                ;
    }

}
