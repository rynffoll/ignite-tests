package org.example.clear_vs_destroy;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class ClearMain {

    public static void main(String[] args) {
        System.setProperty("IGNITE_UPDATE_NOTIFIER", "false");
        System.setProperty("IGNITE_MBEAN_APPEND_CLASS_LOADER_ID", "false");

        Ignite ignite = G.start(
                new IgniteConfiguration()
                        .setConsistentId("server")
                        .setIgniteInstanceName("server")
                        .setDataStorageConfiguration(
                                new DataStorageConfiguration()
                                        .setDefaultDataRegionConfiguration(
                                                new DataRegionConfiguration()
                                                        .setName("Memory")
                                        )
//                                        .setDataRegionConfigurations(
//                                                new DataRegionConfiguration()
//                                                        .setName("test")
//                                                        .setMetricsEnabled(true)
//                                        )
                        )

        );

        Ignite ignite2 = G.start(
                new IgniteConfiguration()
                        .setConsistentId("server2")
                        .setIgniteInstanceName("server2")
                        .setDataStorageConfiguration(
                                new DataStorageConfiguration()
                                        .setDefaultDataRegionConfiguration(
                                                new DataRegionConfiguration()
                                                        .setName("Memory")
                                        )
//                                        .setDataRegionConfigurations(
//                                                new DataRegionConfiguration()
//                                                        .setName("test")
//                                                        .setPersistenceEnabled(true)
//                                                        .setMetricsEnabled(true)
//                                        )
                        )

        );


        for (int i = 0; i < 1_000; i++) {

            IgniteCache<Integer, Value> cache = ignite.getOrCreateCache(
                    new CacheConfiguration<Integer, Value>("test")
                            .setDataRegionName("test")
                            .setQueryEntities(
                                    Collections.singletonList(
                                            new QueryEntity(int.class, Value.class)
                                    )
                            )
                            .setExpiryPolicyFactory(
                                    CreatedExpiryPolicy.factoryOf(
                                            new Duration(TimeUnit.DAYS, 3)
                                    )
                            )
            );

            System.out.printf("%4d :: %20s: %10d / %10d / %10d%n", i, "before fill", cache.size(), totalUsedPages(ignite), totalUsedSize(ignite));

            fill(ignite, cache);

            System.out.printf("%4d :: %20s: %10d / %10d / %10d%n", i, "after fill", cache.size(), totalUsedPages(ignite), totalUsedSize(ignite));

//            cache.destroy();
//
//            System.out.printf("%4d :: %20s: %10d / %10d / %10d%n", i, "after destroy", -1, totalUsedPages(ignite), totalUsedSize(ignite));

            cache.clear();

            System.out.printf("%4d :: %20s: %10d / %10d / %10d%n", i, "after clear", cache.size(), totalUsedPages(ignite), totalUsedSize(ignite));
        }

    }

    private static long totalUsedPages(Ignite ignite) {
        return ignite.dataRegionMetrics()
                .stream()
                .filter(dr -> dr.getName().equals("test"))
                .map(DataRegionMetrics::getTotalUsedPages)
                .findFirst()
                .orElse(-1L);
    }

    private static long totalUsedSize(Ignite ignite) {
        return ignite.dataRegionMetrics()
                .stream()
                .filter(dr -> dr.getName().equals("test"))
//                .map(DataRegionMetrics::getTotalUsedSize)
                .map(x -> 0L)
                .findFirst()
                .orElse(-1L);
    }

    private static void fill(Ignite ignite, IgniteCache<Integer, Value> cache) {
        IntStream.range(0, 100_000)
                .mapToObj(i -> new Value(i + "", i))
                .forEach(v -> cache.put(v.getI(), v));
    }

    @Data
    @AllArgsConstructor
    public static class Value {
        @QuerySqlField
        private String s;
        @QuerySqlField
        private int i;
    }
}
