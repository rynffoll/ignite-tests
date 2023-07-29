package org.example.warmup;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.*;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.example.warmup.plugin.LoadAllWarmUpParallelConfiguration;
import org.example.warmup.plugin.LoadAllWarmUpParallelPluginProvider;
import org.springframework.util.unit.DataSize;

import java.util.stream.IntStream;

@Slf4j
public class WarmUp {

    static {
        System.setProperty("IGNITE_UPDATE_NOTIFIER", "false");
    }

    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start(
                new IgniteConfiguration()
                        .setIgniteInstanceName("WarmUp-1")
                        .setConsistentId("WarmUp-1")
                        .setGridLogger(
                                new Slf4jLogger()
                        )
                        .setShutdownPolicy(
                                ShutdownPolicy.GRACEFUL
                        )
                        .setDataStorageConfiguration(
                                new DataStorageConfiguration()
                                        .setDataRegionConfigurations(
                                                new DataRegionConfiguration()
                                                        .setName("pds")
                                                        .setMaxSize(
                                                                DataSize.ofGigabytes(5).toBytes()
                                                        )
                                                        .setWarmUpConfiguration(
//                                                                new LoadAllWarmUpConfiguration()
                                                                new LoadAllWarmUpParallelConfiguration()
                                                        )
                                                        .setPersistenceEnabled(true)
                                                        .setMetricsEnabled(true)
                                        )
                                        .setCheckpointFrequency(1_000)
                                        .setWalMode(WALMode.LOG_ONLY)
                                        .setWalCompactionEnabled(true)
//                                        .setWalPageCompression(DiskPageCompression.SNAPPY)
//                                        .setWriteThrottlingEnabled(true)
                                        .setMetricsEnabled(true)
                        )
                        .setMetricExporterSpi(
//                                new LogExporterSpi()
                        )
                        .setPluginProviders(
                                new LoadAllWarmUpParallelPluginProvider()
                        )
        )) {

            if (ignite.cluster().state() == ClusterState.INACTIVE) {
                ignite.cluster().state(ClusterState.ACTIVE);
            }

            IntStream.range(0, 10).forEach(
                    i -> {
                        IgniteCache<BinaryObject, BinaryObject> cache = ignite.getOrCreateCache(
                                new CacheConfiguration<>()
                                        .setName("test_" + i)
                                        .setDataRegionName("pds")
                                        .setAffinity(
                                                new RendezvousAffinityFunction()
                                                        .setPartitions(100)
                                        )
                                        .setBackups(0)
                                        .setCacheMode(CacheMode.PARTITIONED)
                                        .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                                        .setStatisticsEnabled(true)
                        ).withKeepBinary();

                        IntStream.range(0, 100_000)
                                .parallel()
                                .forEach(j -> {
                                    BinaryObject key = ignite.binary()
                                            .builder("testKey")
                                            .setField("name", Integer.toString(j), String.class)
                                            .setField("index", j, int.class)
                                            .build();

                                    BinaryObject value = ignite.binary()
                                            .builder("testValue")
                                            .setField("bytes", new byte[4 * 1024], byte[].class)
                                            .build();

                                    cache.putAsync(key, value);
                                });
                    }
            );

        }
    }

}
