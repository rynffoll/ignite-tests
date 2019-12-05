package org.example.pds_expiration;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.EventType;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.util.Collections;
import java.util.UUID;

public class Cluster {

    public static final String CACHE = "test";

    public static Ignite ignite(String id, boolean client) {
        Ignite ignite = Ignition.start(igniteConfiguration(id, client));
        return ignite;
    }

    private static IgniteConfiguration igniteConfiguration(String id, boolean client) {
       return new IgniteConfiguration()
               .setConsistentId(id)
               .setClientMode(client)
               .setDiscoverySpi(discoverySpi())
               .setDataStorageConfiguration(dataStorageConfiguration())
               .setCacheConfiguration((cacheConfiguration()))
               .setIncludeEventTypes(EventType.EVT_CACHE_OBJECT_PUT)
               .setDeploymentMode(DeploymentMode.CONTINUOUS)
               .setPeerClassLoadingMissedResourcesCacheSize(0);
    }

    private static DiscoverySpi discoverySpi() {
        return new TcpDiscoverySpi()
                .setIpFinder(tcpDiscoveryIpFinder());
    }

    private static TcpDiscoveryIpFinder tcpDiscoveryIpFinder() {
        return new TcpDiscoveryVmIpFinder()
                .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));
    }

    private static CacheConfiguration<Integer, UUID> cacheConfiguration() {
        return new CacheConfiguration<Integer, UUID>(CACHE)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(1)
                .setCacheMode(CacheMode.PARTITIONED)
                .setManagementEnabled(true)
                .setStatisticsEnabled(true)
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ONE_MINUTE))
                .setEagerTtl(true)
                .setDataRegionName("test");
    }

    private static DataStorageConfiguration dataStorageConfiguration() {
        return new DataStorageConfiguration()
                .setWalMode(WALMode.FSYNC)
                .setWalCompactionEnabled(true)
                .setDataRegionConfigurations(dataRegionConfiguration());
    }

    private static DataRegionConfiguration dataRegionConfiguration() {
        return new DataRegionConfiguration()
                .setName("test")
//                .setPersistenceEnabled(true)
                .setMetricsEnabled(true);
    }
}
