package org.example.bo_ClassNotFoundException;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Collections;

public class BO_ClassNotFoundException {

    public static void main(String[] args) {
        Ignite server = G.start(
                new IgniteConfiguration()
                        .setConsistentId("server")
                        .setIgniteInstanceName("server")
                        .setDiscoverySpi(
                                new TcpDiscoverySpi()
                                        .setIpFinder(
                                                new TcpDiscoveryVmIpFinder()
                                                        .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))
                                        )
                        )
        );

        Ignite client = G.start(
                new IgniteConfiguration()
                        .setConsistentId("client")
                        .setIgniteInstanceName("client")
                        .setDiscoverySpi(
                                new TcpDiscoverySpi()
                                        .setIpFinder(
                                                new TcpDiscoveryVmIpFinder()
                                                        .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))
                                        )
                        )
        );

        IgniteCache<Key, Val> cache = client.getOrCreateCache(
                new CacheConfiguration<>("test")
        );

        Key key = new Key("sk", 1);
        Val val = new Val("sv", 1);

        cache.put(key, val);

        Val val1 = cache.get(key);

        System.out.println();
    }

    @Data
    @AllArgsConstructor
    public static class Key {
        private String sk;
        private int ik;
    }

    @Data
    @AllArgsConstructor
    public static class Val {
        private String sv;
        private int iv;
    }
}
