package org.example.blt;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Collections;
import java.util.stream.Collectors;

public class BLT_PureInMemory {

    public static void main(String[] args) {
        Ignite server1 = Ignition.start(
                new IgniteConfiguration()
                        .setConsistentId("server1")
                        .setIgniteInstanceName("server1")
                        .setDiscoverySpi(
                                new TcpDiscoverySpi()
                                        .setIpFinder(
                                                new TcpDiscoveryVmIpFinder()
                                                        .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))
                                        )
                        )
        );

        Ignite server2 = Ignition.start(
                new IgniteConfiguration()
                        .setIgniteInstanceName("server2")
                        .setConsistentId("server2")
                        .setDiscoverySpi(
                                new TcpDiscoverySpi()
                                        .setIpFinder(
                                                new TcpDiscoveryVmIpFinder()
                                                        .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))
                                        )
                        )
        );

        Ignite server3 = Ignition.start(
                new IgniteConfiguration()
                        .setIgniteInstanceName("server3")
                        .setConsistentId("server3")
                        .setDiscoverySpi(
                                new TcpDiscoverySpi()
                                        .setIpFinder(
                                                new TcpDiscoveryVmIpFinder()
                                                        .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))
                                        )
                        )
        );

        printBLT(server1);
        printBLT(server2);
        printBLT(server3);

        long topologyVersion = server1.cluster().topologyVersion();
        server1.cluster().baselineAutoAdjustEnabled(false);
        server1.cluster().setBaselineTopology(topologyVersion);

        printBLT(server1);
        printBLT(server2);
        printBLT(server3);

        System.out.println();

        printBLT(server1);
        printTopology(server1);

        G.stop("server3", true);
        System.out.println("Stop server3");

        printBLT(server1);
        printTopology(server1);

        System.out.println();

        server3 = Ignition.start(
                new IgniteConfiguration()
                        .setIgniteInstanceName("server3")
                        .setConsistentId("server3")
                        .setDiscoverySpi(
                                new TcpDiscoverySpi()
                                        .setIpFinder(
                                                new TcpDiscoveryVmIpFinder()
                                                        .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))
                                        )
                        )
        );

        printBLT(server1);
        printTopology(server1);

        System.out.println();
    }

    private static void printTopology(Ignite ignite) {
        long topologyVersion = ignite.cluster().topologyVersion();
        System.out.println(
                "Topology = " + ignite.cluster()
                        .topology(
                                topologyVersion
                        )
                        .stream()
                        .map(BaselineNode::consistentId)
                        .collect(Collectors.toList())

        );
    }

    private static void printBLT(Ignite ignite) {
        System.out.println(
                "BLT = " + ignite.cluster()
                        .currentBaselineTopology()
                        .stream()
                        .map(BaselineNode::consistentId)
                        .collect(Collectors.toList())

        );
    }

}
