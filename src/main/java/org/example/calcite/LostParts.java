package org.example.calcite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class LostParts {

    public static void main(String[] args) {
        Ignite s1 = Ignition.start(config("s1"));
        Ignite s2 = Ignition.start(config("s2"));

        s1.cluster().baselineAutoAdjustEnabled(false);
        Collection<BaselineNode> blt = s1.cluster().currentBaselineTopology();

        System.out.println("BLT = " + blt.stream().map(BaselineNode::consistentId).collect(Collectors.toList()));


        assert blt.size() == 2;

//        Ignite thick = Ignition.start(config("c1").setClientMode(true));

        IgniteClient thin = Ignition.startClient(
                new ClientConfiguration()
                        .setAddresses("localhost:10800", "localhost:10801")
        );

        try (FieldsQueryCursor<List<?>> cursor = thin.query(new SqlFieldsQuery("create table test (id int, str varchar)"))) {
            for (List<?> row : cursor) {
                System.out.println(row);
            }
        }
        System.out.println("Created");

        int inserted = 10;
        String cacheName = "SQL_PUBLIC_TEST";
        for (int i = 0; i < inserted; i++) {
            SqlFieldsQuery query = new SqlFieldsQuery("insert into test (id, str) values (?, ?)")
                    .setArgs(i, Integer.toString(i));

            try (FieldsQueryCursor<List<?>> cursor = thin.query(query)) {
                for (List<?> row : cursor) {
                    System.out.println(i + " => " + s1.affinity(cacheName).mapKeyToNode(i).consistentId());
                }
            }
        }
        System.out.println("Inserted = " + inserted);

        try (FieldsQueryCursor<List<?>> cursor = thin.query(new SqlFieldsQuery("select /*+ QUERY_ENGINE('calcite') */ * from test"))) {
            int size = 0;
            for (List<?> row : cursor) {
                System.out.println(row);
                size++;
            }
            System.out.println("size(calcite) = " + size);
        }

        Ignition.stop("s2", true);

        System.out.println("Lost parts = " + s1.cache(cacheName).lostPartitions());

        try (FieldsQueryCursor<List<?>> cursor = thin.query(new SqlFieldsQuery("select /*+ QUERY_ENGINE('calcite') */ * from test"))) {
            int size = 0;
            for (List<?> row : cursor) {
                System.out.println(row);
                size++;
            }
            System.out.println("size(calcite) = " + size);
        }

        try (FieldsQueryCursor<List<?>> cursor = thin.query(new SqlFieldsQuery("select /*+ QUERY_ENGINE('h2') */ * from test"))) {
            int size = 0;
            for (List<?> row : cursor) {
                System.out.println(row);
                size++;
            }
            System.out.println("size(h2) = " + size);
        } catch (Exception e) {
            System.err.println("QUERY_ENGINE('h2') = " + e.getMessage());
            e.printStackTrace();
        }


        System.out.println();
    }

    private static IgniteConfiguration config(String instanceName) {
        return new IgniteConfiguration()
                .setIgniteInstanceName(instanceName)
                .setConsistentId(instanceName)
                .setDiscoverySpi(
                        new TcpDiscoverySpi()
                                .setIpFinder(
                                        new TcpDiscoveryVmIpFinder()
                                                .setAddresses(Collections.singletonList("localhost:47500..47501"))
                                )
                )
                .setSqlConfiguration(
                        new SqlConfiguration()
                                .setQueryEnginesConfiguration(
                                        new CalciteQueryEngineConfiguration()
                                                .setDefault(true),
                                        new IndexingQueryEngineConfiguration() // H2
                                )
                )
                ;
    }

}
