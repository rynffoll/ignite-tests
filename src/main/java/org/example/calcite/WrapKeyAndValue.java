package org.example.calcite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
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
import java.util.stream.Collectors;

public class WrapKeyAndValue {

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

        SqlFieldsQuery calciteDdl = new SqlFieldsQuery(
                "create table calcite (id int, str varchar) with \"wrap_key=false,wrap_value=false\""
        );
        thin.query(calciteDdl).getAll();

        SqlFieldsQuery h2Ddl = new SqlFieldsQuery(
                "create table /*+ QUERY_ENGINE('h2') */ h2 (id int, str varchar) with \"wrap_key=false,wrap_value=false\""
        );
        thin.query(h2Ddl).getAll();

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
