package org.example;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.BaselineNode;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Client1 {

    private static final int N = 10_000;

    public static void main(String[] args) throws InterruptedException {
        Ignite ignite = Cluster.ignite("Client1", true);
        IgniteCluster cluster = ignite.cluster();

        if (!cluster.active()) {
            cluster.active(true);
        }

        List<Object> baseline = cluster.currentBaselineTopology()
                .stream()
                .map(BaselineNode::consistentId)
                .collect(Collectors.toList());

        System.out.format("Baseline: %s%n", baseline);

        IgniteCache<Integer, UUID> cache = ignite.cache(Cluster.CACHE);
        for (int i = 0; i < N; i++) {
            cache.put(i, UUID.randomUUID());
        }
        System.out.format("%d records inserted%n", N);

        for (int i = 0; i < 100; i++) {
            int all = cache.size(CachePeekMode.ALL);
            int primary = cache.size(CachePeekMode.PRIMARY);
            int backup = cache.size(CachePeekMode.BACKUP);
            int query = cache.query(new ScanQuery<>()).getAll().size();

            System.out.format("%s :: [%d] all=%d, primary=%d, backup=%d, query=%d%n",
                    LocalDateTime.now(), i, all, primary, backup, query);

            TimeUnit.SECONDS.sleep(10);
        }
    }

}
