package org.example.warmup.plugin;

import com.google.common.base.Supplier;
import lombok.RequiredArgsConstructor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.warmup.WarmUpStrategy;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;

@RequiredArgsConstructor
public class LoadAllWarmUpParallelStrategy implements WarmUpStrategy<LoadAllWarmUpParallelConfiguration> {

    @GridToStringExclude
    private final IgniteLogger log;

    @GridToStringExclude
    private final Supplier<Collection<CacheGroupContext>> cacheGroupContextsSupplier;


    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    private volatile boolean stop;

    @Override
    public Class<LoadAllWarmUpParallelConfiguration> configClass() {
        return LoadAllWarmUpParallelConfiguration.class;
    }

    @Override
    public void warmUp(
            LoadAllWarmUpParallelConfiguration cfg,
            DataRegion region
    ) throws IgniteCheckedException {
        if (stop)
            return;

        assert region.config().isPersistenceEnabled();

        Map<CacheGroupContext, List<LoadPartition>> loadDataInfo = loadDataInfo(region);

        long availableLoadPageCnt = availableLoadPageCount(region);

        if (log.isInfoEnabled()) {
            Collection<List<LoadPartition>> parts = loadDataInfo.values();

            log.info("Order of cache groups loaded into data region [name=" + region.config().getName()
                    + ", partCnt=" + parts.stream().mapToLong(Collection::size).sum()
                    + ", pageCnt=" + parts.stream().flatMap(Collection::stream).mapToLong(LoadPartition::pages).sum()
                    + ", availablePageCnt=" + availableLoadPageCnt + ", grpNames=" +
                    loadDataInfo.keySet().stream().map(CacheGroupContext::cacheOrGroupName).collect(toList()) + ']');
        }

        // FIXME
        long[] loadedPageCnt = {0};

        // FIXME
        List<Future<?>> futures = new ArrayList<>();

        for (Map.Entry<CacheGroupContext, List<LoadPartition>> e : loadDataInfo.entrySet()) {
            CacheGroupContext grp = e.getKey();
            List<LoadPartition> parts = e.getValue();

            if (log.isInfoEnabled()) {
                log.info("Start warm-up cache group, with estimated statistics [name=" + grp.cacheOrGroupName()
                        + ", partCnt=" + parts.size() + ", pageCnt="
                        + parts.stream().mapToLong(LoadPartition::pages).sum() + ']');
            }

            PageMemoryEx pageMemEx = (PageMemoryEx) region.pageMemory();

            for (LoadPartition part : parts) {
                Future<?> f = executorService.submit(() -> {
                    long pageId = pageMemEx.partitionMetaPageId(grp.groupId(), part.part());

                    for (int i = 0; i < part.pages(); i++, pageId++, loadedPageCnt[0]++) {
                        if (stop) {
                            if (log.isInfoEnabled()) {
                                log.info("Stop warm-up cache group with loaded statistics [name="
                                        + grp.cacheOrGroupName() + ", pageCnt=" + loadedPageCnt[0]
                                        + ", remainingPageCnt=" + (availableLoadPageCnt - loadedPageCnt[0]) + ']');
                            }

                            return;
                        }

                        long pagePtr = -1;

                        try {
                            pagePtr = pageMemEx.acquirePage(grp.groupId(), pageId);
                        } catch (IgniteCheckedException ex) {
                            throw new RuntimeException(ex);
                        } finally {
                            if (pagePtr != -1)
                                pageMemEx.releasePage(grp.groupId(), pageId, pagePtr);
                        }
                    }
                });

                futures.add(f);
            }

        }

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IgniteCheckedException(e);
            }
        }
    }

    @Override
    public void stop() throws IgniteCheckedException {
        stop = true;
    }

    @Override
    public String toString() {
        return S.toString(LoadAllWarmUpParallelStrategy.class, this);
    }

    /**
     * Getting count of pages available for loading into data region.
     *
     * @param region Data region.
     * @return Count(non - negative) of pages available for loading into data region.
     */
    protected long availableLoadPageCount(DataRegion region) {
        long maxSize = region.config().getMaxSize();
        long curSize = region.pageMemory().loadedPages() * region.pageMemory().systemPageSize();

        return Math.max(0, (maxSize - curSize) / region.pageMemory().systemPageSize());
    }

    /**
     * Calculation of cache groups, partitions and count of pages that can load
     * into data region. Calculation starts and includes an index partition for
     * each group.
     *
     * @param region Data region.
     * @return Loadable groups and partitions.
     * @throws IgniteCheckedException – if faild.
     */
    protected Map<CacheGroupContext, List<LoadPartition>> loadDataInfo(
            DataRegion region
    ) throws IgniteCheckedException {
        // Get cache groups of data region.
        List<CacheGroupContext> regionGrps = cacheGroupContextsSupplier.get().stream()
                .filter(grpCtx -> region.equals(grpCtx.dataRegion())).collect(toList());

        long availableLoadPageCnt = availableLoadPageCount(region);

        // Computing groups, partitions, and pages to load into data region.
        Map<CacheGroupContext, List<LoadPartition>> loadableGrps = new LinkedHashMap<>();

        for (int i = 0; i < regionGrps.size() && availableLoadPageCnt > 0; i++) {
            CacheGroupContext grp = regionGrps.get(i);

            // Index partition in priority.
            List<GridDhtLocalPartition> locParts = grp.topology().localPartitions();

            for (int j = -1; j < locParts.size() && availableLoadPageCnt > 0; j++) {
                int p = j == -1 ? INDEX_PARTITION : locParts.get(j).id();

                long partPageCnt = grp.shared().pageStore().pages(grp.groupId(), p);

                if (partPageCnt > 0) {
                    long pageCnt = (availableLoadPageCnt - partPageCnt) >= 0 ? partPageCnt : availableLoadPageCnt;

                    availableLoadPageCnt -= pageCnt;

                    loadableGrps.computeIfAbsent(grp, grpCtx -> new ArrayList<>()).add(new LoadPartition(p, pageCnt));
                }
            }
        }

        return loadableGrps;
    }

    /**
     * Information about loaded partition.
     */
    static class LoadPartition {
        /**
         * Partition id.
         */
        private final int part;

        /**
         * Number of pages to load.
         */
        private final long pages;

        /**
         * Constructor.
         *
         * @param part  Partition id.
         * @param pages Number of pages to load.
         */
        public LoadPartition(int part, long pages) {
            assert part >= 0 : "Partition id cannot be negative.";
            assert pages > 0 : "Number of pages to load must be greater than zero.";

            this.part = part;
            this.pages = pages;
        }

        /**
         * Return partition id.
         *
         * @return Partition id.
         */
        public int part() {
            return part;
        }

        /**
         * Return number of pages to load.
         *
         * @return Number of pages to load.
         */
        public long pages() {
            return pages;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return S.toString(LoadPartition.class, this);
        }
    }
}
