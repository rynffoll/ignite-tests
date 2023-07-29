package org.example.warmup.plugin;

import com.google.common.base.Supplier;
import lombok.RequiredArgsConstructor;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.warmup.WarmUpStrategy;
import org.apache.ignite.internal.processors.cache.warmup.WarmUpStrategySupplier;
import org.apache.ignite.plugin.IgnitePlugin;

import java.util.Arrays;
import java.util.Collection;

@RequiredArgsConstructor
public class LoadAllWarmUpParallelPlugin implements IgnitePlugin, WarmUpStrategySupplier {

    private final IgniteLogger log;
    private final Supplier<Collection<CacheGroupContext>> cacheGroupContextsSupplier;

    @Override
    public Collection<WarmUpStrategy<?>> strategies() {
        return Arrays.asList(
                new LoadAllWarmUpParallelStrategy(log, cacheGroupContextsSupplier)
        );
    }

}
