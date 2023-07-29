package org.example.warmup.plugin;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.warmup.WarmUpStrategySupplier;
import org.apache.ignite.plugin.*;

import java.io.Serializable;
import java.util.UUID;

public class LoadAllWarmUpParallelPluginProvider implements PluginProvider<PluginConfiguration> {

    private LoadAllWarmUpParallelPlugin plugin;

    @Override
    public String name() {
        return "LoadAllWarmUpParallelPlugin";
    }

    @Override
    public String version() {
        return "0.1";
    }

    @Override
    public String copyright() {
        return "John Doe";
    }

    @Override
    public LoadAllWarmUpParallelPlugin plugin() {
        return plugin;
    }

    @Override
    public void initExtensions(PluginContext ctx, ExtensionRegistry registry) throws IgniteCheckedException {
        plugin = new LoadAllWarmUpParallelPlugin(
                ctx.log(LoadAllWarmUpParallelPlugin.class),
                () -> ((IgniteKernal) ctx.grid()).context().cache().cacheGroups()
        );

        registry.registerExtension(WarmUpStrategySupplier.class, plugin);
    }

    @Override
    public <T> T createComponent(PluginContext ctx, Class<T> cls) {
        return null;
    }

    @Override
    public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return null;
    }

    @Override
    public void start(PluginContext ctx) throws IgniteCheckedException {

    }

    @Override
    public void stop(boolean cancel) throws IgniteCheckedException {

    }

    @Override
    public void onIgniteStart() throws IgniteCheckedException {

    }

    @Override
    public void onIgniteStop(boolean cancel) {

    }

    @Override
    public Serializable provideDiscoveryData(UUID nodeId) {
        return null;
    }

    @Override
    public void receiveDiscoveryData(UUID nodeId, Serializable data) {

    }

    @Override
    public void validateNewNode(ClusterNode node) throws PluginValidationException {

    }

}
