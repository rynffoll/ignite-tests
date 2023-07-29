package org.example.warmup.plugin;

import org.apache.ignite.configuration.WarmUpConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;

public class LoadAllWarmUpParallelConfiguration implements WarmUpConfiguration {

    private static final long serialVersionUID = 0L;

    @Override
    public String toString() {
        return S.toString(LoadAllWarmUpParallelConfiguration.class, this);
    }

}
