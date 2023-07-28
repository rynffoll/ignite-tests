package org.example.pds_ignite_sys_cache_wal;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class IgniteSysCacheWal {

    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start(
                new IgniteConfiguration()
                        .setDataStorageConfiguration(
                                new DataStorageConfiguration()
                                        .setDataRegionConfigurations(
                                                new DataRegionConfiguration()
                                                        .setName("pds")
                                                        .setPersistenceEnabled(true)
                                        )
                        )
        )
        ) {
            String cacheName = "ignite-sys-cache";

            boolean walEnabled = ignite.cluster().isWalEnabled(cacheName);

            System.out.printf("cache=%s, wal=%b%n", cacheName, walEnabled);
        }

    }

}
