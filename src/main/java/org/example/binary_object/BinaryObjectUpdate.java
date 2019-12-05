package org.example.binary_object;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;

public class BinaryObjectUpdate {

    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("server.xml")) {
            IgniteCache<Integer, BinaryObject> cache = ignite.getOrCreateCache(
                    new CacheConfiguration<>("test")
            ).withKeepBinary();

            BinaryObject value = ignite.binary().builder("testVal")
                    .setField("name", "John Doe", String.class)
                    .setField("login", "john_doe", String.class)
                    .setField("age", 26, Integer.class)
                    .build();
            cache.put(0, value);

            System.out.println(cache.get(0));

            cache.invoke(
                    0,
                    (CacheEntryProcessor<Integer, BinaryObject, Void>) (entry, arguments) -> {
                        BinaryObjectBuilder builder = entry.getValue().toBuilder();

                        // error
                        // javax.cache.processor.EntryProcessorException: java.lang.ClassCastException: java.lang.String cannot be cast to org.apache.ignite.binary.BinaryObjectBuilder
                        builder.setField("name", builder.getField("login"));
                        // ok
//                    builder.setField("name", (String) builder.getField("login"));
                        // ok
//                    builder.setField("name", builder.getField("login"), String.class);

                        entry.setValue(builder.build());

                        return null;
                    }
            );

            System.out.println(cache.get(0));
        }
    }

}
