package com.github.aesteve.kafka.streams.examples.utils;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

public interface CollectionUtils {


    static<T> Collection<List<T>> chunked(Stream<T> stream, int chunkSize) {
        var counter = new AtomicInteger();
        return stream
                .collect(groupingBy(i -> counter.getAndIncrement()/chunkSize))
                .values();
    }

    static <T> Collection<List<T>> chunked(Collection<T> collection, int chunkSize) {
        return chunked(collection.stream(), chunkSize);
    }

}
