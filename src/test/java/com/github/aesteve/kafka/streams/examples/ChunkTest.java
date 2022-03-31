package com.github.aesteve.kafka.streams.examples;

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static com.github.aesteve.kafka.streams.examples.utils.CollectionUtils.chunked;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ChunkTest {

    @Test
    void chunkAList() {
        var list = IntStream.range(0, 10).boxed().toList();
        var chunked = chunked(list, 2);
        assertEquals(5, chunked.size());
        for (var chunk : chunked) {
            assertEquals(2, chunk.size());
        }
    }
}
