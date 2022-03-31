package com.github.aesteve.kafka.streams.examples.utils;

import java.util.Random;

public interface RngUtils {

    static int gaussianIdxIn(Random random, int arraySize) {
        var idx = Math.round(random.nextGaussian()*0.5* arraySize / 2 + (arraySize / 2));
        if (idx < 0 || idx >= arraySize) {
            return gaussianIdxIn(random, arraySize); // remove outliers
        }
        return (int) idx;
    }

}
