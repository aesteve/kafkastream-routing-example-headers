package com.github.aesteve.kafka.streams.examples;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static com.github.aesteve.kafka.streams.examples.utils.RngUtils.gaussianIdxIn;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RngTest {

    @Test
    void gaussianInRageMustNotOverflowOrUnderflow() { // use PBT here
        var random = new Random();
        var upperBound = 100;
        var distrib = new int[upperBound];
        for (int i = 0; i < 100_000; i++) {
            var res = gaussianIdxIn(random, upperBound);
            assertTrue(res >= 0);
            assertTrue(res < upperBound);
            var old = distrib[res];
            distrib[res] = ++old;
        }
        for (int i = 0; i < distrib.length; i++) {
            System.out.println(i + ";" + distrib[i]);
        }
    }


}
