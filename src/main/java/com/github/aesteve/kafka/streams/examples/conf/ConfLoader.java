package com.github.aesteve.kafka.streams.examples.conf;

import java.io.IOException;
import java.util.Properties;

public class ConfLoader {

    public static Properties fromResources(String name) throws IOException {
        var props = new Properties();
        props.load(ConfLoader.class.getClassLoader().getResourceAsStream(name));
        return props;
    }

}
