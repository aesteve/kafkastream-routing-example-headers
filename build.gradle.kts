plugins {
    java
}

group = "com.github.aesteve"
version = "0.0.1"

repositories {
    mavenCentral()
}

val kafkaVersion = "3.1.0"

dependencies {
    implementation("org.apache.kafka:kafka-streams:$kafkaVersion")
    implementation("org.slf4j:slf4j-api:1.7.36")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:$kafkaVersion")

    runtimeOnly("ch.qos.logback:logback-classic:1.2.11")

    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}