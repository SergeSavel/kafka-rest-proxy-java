plugins {
    id("application")
}

group = "pro.savel.kafka"
version = "2.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

application {
    mainClass = "pro.savel.kafka.Application"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.netty:netty-all:4.1.119.Final")
    implementation("org.apache.kafka:kafka-clients:3.9.0")
    implementation("org.apache.logging.log4j:log4j:2.24.3")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}