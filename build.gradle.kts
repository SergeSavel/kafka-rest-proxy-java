plugins {
    id("application")
}

group = "pro.savel.kafka"
version = "5.0-SNAPSHOT"

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
    implementation("org.apache.kafka:kafka-clients:4.0.0")
    //implementation("org.apache.logging.log4j:log4j:2.24.3")
    implementation("org.slf4j:slf4j-api:2.0.17")
    implementation(platform("org.apache.logging.log4j:log4j-bom:2.24.3"))
    //implementation("org.apache.logging.log4j:log4j-api")
    runtimeOnly("org.apache.logging.log4j:log4j-core")
    runtimeOnly("org.apache.logging.log4j:log4j-layout-template-json")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.1")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}