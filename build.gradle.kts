plugins {
    id("application")
}

group = "pro.savel.krp"
version = "2.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

application {
    mainClass = "pro.savel.krp.Application"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework:spring-context:6.2.4")
    implementation("io.netty:netty-all:4.1.119.Final")
    implementation("org.apache.kafka:kafka-clients:3.9.0")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}