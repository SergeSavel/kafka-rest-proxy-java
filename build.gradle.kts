plugins {
    id("application")
    java
}

group = "pro.savel.kafka"
version = "5.0.0-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

application {
    mainClass = "pro.savel.kafka.Application"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.netty:netty-all:4.2.6.Final")
    implementation("org.apache.kafka:kafka-clients:4.1.0")
    //implementation("org.apache.logging.log4j:log4j:2.24.3")
    implementation("org.slf4j:slf4j-api:2.0.17")
    implementation(platform("org.apache.logging.log4j:log4j-bom:2.25.2"))
    //implementation("org.apache.logging.log4j:log4j-api")
    runtimeOnly("org.apache.logging.log4j:log4j-core")
    runtimeOnly("org.apache.logging.log4j:log4j-layout-template-json")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl")
    compileOnly("org.projectlombok:lombok:1.18.42")
    annotationProcessor("org.projectlombok:lombok:1.18.42")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.19.2")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testCompileOnly("org.projectlombok:lombok:1.18.42")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.42")
}

distributions {
    main {
        contents {
            from("LICENSE")
        }
    }
}

tasks.jar {
    manifest {
        attributes(mapOf("Implementation-Version" to version))
    }
}

tasks.test {
    useJUnitPlatform()
}