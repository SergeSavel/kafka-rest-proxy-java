import org.gradle.api.file.RelativePath

plugins {
    id("application")
    java
}

val nettyVersion = "4.2.17.Final"

group = "pro.savel.kafka"
version = "5.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

application {
    mainClass = "pro.savel.kafka.Application"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(platform("io.netty:netty-bom:$nettyVersion"))
    implementation("io.netty:netty-common")
    implementation("io.netty:netty-buffer")
    implementation("io.netty:netty-transport")
    implementation("io.netty:netty-transport-classes-epoll")
    implementation("io.netty:netty-codec-http")
    implementation("io.netty:netty-handler")
    runtimeOnly("io.netty:netty-transport-native-epoll:$nettyVersion:linux-x86_64")
    runtimeOnly("io.netty:netty-transport-native-epoll:$nettyVersion:linux-aarch_64")
    implementation("org.apache.kafka:kafka-clients:4.1.2")
    implementation("jakarta.validation:jakarta.validation-api:3.1.1")
    implementation("org.hibernate.validator:hibernate-validator:9.1.3.Final")
    implementation("org.slf4j:slf4j-api:2.0.18")
    implementation(platform("org.apache.logging.log4j:log4j-bom:2.26.1"))
    runtimeOnly("org.apache.logging.log4j:log4j-core")
    runtimeOnly("org.apache.logging.log4j:log4j-layout-template-json")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl")
    compileOnly("org.projectlombok:lombok:1.18.46")
    annotationProcessor("org.projectlombok:lombok:1.18.46")
    testCompileOnly("org.projectlombok:lombok:1.18.46")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.46")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.22.1")
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.mockito:mockito-core:5.23.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.23.0")
}

distributions {
    main {
        contents {
            from("LICENSE")
            from("NOTICE")
            from("README.md")
            from("kafka-gateway.service")
            from("install-windows-service.ps1")
        }
    }
}

tasks.jar {
    manifest {
        attributes(mapOf("Implementation-Version" to version))
    }
}

// Put distribution files at the archive root instead of a kafka-gateway-<version>/ folder.
// docs are added at the task level (outside the wrapped distribution spec) to avoid
// stray directory entries of the wrapper folder in the archives.
listOf(tasks.distZip, tasks.distTar).forEach { distTask ->
    distTask.configure {
        val distributionDir = "${project.name}-${project.version}"
        from(fileTree("docs")) {
            into("docs")
        }
        eachFile {
            if (relativePath.segments.first() == distributionDir)
                relativePath = RelativePath(true, *relativePath.segments.drop(1).toTypedArray())
        }
    }
}

tasks.installDist {
    from(fileTree("docs")) {
        into("docs")
    }
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("-Xshare:off")
}
