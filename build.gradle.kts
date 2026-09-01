import de.thetaphi.forbiddenapis.gradle.CheckForbiddenApis
import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone
import org.sonarqube.gradle.SonarTask

plugins {
  java
  `java-test-fixtures`
  `jvm-test-suite`
  application
  checkstyle
  jacoco
  `maven-publish`
  signing
  alias(libs.plugins.build.health)
  alias(libs.plugins.errorprone)
  alias(libs.plugins.forbiddenapis)
  alias(libs.plugins.jmh)
  alias(libs.plugins.sonarqube)
}

repositories {
  mavenCentral()
  gradlePluginPortal()
}

configurations.all {
  resolutionStrategy.eachDependency {
    if (requested.group == "com.fasterxml.jackson.core" && requested.name == "jackson-core") {
      useVersion(libs.versions.jackson.get())
    }
    if (requested.group == "com.fasterxml.jackson.core" && requested.name == "jackson-databind") {
      useVersion(libs.versions.jackson.get())
    }
    if (requested.group == "org.apache.commons" && requested.name == "commons-lang3") {
      useVersion(libs.versions.commons.lang3.get())
    }
    if (requested.group == "org.codehaus.plexus" && requested.name == "plexus-utils") {
      useVersion(libs.versions.plexus.utils.get())
    }
    if (requested.group == "at.yawk.lz4" && requested.name == "lz4-java") {
      useVersion(libs.versions.lz4.get())
    }
  }
}

dependencies {
  errorprone(libs.errorProne)
  errorprone(libs.nullaway)

  jmh(platform(libs.spring.boot.bom))
  jmh(libs.spring.boot.starter.kafka.test)

  jmhImplementation(libs.spring.kafka.test)
  jmhImplementation(platform(libs.junit.bom))
  jmhImplementation(libs.junit.platform.engine)
  jmhImplementation(libs.junit.platform.launcher)
  jmhRuntimeOnly(libs.junit.jupiter.engine)

  testFixturesApi(libs.spring.test)
  testFixturesApi(libs.testcontainers.kafka)

  testFixturesCompileOnly(libs.jspecify)

  testFixturesImplementation(platform(libs.junit.bom))
  testFixturesImplementation(libs.junit.jupiter.api)
  testFixturesImplementation(libs.kafka.client)
  testFixturesImplementation(platform(libs.spring.boot.bom))
  testFixturesImplementation(libs.spring.beans)
  testFixturesImplementation(libs.spring.context)
  testFixturesImplementation(libs.spring.core)
  testFixturesImplementation(libs.testcontainers)

  testFixturesRuntimeOnly(libs.spring.boot.test)

  testImplementation(libs.assertj.core)
  testImplementation(libs.awaitility)
  testImplementation(libs.docker.java.api)
  testImplementation(libs.jackson)
  testImplementation(libs.jackson.databind)
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.kafka.client)
  testImplementation(platform(libs.spring.boot.bom))
  testImplementation(libs.spring.beans)
  testImplementation(libs.spring.boot.test)
  testImplementation(libs.spring.context)
  testImplementation(libs.spring.core)

  testRuntimeOnly(libs.junit.jupiter.engine)
  testRuntimeOnly(libs.junit.platform.launcher)
  testRuntimeOnly(libs.snakeyaml)
}

java {
  toolchain {
    languageVersion = JavaLanguageVersion.of(17)
  }
  withSourcesJar()
  withJavadocJar()
}

tasks.jar {
  metaInf {
    from("LICENSE")
    from("NOTICE")
  }
}

group = "com.dynatrace.junit.kafkatestcontainers"
version = "0.2.0"

checkstyle {
  toolVersion = libs.versions.checkstyle.get()
}

jmh {
  resultsFile = project.file("benchmark_result/result.json")
  resultFormat = "JSON"
  timeUnit = "ms"
}

jacoco {
  toolVersion = libs.versions.jacoco.get()
}

sonarqube {
  properties {
    property("sonar.projectKey", "dynatrace-oss_junit-kafkatestcontainers")
    property("sonar.organization", "dynatrace-oss")
    property("sonar.host.url", "https://sonarcloud.io")
    property("sonar.sources", "src/testFixtures/java")
    property(
      "sonar.java.binaries",
      "${layout.buildDirectory.get()}/classes/java/testFixtures",
    )
    property(
      "sonar.java.libraries",
      configurations["testFixturesCompileClasspath"].resolve().joinToString(",") { it.absolutePath },
    )
    property(
      "sonar.coverage.jacoco.xmlReportPaths",
      "${layout.buildDirectory.get()}/reports/jacoco/test/jacocoTestReport.xml",
    )
  }
}

dependencyAnalysis {
  issues {
    all {
      onIncorrectConfiguration {
        exclude("org.springframework:spring-test", "org.testcontainers:testcontainers-kafka")
      }
      sourceSet("testJava21") {
        onUnusedDependencies {
          exclude("org.junit.jupiter:junit-jupiter")
        }
      }
      sourceSet("testJava25") {
        onUnusedDependencies {
          exclude("org.junit.jupiter:junit-jupiter")
        }
      }
    }
  }
}

tasks.register("checkstyleAll") {
  dependsOn("checkstyleMain")
  dependsOn("checkstyleTest")
  dependsOn("checkstyleTestFixtures")
}

tasks.named<Test>("test") {
  useJUnitPlatform()
}

testing {
  suites {
    register<JvmTestSuite>("testJava21") {
      sources.java.setSrcDirs(listOf("src/test/java"))
      sources.resources.setSrcDirs(listOf("src/test/resources"))
      dependencies {
        implementation(testFixtures(project()))
      }
    }
    register<JvmTestSuite>("testJava25") {
      sources.java.setSrcDirs(listOf("src/test/java"))
      sources.resources.setSrcDirs(listOf("src/test/resources"))
      dependencies {
        implementation(testFixtures(project()))
      }
    }
  }
}

configurations["testJava21Implementation"].extendsFrom(configurations["testImplementation"])
configurations["testJava21RuntimeOnly"].extendsFrom(configurations["testRuntimeOnly"])
configurations["testJava25Implementation"].extendsFrom(configurations["testImplementation"])
configurations["testJava25RuntimeOnly"].extendsFrom(configurations["testRuntimeOnly"])

tasks.named<Test>("testJava21") {
  useJUnitPlatform()
  javaLauncher.set(javaToolchains.launcherFor {
    languageVersion.set(JavaLanguageVersion.of(21))
  })
}

tasks.named<Test>("testJava25") {
  useJUnitPlatform()
  javaLauncher.set(javaToolchains.launcherFor {
    languageVersion.set(JavaLanguageVersion.of(25))
  })
}

tasks.check {
  dependsOn(testing.suites.named("testJava21"))
  dependsOn(testing.suites.named("testJava25"))
}

tasks.withType<Checkstyle>().configureEach {
  logging.captureStandardOutput(LogLevel.LIFECYCLE)
  logging.captureStandardError(LogLevel.LIFECYCLE)
  javaLauncher = javaToolchains.launcherFor {
    languageVersion.set(JavaLanguageVersion.of(21))
  }
}

tasks.withType<JavaCompile>().configureEach {
  options.compilerArgs.add("-XDaddTypeAnnotationsToSymbol=true")
  options.errorprone {
    check("NullAway", CheckSeverity.ERROR)
    option("NullAway:AnnotatedPackages", "com.dynatrace")
    option("NullAway:JSpecifyMode", "true")
    if ((name.contains("test", ignoreCase = true) || name.contains("jmh", ignoreCase = true))
        && !name.contains("fixtures", ignoreCase = true)) {
      disable("NullAway")
    }
  }
}

tasks.withType(CheckForbiddenApis::class).configureEach {
  bundledSignatures = setOf("jdk-unsafe", "jdk-system-out")
  signaturesFiles += rootProject.files("config/forbiddenapis/forbidden.signatures.txt")
  signaturesFiles += rootProject.files("config/forbiddenapis/forbidden-logging.signatures.txt")
  ignoreSignaturesOfMissingClasses = true
}

tasks.named<CheckForbiddenApis>("forbiddenApisTest") {
  signaturesFiles += rootProject.files("config/forbiddenapis/forbidden.signatures.TEST.txt")
}

tasks.named<JacocoReport>("jacocoTestReport") {
  dependsOn(tasks.named("test"))
  classDirectories.setFrom(files(sourceSets["testFixtures"].output.classesDirs))
  sourceDirectories.setFrom(files(sourceSets["testFixtures"].java.srcDirs))
  executionData.setFrom(fileTree(layout.buildDirectory) { include("jacoco/test.exec") })
  reports {
    xml.required.set(true)
    csv.required.set(true)
  }
}

tasks.withType<SonarTask> {
  dependsOn(tasks.named("jacocoTestReport"))
}

publishing {
  publications {
    create<MavenPublication>("mavenJava") {
      from(components["java"])
      pom {
        name = "com.dynatrace.junit.kafkatestcontainers:junit-kafkatestcontainers"
        description = "A JUnit extension for Kafka integration tests"
        url = "https://github.com/dynatrace-oss/junit-kafkatestcontainers"
        licenses {
          license {
            name = "The Apache License, Version 2.0"
            url = "https://www.apache.org/licenses/LICENSE-2.0.html"
          }
        }
        developers {
          developer {
            id = "Dynatrace"
            name = "Dynatrace LLC"
            email = "opensource@dynatrace.com"
          }
        }
        scm {
          connection = "scm:git:git://github.com/dynatrace-oss/junit-kafkatestcontainers.git"
          developerConnection = "scm:git:ssh://github.com/dynatrace-oss/junit-kafkatestcontainers.git"
          url = "https://github.com/dynatrace-oss/junit-kafkatestcontainers"
        }
      }
    }

    repositories {
      maven {
        url = uri("$projectDir/build/mavencentral/repo")
      }
    }
  }
}


signing {
  useInMemoryPgpKeys(System.getenv("GPG_PRIVATE_KEY"), System.getenv("GPG_PASSPHRASE"))
  sign(publishing.publications["mavenJava"])
}

gradle.taskGraph.whenReady {
  tasks.withType<Sign>().configureEach {
    isRequired = hasTask(":createRepoForUploadToMavenCentral")
  }
}

tasks.register("createRepoForUploadToMavenCentral", Zip::class) {
  dependsOn(tasks.named("build"), tasks.named("publish"))
  from("$projectDir/build/mavencentral/repo")
  destinationDirectory.set(layout.buildDirectory.dir("mavencentral"))
  archiveFileName.set("junit-kafkatestcontainers-$version.zip")
}
