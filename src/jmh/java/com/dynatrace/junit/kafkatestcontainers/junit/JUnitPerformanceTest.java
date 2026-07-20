/*
 * Copyright 2026 Dynatrace LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dynatrace.junit.kafkatestcontainers.junit;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * End-to-end cost of executing a full JUnit 5 test class through the platform launcher, comparing
 * the two ways of provisioning Kafka for a plain (non-Spring) test: Spring's {@code @EmbeddedKafka}
 * in-process broker ("embedded") versus this library's Docker-backed {@code @KafkaTestcontainers}
 * annotation ("testcontainers"). Because it runs the whole test class, the measurement captures the
 * real per-test-class overhead a developer pays — broker lifecycle plus the produce/consume test
 * body — rather than broker startup in isolation.
 */
@Fork(2)
@Warmup(iterations = 3, batchSize = 1)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, batchSize = 1)
@State(Scope.Benchmark)
public class JUnitPerformanceTest {

  @Param({"embedded", "testcontainers"})
  private String brokerType;

  private LauncherDiscoveryRequest request;
  private Launcher launcher;

  @Setup(Level.Trial)
  public void setUpLauncher() {
    Class<?> testClass = brokerType.equals("embedded") ? EmbeddedKafkaTest.class : KafkaTestcontainersTest.class;
    request = LauncherDiscoveryRequestBuilder.request().selectors(selectClass(testClass)).build();
    // Create the launcher once and reuse it, mirroring how Gradle/Surefire run all
    // test classes through a single launcher instead of opening a session per class.
    launcher = LauncherFactory.create();
  }

  @Benchmark
  public void runTestClass() {
    SummaryGeneratingListener listener = new SummaryGeneratingListener();
    launcher.execute(request, listener);
    var summary = listener.getSummary();
    List<TestExecutionSummary.Failure> failures = summary.getFailures();
    if (!failures.isEmpty()) {
      throw new RuntimeException("Test failures in " + brokerType + ": " + failures);
    }
  }

}
