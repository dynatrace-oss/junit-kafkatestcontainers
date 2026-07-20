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

package com.dynatrace.junit.kafkatestcontainers.standalone;

import java.util.concurrent.TimeUnit;
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
 * Measures how long it takes to shut down a running Kafka broker, comparing the in-process Spring
 * {@code EmbeddedKafkaKraftBroker} ("embedded") against a Dockerized {@code KafkaContainer} via
 * Testcontainers ("testcontainers"). A fresh broker is started before every invocation so each shot
 * times a single {@code stop()} in isolation.
 */
@Fork(value = 2)
@Warmup(iterations = 5)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class StopPerformanceTest extends AbstractKafkaBenchmark {

  @Param({"embedded", "testcontainers"})
  private String brokerType;

  @Override
  protected BrokerLifecycle createBrokerLifecycle() {
    return brokerType.equals("embedded") ? new EmbeddedBrokerLifecycle() : new TestcontainersBrokerLifecycle();
  }

  @Setup(Level.Invocation)
  public void setUp() {
    startContainer();
  }

  @Benchmark
  public void measureStop() {
    stopContainer();
  }

}
