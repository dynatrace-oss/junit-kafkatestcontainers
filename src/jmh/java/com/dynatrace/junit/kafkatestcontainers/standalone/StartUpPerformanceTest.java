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
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures how long it takes to bring a Kafka broker up to the point where it hands back a usable
 * bootstrap address, comparing the in-process Spring {@code EmbeddedKafkaKraftBroker} ("embedded")
 * against a Dockerized {@code KafkaContainer} via Testcontainers ("testcontainers"). Each shot
 * times a single cold {@code start()}, and the broker is torn down after every invocation so no
 * warm state carries over between measurements.
 */
@Fork(value = 2)
@Warmup(iterations = 5)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class StartUpPerformanceTest extends AbstractKafkaBenchmark {

  @Param({"embedded", "testcontainers"})
  private String brokerType;

  @Override
  protected BrokerLifecycle createBrokerLifecycle() {
    return brokerType.equals("embedded") ? new EmbeddedBrokerLifecycle() : new TestcontainersBrokerLifecycle();
  }

  @Benchmark
  public String measureStartUp() {
    return startContainer();
  }

  @TearDown(Level.Invocation)
  public void tearDown() {
    stopContainer();
  }

}
