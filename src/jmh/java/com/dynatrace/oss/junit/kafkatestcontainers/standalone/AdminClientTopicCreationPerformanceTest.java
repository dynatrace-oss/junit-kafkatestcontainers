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

package com.dynatrace.oss.junit.kafkatestcontainers.standalone;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures topic creation against an already-running broker. The {@link AdminClient} is created
 * once per trial in setup so the benchmark reflects the cost of {@code createTopics} itself, not
 * the client's bootstrap/metadata round-trip.
 */
@Fork(value = 2)
@Warmup(iterations = 5)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class AdminClientTopicCreationPerformanceTest extends AbstractKafkaBenchmark {

  @Param({"embedded", "testcontainers"})
  private String brokerType;

  @Override
  protected BrokerLifecycle createBrokerLifecycle() {
    return brokerType.equals("embedded") ? new EmbeddedBrokerLifecycle() : new TestcontainersBrokerLifecycle();
  }

  private AdminClient admin;

  @Setup(Level.Trial)
  public void setUpBroker() {
    String bootstrapServers = startContainer();
    admin = AdminClient.create(Map.of(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
    ));
  }

  @Benchmark
  public String measureTopicCreation() throws Exception {
    String topicName = "benchmark-" + UUID.randomUUID();
    admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get();
    return topicName;
  }

  @TearDown(Level.Trial)
  public void tearDownBroker() {
    admin.close();
    stopContainer();
  }

}
