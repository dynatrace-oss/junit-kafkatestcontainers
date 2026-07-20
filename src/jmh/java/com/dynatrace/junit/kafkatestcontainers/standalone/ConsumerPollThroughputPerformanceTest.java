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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Throughput variant: produce a full batch of records to an already-running broker (excluded from
 * timing) and measure how long the consumer takes to drain the whole batch. Contrast with the
 * latency variant in {@link ConsumerPollLatencyPerformanceTest}, which measures a single fetch.
 * {@link OperationsPerInvocation} normalizes the result to per-record time.
 */
@Fork(value = 2)
@Warmup(iterations = 5)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class ConsumerPollThroughputPerformanceTest extends AbstractKafkaBenchmark {

  @Param({"embedded", "testcontainers"})
  private String brokerType;

  @Override
  protected BrokerLifecycle createBrokerLifecycle() {
    return brokerType.equals("embedded") ? new EmbeddedBrokerLifecycle() : new TestcontainersBrokerLifecycle();
  }

  private static final String TOPIC = "benchmark-consumer-topic";
  private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
  private static final int RECORD_COUNT = 1_000;

  private KafkaProducer<String, String> producer;
  private KafkaConsumer<String, String> consumer;

  @Setup(Level.Trial)
  public void setUpBroker() throws Exception {
    String bootstrapServers = startContainer();
    try (AdminClient admin = AdminClient.create(Map.of(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
    ))) {
      admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
    }
    producer = new KafkaProducer<>(Map.of(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
    ));
    consumer = new KafkaConsumer<>(Map.of(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG, "benchmark-consumer-group",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
    ));
    consumer.subscribe(List.of(TOPIC));
  }

  @Setup(Level.Invocation)
  @SuppressWarnings("FutureReturnValueIgnored")
  public void produceBatch() {
    for (int i = 0; i < RECORD_COUNT; i++) {
      producer.send(new ProducerRecord<>(TOPIC, "key", "benchmark-value-" + i));
    }
    // Block until the whole batch is durably written, so the benchmark measures only the drain.
    producer.flush();
  }

  @Benchmark
  @OperationsPerInvocation(RECORD_COUNT)
  public int measureConsumerPoll() {
    int consumed = 0;
    while (consumed < RECORD_COUNT) {
      consumed += consumer.poll(POLL_TIMEOUT).count();
    }
    return consumed;
  }

  @TearDown(Level.Trial)
  public void tearDownBroker() {
    producer.close();
    consumer.close();
    stopContainer();
  }

}
