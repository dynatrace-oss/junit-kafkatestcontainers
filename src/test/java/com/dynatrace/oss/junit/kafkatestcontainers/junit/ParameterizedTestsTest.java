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

package com.dynatrace.oss.junit.kafkatestcontainers.junit;

import static com.dynatrace.oss.junit.kafkatestcontainers.TestHelper.buildConsumer;
import static com.dynatrace.oss.junit.kafkatestcontainers.TestHelper.buildProducer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.dynatrace.oss.junit.kafkatestcontainers.KafkaTestcontainers;
import com.dynatrace.oss.junit.kafkatestcontainers.TestData;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

@KafkaTestcontainers
class ParameterizedTestsTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static Stream<Arguments> produceAndConsumeScenarios() {
    return Stream.of(
      Arguments.of("topic_1", new TestData("Topic1Data1", 1, "Topic 1 Data 1 description")),
      Arguments.of("topic_2", new TestData("Topic2Data1", 100, "Topic 2 Data 1 description")),
      Arguments.of("topic_1", new TestData("Topic1Data2", 0, "Topic 1 Data 2 description"))
    );
  }

  @ParameterizedTest(name = "topic [{0}] with payload {1}")
  @MethodSource("com.dynatrace.oss.junit.kafkatestcontainers.junit.ParameterizedTestsTest#produceAndConsumeScenarios")
  void shouldProduceAndConsumeFromTopic(String topic, TestData data, KafkaContainer kafka) throws Exception {
    String bootstrapAddress = kafka.getBootstrapServers();

    try (KafkaProducer<String, String> producer = buildProducer(bootstrapAddress)) {
      producer.send(new ProducerRecord<>(topic, data.name(), data.toJson())).get();
    }

    try (KafkaConsumer<String, String> consumer = buildConsumer(bootstrapAddress)) {
      consumer.subscribe(List.of(topic));
      await().atMost(3, TimeUnit.SECONDS).until(() -> {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        if (records.isEmpty()) {
          return false;
        }
        String consumed = records.iterator().next().value();
        TestData consumedData = OBJECT_MAPPER.readValue(consumed, TestData.class);
        assertThat(consumedData.name()).isEqualTo(data.name());
        assertThat(consumedData.value()).isEqualTo(data.value());
        assertThat(consumedData.description()).isEqualTo(data.description());
        return true;
      });
    }
  }

}
