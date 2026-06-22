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

package com.dynatrace.oss.junit.kafkatestcontainers.spring;

import static com.dynatrace.oss.junit.kafkatestcontainers.TestHelper.buildConsumer;
import static com.dynatrace.oss.junit.kafkatestcontainers.TestHelper.buildProducer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.dynatrace.oss.junit.kafkatestcontainers.KafkaTestcontainers;
import com.dynatrace.oss.junit.kafkatestcontainers.TestData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@KafkaTestcontainers(
  topics = @KafkaTestcontainers.Topic(name = KafkaProducerConsumerTest.TOPIC_OLD)
)
@SpringJUnitConfig
class KafkaProducerConsumerTest {

  public static final String TOPIC_OLD = "old_topic";
  private static final String TOPIC_NEW = "new_topic";
  private static final String KEY = "custom-key";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapAddress;

  @Test
  void producedDataIsConsumedAfterProduced() throws Exception {
    // Given
    String name = "SomeData";
    int value = 10;
    String description = "Some description";
    TestData data = new TestData(name, value, description);

    // When
    try (KafkaProducer<String, String> producer = buildProducer(bootstrapAddress)) {
      producer.send(new ProducerRecord<>(TOPIC_OLD, KEY, data.toJson())).get();
    }

    // Then
    try (KafkaConsumer<String, String> consumer = buildConsumer(bootstrapAddress)) {
      consumer.subscribe(List.of(TOPIC_OLD));
      await().atMost(3, TimeUnit.SECONDS).until(() -> {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        return assertRecords(records, data);
      });
    }
  }

  private boolean assertRecords(ConsumerRecords<String, String> records, TestData data)
    throws JsonProcessingException {
    if (records.isEmpty()) {
      return false;
    }

    assertThat(records.count()).isEqualTo(1);
    String consumedValue = records.iterator().next().value();
    TestData consumed = OBJECT_MAPPER.readValue(consumedValue, TestData.class);

    assertThat(consumed.name()).isEqualTo(data.name());
    assertThat(consumed.value()).isEqualTo(data.value());
    assertThat(consumed.description()).isEqualTo(data.description());

    return true;
  }

  @Test
  void producedDataExistsInTopicVerifiedByAdmin() throws Exception {
    // Given
    String name = "SomeNewData";
    int value = 15;
    String description = "Some new description";
    TestData data = new TestData(name, value, description);

    // When
    try (KafkaProducer<String, String> producer = buildProducer(bootstrapAddress)) {
      producer.send(new ProducerRecord<>(TOPIC_NEW, KEY, data.toJson())).get();
    }

    // Then
    Map<String, Object> adminProps = Map.of(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress
    );

    try (AdminClient adminClient = AdminClient.create(adminProps)) {

      Map<TopicPartition, OffsetSpec> request = adminClient
        .describeTopics(List.of(TOPIC_NEW))
        .topicNameValues()
        .get(TOPIC_NEW)
        .get()
        .partitions()
        .stream()
        .collect(Collectors.toMap(
          p -> new TopicPartition(TOPIC_NEW, p.partition()),
          p -> OffsetSpec.latest()
        ));

      long totalMessages = adminClient.listOffsets(request)
                                      .all()
                                      .get()
                                      .values()
                                      .stream()
                                      .mapToLong(ListOffsetsResult.ListOffsetsResultInfo::offset)
                                      .sum();

      assertThat(totalMessages)
        .isEqualTo(1L);
    }
  }

}
