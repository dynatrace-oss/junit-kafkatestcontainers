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

package com.dynatrace.junit.kafkatestcontainers.spring;

import static org.assertj.core.api.Assertions.assertThat;

import com.dynatrace.junit.kafkatestcontainers.KafkaTestcontainers;
import com.dynatrace.junit.kafkatestcontainers.TestHelper;
import java.util.List;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig
@KafkaTestcontainers(
  partitions = DefaultPartitionCountTest.DEFAULT_PARTITION_COUNT,
  topics = {
    @KafkaTestcontainers.Topic(name = DefaultPartitionCountTest.TOPIC_DEFAULT),
    @KafkaTestcontainers.Topic(
      name = DefaultPartitionCountTest.TOPIC_OVERRIDDEN,
      partitions = DefaultPartitionCountTest.OVERRIDDEN_PARTITION_COUNT
      )
  }
)
public class DefaultPartitionCountTest {

  public static final int DEFAULT_PARTITION_COUNT = 64;
  public static final int OVERRIDDEN_PARTITION_COUNT = 8;
  public static final String TOPIC_DEFAULT = "topic-with-default-partitions";
  public static final String TOPIC_OVERRIDDEN = "topic-with-overridden-partitions";
  public static final String AUTO_CREATED_TOPIC = "auto-created-topic";

  @Value("${spring.kafka.bootstrap-servers}")
  private String brokerAddress;

  @Test
  void topicWithoutExplicitCountUsesDefault() throws Exception {
    try (AdminClient client = TestHelper.getAdminClient(brokerAddress)) {
      var descriptions = client.describeTopics(List.of(TOPIC_DEFAULT)).allTopicNames().get();
      assertThat(descriptions.get(TOPIC_DEFAULT).partitions()).hasSize(DEFAULT_PARTITION_COUNT);
    }
  }

  @Test
  void topicWithExplicitCountOverridesDefault() throws Exception {
    try (AdminClient client = TestHelper.getAdminClient(brokerAddress)) {
      var descriptions = client.describeTopics(List.of(TOPIC_OVERRIDDEN)).allTopicNames().get();
      assertThat(descriptions.get(TOPIC_OVERRIDDEN).partitions()).hasSize(OVERRIDDEN_PARTITION_COUNT);
    }
  }

  @Test
  void autoCreatedTopicUsesDefaultPartitionCount() throws Exception {
    try (KafkaProducer<String, String> producer = TestHelper.buildProducer(brokerAddress)) {
      producer.send(new ProducerRecord<>(AUTO_CREATED_TOPIC, "key", "value")).get();
    }

    try (AdminClient client = TestHelper.getAdminClient(brokerAddress)) {
      var descriptions = client.describeTopics(List.of(AUTO_CREATED_TOPIC)).allTopicNames().get();
      assertThat(descriptions.get(AUTO_CREATED_TOPIC).partitions()).hasSize(DEFAULT_PARTITION_COUNT);
    }
  }

}
