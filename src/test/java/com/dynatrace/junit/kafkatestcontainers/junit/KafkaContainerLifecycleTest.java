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

import static org.assertj.core.api.Assertions.assertThat;

import com.dynatrace.junit.kafkatestcontainers.KafkaTestcontainers;
import com.dynatrace.junit.kafkatestcontainers.TestHelper;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.kafka.KafkaContainer;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@KafkaTestcontainers(
  topics = {
    @KafkaTestcontainers.Topic(
      name = KafkaContainerLifecycleTest.TOPIC_1_NAME,
      partitions = KafkaContainerLifecycleTest.TOPIC_1_PARTITION_COUNT
      ),
    @KafkaTestcontainers.Topic(
      name = KafkaContainerLifecycleTest.TOPIC_2_NAME,
      partitions = KafkaContainerLifecycleTest.TOPIC_2_PARTITION_COUNT
      )
  }
)
class KafkaContainerLifecycleTest {

  static final String TOPIC_1_NAME = "topic_1";
  static final short TOPIC_1_PARTITION_COUNT = 100;
  static final String TOPIC_2_NAME = "topic_2";
  static final short TOPIC_2_PARTITION_COUNT = 47;

  private static String capturedBootstrapServers;

  @Test
  @Order(1)
  void containerStartedAndTopicsExist(KafkaContainer kafkaContainer) throws ExecutionException, InterruptedException {
    capturedBootstrapServers = kafkaContainer.getBootstrapServers();

    try (AdminClient client = TestHelper.getAdminClient(capturedBootstrapServers)) {
      assertThat(client.listTopics().names().get())
        .containsExactlyInAnyOrder(TOPIC_1_NAME, TOPIC_2_NAME);
    }
  }

  @Test
  @Order(2)
  void containerAndTopicsPersistAcrossTestMethods(KafkaContainer kafkaContainer)
    throws ExecutionException, InterruptedException {
    assertThat(kafkaContainer.getBootstrapServers()).isEqualTo(capturedBootstrapServers);

    try (AdminClient client = TestHelper.getAdminClient(kafkaContainer.getBootstrapServers())) {
      assertThat(client.listTopics().names().get())
        .containsExactlyInAnyOrder(TOPIC_1_NAME, TOPIC_2_NAME);
    }
  }

  @Test
  void topicsHaveCorrectPartitionConfig(KafkaContainer kafkaContainer)
    throws ExecutionException, InterruptedException {
    try (AdminClient client = TestHelper.getAdminClient(kafkaContainer.getBootstrapServers())) {
      var descriptions = client.describeTopics(List.of(TOPIC_1_NAME, TOPIC_2_NAME))
                               .allTopicNames().get();

      assertThat(descriptions.get(TOPIC_1_NAME).partitions()).hasSize(TOPIC_1_PARTITION_COUNT);
      assertThat(descriptions.get(TOPIC_2_NAME).partitions()).hasSize(TOPIC_2_PARTITION_COUNT);
    }
  }
}
