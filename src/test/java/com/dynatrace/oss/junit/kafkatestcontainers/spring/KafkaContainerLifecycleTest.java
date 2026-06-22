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

import static org.assertj.core.api.Assertions.assertThat;

import com.dynatrace.oss.junit.kafkatestcontainers.KafkaTestcontainers;
import com.dynatrace.oss.junit.kafkatestcontainers.TestHelper;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.MethodMode;
import org.springframework.test.context.ActiveProfiles;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ActiveProfiles("complex-topic-creation")
@KafkaTestcontainers(
  topics = {
    @KafkaTestcontainers.Topic(
      name = KafkaContainerLifecycleTest.TOPIC_1_NAME,
      partitions = KafkaContainerLifecycleTest.TOPIC_1_PARTITION_COUNT
      ),
    @KafkaTestcontainers.Topic(name = "${testcontainers.kafka.tables.broker.topic}"),
    @KafkaTestcontainers.Topic(
      name = KafkaContainerLifecycleTest.TOPIC_2_NAME,
      partitions = KafkaContainerLifecycleTest.TOPIC_2_PARTITION_COUNT
      )
  }
)
@SpringBootTest(classes = {KafkaContainerLifecycleTest.BeanConfig.class})
public class KafkaContainerLifecycleTest {

  public static final String TOPIC_1_NAME = "topic_1";
  public static final short TOPIC_1_PARTITION_COUNT = 100;
  public static final String TOPIC_2_NAME = "topic_2";
  public static final short TOPIC_2_PARTITION_COUNT = 47;

  @Value("${testcontainers.kafka.tables.broker.topic}")
  private String topic3Name;

  private static String capturedBootstrapAddress;

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapAddress;


  @Test
  @Order(1)
  void beansCreatedAndTopicsExist() throws ExecutionException, InterruptedException {
    capturedBootstrapAddress = bootstrapAddress;

    try (AdminClient client = TestHelper.getAdminClient(bootstrapAddress)) {
      assertThat(client.listTopics().names().get())
        .containsExactlyInAnyOrder(TOPIC_1_NAME, topic3Name, TOPIC_2_NAME);
    }
  }

  @Test
  @Order(2)
  void beansAndTopicsPersistAcrossTestMethods() throws ExecutionException, InterruptedException {
    assertThat(bootstrapAddress).isEqualTo(capturedBootstrapAddress);

    try (AdminClient client = TestHelper.getAdminClient(bootstrapAddress)) {
      assertThat(client.listTopics().names().get())
        .containsExactlyInAnyOrder(TOPIC_1_NAME, topic3Name, TOPIC_2_NAME);
    }
  }


  @Test
  void topicsHaveCorrectPartitionAndReplicationConfig() throws ExecutionException, InterruptedException {
    try (AdminClient client = TestHelper.getAdminClient(bootstrapAddress)) {
      Set<String> topics = client.listTopics().names().get();
      var descriptions = client.describeTopics(topics)
                               .allTopicNames().get();

      assertThat(descriptions.get(TOPIC_1_NAME).partitions()).hasSize(TOPIC_1_PARTITION_COUNT);

      assertThat(descriptions.get(topic3Name).partitions()).hasSize(1);

      assertThat(descriptions.get(TOPIC_2_NAME).partitions()).hasSize(TOPIC_2_PARTITION_COUNT);
    }
  }

  @Test
  @Order(3)
  @DirtiesContext(methodMode = MethodMode.BEFORE_METHOD)
  void containerBeanIsReplacedOnContextRefresh() throws ExecutionException, InterruptedException {
    assertThat(bootstrapAddress).isNotEqualTo(capturedBootstrapAddress);

    try (AdminClient client = TestHelper.getAdminClient(bootstrapAddress)) {
      assertThat(client.listTopics().names().get())
        .containsExactlyInAnyOrder(TOPIC_1_NAME, topic3Name, TOPIC_2_NAME);
    }
  }

  @Test
  void isPropertyResolved(@Value("${testcontainers.kafka.tables.broker.bootstrap-server}") String sut) {
    assertThat(sut)
      .isNotEmpty()
      .isNotEqualTo("${testcontainers.kafka.tables.broker.bootstrap-server}")
      .isEqualTo(bootstrapAddress);
  }

  @Configuration
  public static class BeanConfig {

  }

}
