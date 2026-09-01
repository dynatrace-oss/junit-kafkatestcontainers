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
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

public class AnnotationPropertiesTest {

  @Nested
  @KafkaTestcontainers
  class DefaultPropertiesTest {

    @Test
    void noTopicsExistsByDefault(KafkaContainer kafkaContainer) throws Exception {
      try (AdminClient client = TestHelper.getAdminClient(kafkaContainer.getBootstrapServers())) {
        Set<String> topics = client.listTopics().names().get();
        assertThat(topics).isEmpty();
      }
    }

    @Test
    void shouldBeDefaultVersion(KafkaContainer kafkaContainer) {
      String currentClientVersion = AppInfoParser.getVersion();

      Optional<String> sut = TestHelper.findKafkaVersion(kafkaContainer.getContainerId());

      assertThat(sut)
        .isPresent()
        .contains(currentClientVersion);
    }

    @Test
    void onlyDefaultEnvVarSet(KafkaContainer kafkaContainer) {
      assertThat(kafkaContainer.getEnvMap())
        .containsEntry("KAFKA_NUM_PARTITIONS", "1")
        .doesNotContainKey(CustomProperties.CUSTOM_ENV_VAR_NAME);
    }

  }

  @Nested
  @KafkaTestcontainers(
    version = CustomProperties.CUSTOM_KAFKA_BROKER_VERSION,
    envVars = @KafkaTestcontainers.EnvVar(
      name = CustomProperties.CUSTOM_ENV_VAR_NAME,
      value = CustomProperties.CUSTOM_ENV_VAR_VALUE
    )
  )
  class CustomProperties {

    public static final String CUSTOM_KAFKA_BROKER_VERSION = "3.9.2";
    public static final String CUSTOM_ENV_VAR_NAME = "KAFKA_LOG_RETENTION_MS";
    public static final String CUSTOM_ENV_VAR_VALUE = "100";

    @Test
    void shouldBeCustomVersion(KafkaContainer kafkaContainer) {
      Optional<String> sut = TestHelper.findKafkaVersion(kafkaContainer.getContainerId());
      assertThat(sut)
        .isPresent()
        .contains(CUSTOM_KAFKA_BROKER_VERSION);
    }

    @Test
    void defaultAndCustomEnvVarsPresent(KafkaContainer kafkaContainer) {
      assertThat(kafkaContainer.getEnvMap())
        .containsEntry("KAFKA_NUM_PARTITIONS", "1")
        .containsEntry(CUSTOM_ENV_VAR_NAME, CUSTOM_ENV_VAR_VALUE);
    }

  }

  @Nested
  @KafkaTestcontainers(
    envVars = @KafkaTestcontainers.EnvVar(name = "KAFKA_NUM_PARTITIONS", value = "5")
  )
  class EnvVarOverridesDefaultPartitionsTest {

    @Test
    void envVarOverridesLibraryPartitions(KafkaContainer kafkaContainer) {
      assertThat(kafkaContainer.getEnvMap()).containsEntry("KAFKA_NUM_PARTITIONS", "5");
    }

  }

  @Nested
  @KafkaTestcontainers(
    partitions = 3,
    envVars = @KafkaTestcontainers.EnvVar(name = "KAFKA_NUM_PARTITIONS", value = "7")
  )
  class EnvVarOverridesExplicitPartitionsTest {

    @Test
    void envVarOverridesExplicitPartitions(KafkaContainer kafkaContainer) {
      assertThat(kafkaContainer.getEnvMap()).containsEntry("KAFKA_NUM_PARTITIONS", "7");
    }

  }

  @Nested
  @KafkaTestcontainers(
    topics = {
      @KafkaTestcontainers.Topic(
        name = TopicCreationTest.TOPIC_NAME,
        partitions = TopicCreationTest.TOPIC_NAME_PARTITION_COUNT
        ),
      @KafkaTestcontainers.Topic(name = TopicCreationTest.TOPIC_OTHER, partitions = 1)
    }
  )
  class TopicCreationTest {

    public static final String TOPIC_NAME = "someTopic";
    public static final int TOPIC_NAME_PARTITION_COUNT = 128;
    public static final String TOPIC_OTHER = "someOtherTopic";

    @Test
    void topicsCreated(KafkaContainer kafkaContainer) throws Exception {
      try (AdminClient client = TestHelper.getAdminClient(kafkaContainer.getBootstrapServers())) {
        Set<String> topics = client.listTopics().names().get();
        assertThat(topics)
          .hasSize(2)
          .contains(TOPIC_NAME, TOPIC_OTHER);

        var descriptions = client.describeTopics(topics).allTopicNames().get();
        assertThat(descriptions.get(TOPIC_NAME).partitions()).hasSize(TOPIC_NAME_PARTITION_COUNT);
        assertThat(descriptions.get(TOPIC_OTHER).partitions()).hasSize(1);
      }
    }

  }

}
