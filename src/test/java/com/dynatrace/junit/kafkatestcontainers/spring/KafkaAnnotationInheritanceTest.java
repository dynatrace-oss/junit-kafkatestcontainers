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
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;

public class KafkaAnnotationInheritanceTest {

  @Nested
  @DirtiesContext
  class SimpleInheritanceTest extends KafkaTestcontainersOnAbstractTest {

    @Value("${spring.abstract.test}")
    private String bootstrapAddress;

    @Test
    void brokerReachableAndTopicCreated() throws Exception {
      try (AdminClient adminClient = TestHelper.getAdminClient(bootstrapAddress)) {
        Set<String> topics = adminClient.listTopics().names().get();
        assertThat(topics)
          .hasSize(1)
          .contains(ABSTRACT_TOPIC);
      }
    }

  }

  @Nested
  @KafkaTestcontainers(topics = @KafkaTestcontainers.Topic(name = OverrideInheritanceTest.CONCRETE_TOPIC))
  @DirtiesContext
  class OverrideInheritanceTest extends KafkaTestcontainersOnAbstractTest {

    public static final String CONCRETE_TOPIC = "concrete_topic";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Test
    void brokerReachableAndTopicCreated() throws Exception {
      try (AdminClient adminClient = TestHelper.getAdminClient(bootstrapAddress)) {
        Set<String> topics = adminClient.listTopics().names().get();
        assertThat(topics)
          .hasSize(1)
          .contains(CONCRETE_TOPIC);
      }
    }

  }

}
