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

import com.dynatrace.oss.junit.kafkatestcontainers.KafkaTestcontainers;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.kafka.KafkaContainer;

@KafkaTestcontainers(
  topics = {
    @KafkaTestcontainers.Topic(name = AbstractKafkaTest.TOPIC_1, partitions = AbstractKafkaTest.PARTITION_COUNT),
    @KafkaTestcontainers.Topic(name = AbstractKafkaTest.TOPIC_2, partitions = AbstractKafkaTest.PARTITION_COUNT)
  }
)
public class KafkaTestcontainersTest extends AbstractKafkaTest {

  @BeforeEach
  void setUp(KafkaContainer kafka) {
    bootstrapAddress = kafka.getBootstrapServers();
  }
}
