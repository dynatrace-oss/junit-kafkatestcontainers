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

package com.dynatrace.junit.kafkatestcontainers;

import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.DockerClientFactory;

public class TestHelper {

  public static AdminClient getAdminClient(String bootstrapAddress) {
    return AdminClient.create(Map.of(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress
    ));
  }

  public static Optional<String> findKafkaVersion(String containerId) {
    String image = DockerClientFactory.instance().client()
                                      .inspectContainerCmd(containerId)
                                      .exec()
                                      .getConfig()
                                      .getImage();
    return Optional.ofNullable(image)
                   .map(i -> i.contains(":") ? i.split(":", -1)[1] : null);
  }

  public static KafkaProducer<String, String> buildProducer(String bootstrapAddress) {
    return new KafkaProducer<>(Map.of(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
    ));
  }

  public static KafkaConsumer<String, String> buildConsumer(String bootstrapAddress) {
    return new KafkaConsumer<>(Map.of(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress,
      ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
    ));
  }

}
