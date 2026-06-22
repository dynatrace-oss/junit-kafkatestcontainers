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

package com.dynatrace.oss.junit.kafkatestcontainers;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.UnaryOperator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.utils.AppInfoParser;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaTestcontainersUtils {

  public static final String DEFAULT_VERSION = AppInfoParser.getVersion();
  public static final String CONTAINER_NAME = "apache/kafka";
  public static final DockerImageName CONTAINER_IMAGE = DockerImageName.parse(CONTAINER_NAME);

  public static AdminClient createAdminClient(String bootstrapServers) {
    return AdminClient.create(Map.of(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
    ));
  }

  public static KafkaContainer createContainerObject(KafkaTestcontainers annotation) {
    String version = annotation.version();
    if (version.isBlank()) {
      version = DEFAULT_VERSION;
    }

    return new KafkaContainer(CONTAINER_IMAGE.withTag(version));
  }

  public static void createTopics(
    KafkaTestcontainers annotation, String bootstrapServers,
    UnaryOperator<String> nameResolver
  ) {
    if (annotation.topics().length == 0) {
      return;
    }
    List<NewTopic> newTopicList = Arrays.stream(annotation.topics())
                                        .map(topic -> new NewTopic(
                                          nameResolver.apply(topic.name()), topic.partitions(),
                                          (short) 1
                                        ))
                                        .toList();
    try (AdminClient client = createAdminClient(bootstrapServers)) {
      client.createTopics(newTopicList).all().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while creating Kafka topics", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to create Kafka topics", e);
    }
  }
}
