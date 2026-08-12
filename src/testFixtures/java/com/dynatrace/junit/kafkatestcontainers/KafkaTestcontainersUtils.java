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

  private KafkaTestcontainersUtils() {
  }

  public static AdminClient createAdminClient(String bootstrapServers) {
    return AdminClient.create(Map.of(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
    ));
  }

  @SuppressWarnings("java:S2095")
  public static KafkaContainer createContainerObject(KafkaTestcontainers annotation) {
    String version = annotation.version();
    if (version.isBlank()) {
      version = DEFAULT_VERSION;
    }

    if (annotation.partitions() < 0) {
      throw new IllegalArgumentException(
        "partitions must not be negative, but was: " + annotation.partitions()
      );
    }
    return new KafkaContainer(CONTAINER_IMAGE.withTag(version))
      .withEnv("KAFKA_NUM_PARTITIONS", String.valueOf(annotation.partitions()));
  }

  public static void createTopics(
    KafkaTestcontainers annotation, String bootstrapServers,
    UnaryOperator<String> nameResolver
  ) {
    if (annotation.topics().length == 0) {
      return;
    }
    int defaultPartitions = annotation.partitions();
    List<NewTopic> newTopicList = Arrays.stream(annotation.topics())
                                        .map(topic -> {
                                          if (topic.partitions() < 0) {
                                            throw new IllegalArgumentException(
                                              "Topic '" + topic.name() + "' partitions must not be negative, but was: " + topic.partitions()
                                            );
                                          }
                                          return new NewTopic(
                                            nameResolver.apply(topic.name()),
                                            topic.partitions() > 0 ? topic.partitions() : defaultPartitions,
                                            (short) 1
                                          );
                                        })
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
