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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;

class KafkaTestcontainersUtilsValidationTest {

  @Test
  void createContainerObjectThrowsOnNegativePartitions() {
    KafkaTestcontainers annotation = annotationWith(-1);

    assertThatThrownBy(() -> KafkaTestcontainersUtils.createContainerObject(annotation))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("-1");
  }

  @Test
  void createTopicsThrowsOnNegativeTopicPartitions() {
    KafkaTestcontainers.Topic topic = topicWith("my-topic", -3);
    KafkaTestcontainers annotation = annotationWith(1, topic);

    assertThatThrownBy(() -> KafkaTestcontainersUtils.createTopics(annotation, "localhost:9092", s -> s))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("my-topic")
      .hasMessageContaining("-3");
  }

  private static KafkaTestcontainers annotationWith(int partitions, KafkaTestcontainers.Topic... topics) {
    return (KafkaTestcontainers) Proxy.newProxyInstance(
      KafkaTestcontainers.class.getClassLoader(),
      new Class[]{KafkaTestcontainers.class},
      (proxy, method, args) -> {
        if ("partitions".equals(method.getName())) {
          return partitions;
        }
        if ("topics".equals(method.getName())) {
          return topics;
        }
        if ("version".equals(method.getName())) {
          return "";
        }
        if ("envVars".equals(method.getName())) {
          return new KafkaTestcontainers.EnvVar[0];
        }
        if ("annotationType".equals(method.getName())) {
          return KafkaTestcontainers.class;
        }
        return method.getDefaultValue();
      }
    );
  }

  private static KafkaTestcontainers.Topic topicWith(String name, int partitions) {
    return (KafkaTestcontainers.Topic) Proxy.newProxyInstance(
      KafkaTestcontainers.Topic.class.getClassLoader(),
      new Class[]{KafkaTestcontainers.Topic.class},
      (proxy, method, args) -> {
        if ("name".equals(method.getName())) {
          return name;
        }
        if ("partitions".equals(method.getName())) {
          return partitions;
        }
        if ("annotationType".equals(method.getName())) {
          return KafkaTestcontainers.Topic.class;
        }
        return method.getDefaultValue();
      }
    );
  }
}
