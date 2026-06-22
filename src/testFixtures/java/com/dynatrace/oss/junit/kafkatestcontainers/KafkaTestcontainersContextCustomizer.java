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
 *
 * Derived from Spring for Apache Kafka Framework
 * Copyright 2016-present the original author or authors.
 *
 * Modifications:
 * - Replaced EmbeddedKafkaBroker with TestContainers KafkaContainer
 * - Refactored property source handling using MapPropertySource
 * - Integrated custom topic creation and container initialization utilities
 */

package com.dynatrace.oss.junit.kafkatestcontainers;

import static com.dynatrace.oss.junit.kafkatestcontainers.KafkaTestcontainersUtils.createContainerObject;
import static com.dynatrace.oss.junit.kafkatestcontainers.KafkaTestcontainersUtils.createTopics;

import java.util.Map;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.MergedContextConfiguration;
import org.testcontainers.kafka.KafkaContainer;

public class KafkaTestcontainersContextCustomizer implements ContextCustomizer {

  private static final String BEAN_NAME = "kafkaTestcontainers";
  private static final String PROPERTY_NAME = "kafkaTestcontainers";

  private final KafkaTestcontainers annotation;

  public KafkaTestcontainersContextCustomizer(KafkaTestcontainers annotation) {
    this.annotation = annotation;
  }

  @Override
  public void customizeContext(ConfigurableApplicationContext context, MergedContextConfiguration mergedConfig) {
    KafkaContainer kafkaContainer = createContainerObject(annotation);
    kafkaContainer.start();

    Map<String, Object> newProperty = Map.of(annotation.bootstrapProperty(), kafkaContainer.getBootstrapServers());
    context.getEnvironment().getPropertySources()
           .addFirst(new MapPropertySource(PROPERTY_NAME, newProperty));

    createTopics(
      annotation, kafkaContainer.getBootstrapServers(),
      context.getEnvironment()::resolvePlaceholders
    );

    if (context instanceof GenericApplicationContext genericApplicationContext) {
      genericApplicationContext.registerBean(
        BEAN_NAME,
        KafkaContainer.class,
        () -> kafkaContainer,
        bd -> bd.setDestroyMethodName("stop")
      );
    } else {
      throw new IllegalStateException(
        "KafkaTestcontainers requires a GenericApplicationContext to register it's bean."
      );
    }
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof KafkaTestcontainersContextCustomizer other && annotation.equals(other.annotation);
  }

  @Override
  public int hashCode() {
    return annotation.hashCode();
  }

}

