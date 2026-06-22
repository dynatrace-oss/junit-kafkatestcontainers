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
 * Copyright 2019-present the original author or authors.
 *
 * Modifications:
 * - Replaced EmbeddedKafkaBroker with TestContainers KafkaContainer
 * - Simplified lifecycle using BeforeAllCallback instead of ExecutionCondition
 * - Refactored storage mechanism from ThreadLocal to ExtensionContext.Namespace
 * - Integrated custom topic creation and container initialization
 */

package com.dynatrace.oss.junit.kafkatestcontainers;

import static com.dynatrace.oss.junit.kafkatestcontainers.KafkaTestcontainersUtils.createContainerObject;
import static com.dynatrace.oss.junit.kafkatestcontainers.KafkaTestcontainersUtils.createTopics;

import java.lang.reflect.AnnotatedElement;
import java.util.Arrays;
import java.util.function.UnaryOperator;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Internal JUnit 5 extension that backs {@link KafkaTestcontainers}.
 * Not intended to be used directly — use {@link KafkaTestcontainers} instead.
 */
public class KafkaTestcontainersExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

  private static final ExtensionContext.Namespace NAMESPACE =
    ExtensionContext.Namespace.create(KafkaTestcontainersExtension.class);
  private static final String CONTAINER_KEY = "kafkaContainer";

  @Override
  public void beforeAll(ExtensionContext context) {
    context.getTestClass()
      .filter(cls -> !isSpringTestContext(cls))
      .ifPresent((Class<?> cls) -> {
        KafkaTestcontainers annotation = AnnotatedElementUtils.findMergedAnnotation(
          cls, KafkaTestcontainers.class
        );
        if (annotation == null) {
          throw new IllegalStateException(
            "Usage of KafkaTestcontainersExtension directly is forbidden, "
              + "use instead the annotation @KafkaTestcontainers");
        }
        initializeContainer(context, annotation);
      });
  }

  @Override
  public void afterAll(ExtensionContext context) {
    KafkaContainer container = getFromStore(context);
    if (container != null && container.isRunning()) {
      container.stop();
      context.getStore(NAMESPACE).remove(CONTAINER_KEY);
    }
  }

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
    throws ParameterResolutionException {
    return KafkaContainer.class.isAssignableFrom(parameterContext.getParameter().getType())
      && getFromStore(extensionContext) != null;
  }

  @Override
  public KafkaContainer resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
    throws ParameterResolutionException {
    KafkaContainer container = getFromStore(extensionContext);
    if (container == null) {
      throw new ParameterResolutionException("No KafkaContainer found in extension store");
    }
    return container;
  }

  private void initializeContainer(ExtensionContext context, KafkaTestcontainers annotation) {
    KafkaContainer container = getFromStore(context);
    if (container == null) {
      KafkaContainer kafkaContainer = createContainerObject(annotation);
      kafkaContainer.start();
      createTopics(annotation, kafkaContainer.getBootstrapServers(), UnaryOperator.identity());
      context.getStore(NAMESPACE).put(CONTAINER_KEY, kafkaContainer);
    }
  }

  private boolean isSpringTestContext(AnnotatedElement annotatedElement) {
    return AnnotatedElementUtils.findAllMergedAnnotations(annotatedElement, ExtendWith.class)
                                .stream()
                                .flatMap(extendWith -> Arrays.stream(extendWith.value()))
                                .anyMatch(SpringExtension.class::isAssignableFrom);
  }

  @Nullable
  private KafkaContainer getFromStore(ExtensionContext context) {
    return context.getStore(NAMESPACE).get(CONTAINER_KEY, KafkaContainer.class);
  }
}
