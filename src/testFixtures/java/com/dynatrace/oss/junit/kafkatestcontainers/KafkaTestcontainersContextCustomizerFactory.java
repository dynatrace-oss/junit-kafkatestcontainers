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
 * Copyright 2017-present the original author or authors.
 *
 * Modifications:
 * - Replaced EmbeddedKafka annotation with KafkaTestcontainers
 * - Updated to work with TestContainers-based KafkaTestcontainersContextCustomizer
 * - Refactored annotation lookup to use AnnotatedElementUtils
 */

package com.dynatrace.oss.junit.kafkatestcontainers;

import java.util.List;
import org.jspecify.annotations.Nullable;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.test.context.ContextConfigurationAttributes;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.ContextCustomizerFactory;

public class KafkaTestcontainersContextCustomizerFactory implements ContextCustomizerFactory {

  @Override
  public @Nullable ContextCustomizer createContextCustomizer(
    Class<?> testClass, List<ContextConfigurationAttributes> configAttributes
  ) {
    KafkaTestcontainers annotation = AnnotatedElementUtils.findMergedAnnotation(
      testClass, KafkaTestcontainers.class
    );
    if (annotation == null) {
      //Return null to uphold the contract stated by ContextCustomizerFactory.
      return null;
    }
    return new KafkaTestcontainersContextCustomizer(annotation);
  }
}
