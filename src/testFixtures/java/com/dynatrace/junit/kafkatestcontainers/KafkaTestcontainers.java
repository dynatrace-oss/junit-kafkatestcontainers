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

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.ContextCustomizerFactories;

/**
 * Bootstraps a real Kafka broker in a Docker container via Testcontainers for use in plain JUnit 5
 * and Spring-based tests, as an alternative to {@code @EmbeddedKafka} when a more production-like,
 * up-to-date broker is needed.
 *
 * <p>Requires a running Docker daemon on the host machine.
 *
 * <p>This annotation is {@link java.lang.annotation.Inherited @Inherited}: it can be placed on an
 * abstract base test class to share a Kafka configuration across subclasses. A subclass may redeclare
 * the annotation to replace the inherited configuration entirely.
 *
 * <h3>Plain JUnit 5</h3>
 * <p>The running {@code KafkaContainer} is available for injection as a test method parameter:
 * <pre>{@code
 * @KafkaTestcontainers(topics = @KafkaTestcontainers.Topic(name = "my-topic"))
 * class MyKafkaTest {
 *
 *     @Test
 *     void myTest(KafkaContainer kafkaContainer) {
 *         String bootstrapServers = kafkaContainer.getBootstrapServers();
 *         // ...
 *     }
 * }
 * }</pre>
 *
 * <h3>Spring</h3>
 * <p>The broker's bootstrap address is automatically injected into the Spring test environment under
 * {@code spring.kafka.bootstrap-servers} (configurable via {@link #bootstrapProperty()}), allowing it
 * to be referenced in {@code application.yaml} or test property files without manual wiring.
 * The container is also registered as a Spring bean named {@code kafkaTestcontainers}.
 * <pre>{@code
 * @KafkaTestcontainers(
 *     topics = {
 *         @KafkaTestcontainers.Topic(name = "${some.application.property}", partitions = 3),
 *         @KafkaTestcontainers.Topic(name = "my-events")
 *     }
 * )
 * @SpringBootTest
 * class MyKafkaTest {
 *
 *     @Value("${spring.kafka.bootstrap-servers}")
 *     private String bootstrapServers;
 * }
 * }</pre>
 *
 * <p>When used on a nested Spring test class, {@code @DirtiesContext} must also be declared on that
 * class to ensure the Spring context is reloaded with the correct bootstrap address for each nested class.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@ExtendWith(KafkaTestcontainersExtension.class)
@ContextCustomizerFactories(KafkaTestcontainersContextCustomizerFactory.class)
public @interface KafkaTestcontainers {

  Topic[] topics() default {};

  /**
   * The Spring property key under which the broker's bootstrap address is injected.
   * Override only when the application uses a non-default property key for Kafka bootstrap servers,
   * e.g. when multiple Kafka clusters are configured.
   */
  String bootstrapProperty() default "spring.kafka.bootstrap-servers";

  /**
   * Allows specifying a custom Kafka broker version when a test requires a specific version that differs from the
   * Kafka-Client version, e.g. to verify compatibility with older brokers.
   * Defaults to the project's Kafka-Client dependency version, which mirrors the broker version by convention.
   */
  String version() default "";

  /**
   * Default partition count applied to every topic that does not specify its own {@link Topic#partitions()}.
   * Individual topics can override this value by setting {@link Topic#partitions()} to a positive number.
   * Topics that are auto-created by the tests will also rely on the default partition count
   */
  int partitions() default 1;

  /**
   * Additional environment variables to pass to the Kafka container.
   * These are applied after the library's own settings (such as {@code KAFKA_NUM_PARTITIONS}),
   * so user-supplied values take precedence and can override library-managed variables.
   */
  EnvVar[] envVars() default {};

  @interface Topic {
    String name();

    /**
     * Partition count for this topic. When {@code 0} (the default), the value is inherited from
     * {@link KafkaTestcontainers#partitions()}. Set to a positive number to override for this topic.
     */
    int partitions() default 0;
  }

  @interface EnvVar {
    String name();

    String value();
  }
}
