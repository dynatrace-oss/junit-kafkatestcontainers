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

package com.dynatrace.oss.junit.kafkatestcontainers.standalone;

import com.dynatrace.oss.junit.kafkatestcontainers.KafkaTestcontainersUtils;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class TestcontainersBrokerLifecycle implements BrokerLifecycle {

  private static final DockerImageName DOCKER_IMAGE_NAME =
    KafkaTestcontainersUtils.CONTAINER_IMAGE.withTag(KafkaTestcontainersUtils.DEFAULT_VERSION);

  private KafkaContainer kafkaContainer;

  @Override
  public String start() {
    kafkaContainer = new KafkaContainer(DOCKER_IMAGE_NAME);
    kafkaContainer.start();
    return kafkaContainer.getBootstrapServers();
  }

  @Override
  public void stop() {
    kafkaContainer.stop();
  }
}
