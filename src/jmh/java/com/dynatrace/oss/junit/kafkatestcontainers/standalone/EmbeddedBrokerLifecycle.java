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

import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

public class EmbeddedBrokerLifecycle implements BrokerLifecycle {

  private EmbeddedKafkaBroker broker;

  @Override
  public String start() {
    broker = new EmbeddedKafkaKraftBroker(1, 128);
    broker.afterPropertiesSet();
    return broker.getBrokersAsString();
  }

  @Override
  public void stop() {
    broker.destroy();
  }
}
