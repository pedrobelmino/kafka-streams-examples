/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package br.com.pedrobelmino.examples.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.TestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;


public class IntegrationTestUtils {

  private static final int UNLIMITED_MESSAGES = -1;
  private static final long DEFAULT_TIMEOUT = 30 * 1000L;

  public static <K, V> List<V> readValues(final String topic, final Properties consumerConfig, final int maxMessages) {
    final List<KeyValue<K, V>> kvs = readKeyValues(topic, consumerConfig, maxMessages);
    return kvs.stream().map(kv -> kv.value).collect(Collectors.toList());
  }

  public static <K, V> List<KeyValue<K, V>> readKeyValues(final String topic, final Properties consumerConfig) {
    return readKeyValues(topic, consumerConfig, UNLIMITED_MESSAGES);
  }

  public static <K, V> List<KeyValue<K, V>> readKeyValues(final String topic, final Properties consumerConfig, final int maxMessages) {
    final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig);
    consumer.subscribe(Collections.singletonList(topic));
    final Duration pollInterval = Duration.ofMillis(100L);
    final long maxTotalPollTimeMs = 10000L;
    long totalPollTimeMs = 0;
    final List<KeyValue<K, V>> consumedValues = new ArrayList<>();
    while (totalPollTimeMs < maxTotalPollTimeMs && continueConsuming(consumedValues.size(), maxMessages)) {
      final long pollStart = System.currentTimeMillis();
      final ConsumerRecords<K, V> records = consumer.poll(pollInterval);
      final long pollEnd = System.currentTimeMillis();
      totalPollTimeMs += (pollEnd - pollStart);
      for (final ConsumerRecord<K, V> record : records) {
        consumedValues.add(new KeyValue<>(record.key(), record.value()));
      }
    }
    consumer.close();
    return consumedValues;
  }

  private static boolean continueConsuming(final int messagesConsumed, final int maxMessages) {
    return maxMessages <= 0 || messagesConsumed < maxMessages;
  }

  public static <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(
      final Properties consumerConfig,
      final String topic,
      final int expectedNumRecords)
      throws InterruptedException {
    return waitUntilMinKeyValueRecordsReceived(consumerConfig, topic, expectedNumRecords, DEFAULT_TIMEOUT);
  }

  public static <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(final Properties consumerConfig,
                                                                                final String topic,
                                                                                final int expectedNumRecords,
                                                                                final long waitTime) throws InterruptedException {
    final List<KeyValue<K, V>> accumData = new ArrayList<>();
    final long startTime = System.currentTimeMillis();
    while (true) {
      final List<KeyValue<K, V>> readData = readKeyValues(topic, consumerConfig);
      accumData.addAll(readData);
      if (accumData.size() >= expectedNumRecords)
        return accumData;
      if (System.currentTimeMillis() > startTime + waitTime)
        throw new AssertionError("Expected " + expectedNumRecords +
            " but received only " + accumData.size() +
            " records before timeout " + waitTime + " ms");
      Thread.sleep(Math.min(waitTime, 100L));
    }
  }

  public static <V> List<V> waitUntilMinValuesRecordsReceived(final Properties consumerConfig,
                                                              final String topic,
                                                              final int expectedNumRecords) throws InterruptedException {

    return waitUntilMinValuesRecordsReceived(consumerConfig, topic, expectedNumRecords, DEFAULT_TIMEOUT);
  }

  public static <V> List<V> waitUntilMinValuesRecordsReceived(final Properties consumerConfig,
                                                              final String topic,
                                                              final int expectedNumRecords,
                                                              final long waitTime) throws InterruptedException {
    final List<V> accumData = new ArrayList<>();
    final long startTime = System.currentTimeMillis();
    while (true) {
      final List<V> readData = readValues(topic, consumerConfig, expectedNumRecords);
      accumData.addAll(readData);
      if (accumData.size() >= expectedNumRecords)
        return accumData;
      if (System.currentTimeMillis() > startTime + waitTime)
        throw new AssertionError("Expected " + expectedNumRecords +
            " but received only " + accumData.size() +
            " records before timeout " + waitTime + " ms");
      Thread.sleep(Math.min(waitTime, 100L));
    }
  }

  public static <K, V> void assertThatKeyValueStoreContains(final ReadOnlyKeyValueStore<K, V> store, final Map<K, V> expected)
      throws InterruptedException {
    TestUtils.waitForCondition(() ->
            expected.keySet()
                .stream()
                .allMatch(k -> expected.get(k).equals(store.get(k))),
        30000,
        "Expected values not found in KV store");
  }

  public static <K, V> void assertThatOldestWindowContains(final ReadOnlyWindowStore<K, V> store, final Map<K, V> expected)
      throws InterruptedException {
    final Instant fromBeginningOfTime = Instant.EPOCH;
    final Instant toNowInProcessingTime = Instant.now();
    TestUtils.waitForCondition(() ->
        expected.keySet().stream().allMatch(k -> {
          try (final WindowStoreIterator<V> iterator = store.fetch(k, fromBeginningOfTime, toNowInProcessingTime)) {
            if (iterator.hasNext()) {
              return expected.get(k).equals(iterator.next().value);
            }
            return false;
          }
        }),
        30000,
        "Expected values not found in WindowStore");
  }

  static <K, V> Map.Entry<K, V> mkEntry(final K k, final V v) {
    return new Map.Entry<K, V>() {
      @Override
      public K getKey() {
        return k;
      }

      @Override
      public V getValue() {
        return v;
      }

      @Override
      public V setValue(final V value) {
        throw new UnsupportedOperationException();
      }
    };
  }

  @SafeVarargs
  static <K, V> Map<K, V> mkMap(final Map.Entry<K, V>... entries) {
    final Map<K, V> result = new LinkedHashMap<>();
    for (final Map.Entry<K, V> entry : entries) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

  static class NothingSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {

    @Override
    public void configure(final Map<String, ?> configuration, final boolean isKey) {}

    @Override
    public T deserialize(final String topic, final byte[] bytes) {
      if (bytes != null) {
        throw new IllegalArgumentException("Expected [" + Arrays.toString(bytes) + "] to be null.");
      } else {
        return null;
      }
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
      if (data != null) {
        throw new IllegalArgumentException("Expected [" + data + "] to be null.");
      } else {
        return null;
      }
    }

    @Override
    public void close() {}

    @Override
    public Serializer<T> serializer() {
      return this;
    }

    @Override
    public Deserializer<T> deserializer() {
      return this;
    }
  }
}
