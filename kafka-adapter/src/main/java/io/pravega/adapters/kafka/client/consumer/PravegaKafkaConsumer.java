/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.adapters.kafka.client.consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import io.pravega.adapters.kafka.client.config.PravegaConfig;
import io.pravega.adapters.kafka.client.config.PravegaConsumerConfig;
import io.pravega.adapters.kafka.client.dataaccess.PravegaReader;
import io.pravega.adapters.kafka.client.dataaccess.Reader;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Serializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;

import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.kafka.clients.consumer.ConsumerRecord.NO_TIMESTAMP;
import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_CHECKSUM;
import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_SIZE;

@NotThreadSafe
@Slf4j
public class PravegaKafkaConsumer<K, V> implements Consumer<K, V> {

    private static final int DEFAULT_READ_TIMEOUT_IN_MILLIS = 500;

    private static final int DEFAULT_RECORDS_TO_READ_PER_READER_AND_ITERATION = 10;

    private static final int DUMMY_PARTITION_NUM = 0;

    private static final int DUMMY_OFFSET = 0;

    private final List<ConsumerInterceptor<K, V>> interceptors;

    private final String controllerUri;

    private final String scope;

    private final String readerGroupId;

    private final String readerId;

    private final int readTimeout;

    private final int maxPollRecords;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final AtomicBoolean isWoken = new AtomicBoolean(false);

    private final Serializer deserializer;

    private final ExecutorService asyncTasksExecutor = Executors.newCachedThreadPool();

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    @Setter(AccessLevel.PACKAGE)
    private Map<String, Reader<V>> readersByStream = new HashMap<>();

    private Set<TopicPartition> topicPartitionsAssigned;

    public PravegaKafkaConsumer(final Properties kafkaConfigProperties) {
        this(kafkaConfigProperties, null, null);
    }

    public PravegaKafkaConsumer(final Properties configProperties,
                                Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        if (keyDeserializer != null) {
            configProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    keyDeserializer.getClass().getCanonicalName());
        }

        if (valueDeserializer != null) {
            configProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    valueDeserializer.getClass().getCanonicalName());
        }

        PravegaConsumerConfig config = new PravegaConsumerConfig(configProperties);
        controllerUri = config.evaluateServerEndpoints();
        scope = config.getScope() != null ? config.getScope() : PravegaConfig.DEFAULT_SCOPE;
        deserializer = config.getSerializer();
        readerGroupId = config.evaluateGroupId(UUID.randomUUID().toString());
        readerId = config.evaluateClientId("default-readerId");
        interceptors = (List) (new ConsumerConfig(configProperties)).getConfiguredInstances(
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptor.class);
        readTimeout = config.getReadTimeoutInMs();
        maxPollRecords = config.getMaxPollRecords();
    }

    /**
     * Returns assigned topic partitions.
     *
     * In Pravega manual assignment of segments is not applicable, as segments (or partitions) are not fixed and
     * can scale dynamically.
     */
    @Override
    public Set<TopicPartition> assignment() {
        log.trace("assignment() called");
        // final Set<TopicPartition> result = new HashSet<>();
        // this.readersByStream.keySet().stream().forEach(topic -> result.add(new TopicPartition(topic, 0)));
        if (topicPartitionsAssigned != null) {
            log.debug("Returning a result of size {}", topicPartitionsAssigned.size());
            return this.topicPartitionsAssigned;
        } else {
            log.debug("No assigned topic partitions");
            return new HashSet<>();
        }
    }

    /**
     * Fetches the topics/segments that the serialization is subscribed to.
     *
     * @return The set of segments that this serialization is subscribed to
     */
    @Override
    public Set<String> subscription() {
        log.trace("subscription()");
        return this.readersByStream.keySet();
    }

    @Override
    public void subscribe(Collection<String> topics) {
        log.trace("subscribe(topics)");
        subscribe(topics, null);
    }

    @Override
    public void subscribe(@NonNull Collection<String> topics, ConsumerRebalanceListener callback) {
        log.trace("Subscribing to topics: {}, with callback {}", topics, callback);
        ensureNotClosed();
        closeAllReaders();
        readersByStream = new HashMap<>();

        int i = 0;
        for (String topic : topics) {
            i++;
            if (!readersByStream.containsKey(topic)) {
                String readerGroupName = this.readerGroupId;
                if (topics.size() > 1) {
                    readerGroupName = readerGroupName + "-" + i;
                }
                // The reason we are not reusing existing readers is because in the case of multiple topics,
                // the internal topic name might change depending of the index of the topic in the list.
                PravegaReader reader = new PravegaReader(this.scope, topic, this.controllerUri, this.deserializer,
                        readerGroupName, this.readerId);
                readersByStream.put(topic, reader);
            }
        }
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        log.trace("subscribe({}, callback)", pattern);
        throw new UnsupportedOperationException("Subscribing to topic(s) matching specified pattern is not supported");
    }

    @Override
    public void subscribe(Pattern pattern) {
        log.trace("subscribe({})", pattern);
        subscribe(pattern, null);
    }

    @Override
    public void unsubscribe() {
        log.trace("unsubscribe(). Un-subscribing from all topics");
        ensureNotClosed();
        closeAllReaders();

        // Reinitialize
        readersByStream = new HashMap<>();
    }

    private void closeAllReaders() {
        readersByStream.forEach((k, v) -> {
            try {
                v.close();
            } catch (Exception e) {
                log.warn("Unable to close the connection: {}", e.getMessage());
            }
        });
    }

    /**
     * In Kafka, this method assigns the consumer to topic partitions, when using application-defined partition
     * load balancing across consumers. (See the background below)
     *
     * In Pravega you cannot assign partitions (segments) manually. Pravega segments typically auto-scale up and down.
     * So, in theory it should be OK to ignore calls to this method.
     *
     * However, this method also gets invoked internally by Flink Kafka connector, which does not invoke
     * `subscribe(topics)`. To allow the connector to work, here we map it to subscribe() method.
     *
     * Background:
     *
     * (Source: Adapted from https://blog.newrelic.com/engineering/effective-strategies-kafka-topic-partitioning/
     *
     * Kafka balances load across the consumers in a consumer group. When a consumer leaves or join a consumer group
     * the brokers automatically re-belance the partitions, which means all consumers drop existing partitions
     * assigned to them and are reassigned partitions. If the application depends on the state associated with
     * consumed data, it needs to drop that start and afresh.
     *
     * For applications that cannot drop the state associated with consumed data, theey can alternatively not use a
     * consumer group and statically assign partitions to the consumer. In that case the application must balance the
     * partitions by itself. This is where this method comes int picture.
     *
     * @param partitions topic partitions that specify which partitions of which topics are assigned to this consumer.
     */
    @Override
    public void assign(@NonNull Collection<TopicPartition> partitions) {
        log.trace("assign(partitions)");

        if (partitions.size() < 1) {
            throw new IllegalArgumentException("Empty partitions specified");
        }
        final Collection<String> topics = new ArrayList<>();
        partitions.stream().forEach(tp -> topics.add(tp.topic()));

        // TODO: Decouple assignment from subscription. And implement the logic for allowing either (but not both)
        //  of them.
        log.debug("invoking subscribe for topics {}", topics);
        this.subscribe(topics);
        this.topicPartitionsAssigned = new HashSet<>(partitions);
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        log.trace("poll(duration)");
        return poll(timeout.toMillis());
    }

    /**
     * Returns a map of records by topic/stream.
     *
     * @param timeout
     * @return
     */
    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        log.trace("poll(timeout)");
        ensureNotClosed();
        if (timeout <= -1) {
            throw new IllegalArgumentException("Specified timeout is a negative value");
        }

        if (!isSubscribedToAnyTopic()) {
            throw new IllegalStateException("This consumer is not subscribed to any topics/Pravega streams");
        }

        // Here's are the key salient points on implementation:
        // - On each poll, serialization should return the records (representing) since last read position in the
        // segments.
        // In Kafka, the last read position/offset is set either manually (through a seek() call) or automatically
        // based on a auto commit configuration. In Pravega too it can be set manually or automatically (by default).
        // For now, we'll not set the offsets manually.
        //
        // - Timeouts:
        //      - If 0, return immediately with whatever records that are in the buffer.
        //      - Throw exception if negative.
        //
        // - Exceptions
        //      - WakeupException - if wakeup() is called before or during invocation
        //      - InterruptException - if the calling thread is interrupted is called before or during invocation
        //      - AuthenticationException - If authentication fails.
        //      - AuthorizationException - if caller doesnot have access to the stream segments
        //
        //

        // Note: Here, we are assuming a timeout of DEFAULT_READ_TIMEOUT_IN_MILLIS (=500 ms) if timeout = 0. In
        // KafkaConsumer, on the other hand, all the preexisting records in the buffer are immediately returned
        // without any delay.
        ConsumerRecords<K, V> consumerRecords = read(timeout > 0 ? timeout : DEFAULT_READ_TIMEOUT_IN_MILLIS,
                DEFAULT_RECORDS_TO_READ_PER_READER_AND_ITERATION);
        return invokeInterceptors(this.interceptors, consumerRecords);
    }

    private boolean isSubscribedToAnyTopic() {
        return this.readersByStream.size() > 0;
    }

    @VisibleForTesting
    ConsumerRecords<K, V> read(long timeout, int numRecordsPerReaderInEachIteration) {
        log.trace("read(..) invoked with timeout: {} and numRecordsPerReaderInEachIteration: {}", timeout,
                numRecordsPerReaderInEachIteration);
        long startTimeInMillis = System.currentTimeMillis();
        AtomicInteger totalCountOfRecords = new AtomicInteger(0);

        assert timeout > 0;
        assert numRecordsPerReaderInEachIteration > 0;
        ensureNotClosed();

        // We use this to honor the timeout, on a best effort basis. The timeout out not strict - there will be cases
        // where result is returned in a duration that is slightly later than the specified timeout.
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        final Map<TopicPartition, List<ConsumerRecord<K, V>>> recordsByPartition = new HashMap<>();
        log.debug("Size of readersByStream={}", this.readersByStream.size());

        // Check that we haven't crossed the timeout yet before starting the iteration again
        while (stopWatch.getTime() < timeout) {
            long finalTimeout = timeout;

            this.readersByStream.entrySet().stream().forEach(i -> {
                ensureNotClosed();
                ensureNotWokenUp();

                // Check that we haven't crossed the timeout yet before initiating reads from the next reader.
                if (stopWatch.getTime() < finalTimeout) {
                    String stream = i.getKey();
                    log.debug("Reading data for scope/stream [{}/{}]", scope, i.getKey());

                    TopicPartition topicPartition = new TopicPartition(stream, DUMMY_PARTITION_NUM);
                    Reader<V> reader = i.getValue();

                    List<ConsumerRecord<K, V>> recordsToAdd = new ArrayList<>();
                    EventRead<V> event = null;

                    int countOfReadEvents = 0;
                    do {
                        try {
                            event = reader.readNextEvent(readTimeout);
                            if (event != null && event.getEvent() != null) {
                                log.trace("Found a non-null event");
                                recordsToAdd.add(translateToConsumerRecord(stream, event));
                                countOfReadEvents++;
                                totalCountOfRecords.addAndGet(1);
                            }
                        } catch (ReinitializationRequiredException e) {
                            throw e;
                        }
                    } while (event != null && event.getEvent() != null
                            && countOfReadEvents <= numRecordsPerReaderInEachIteration
                            && totalCountOfRecords.get() <= this.maxPollRecords
                            && stopWatch.getTime() < finalTimeout);

                    if (!recordsToAdd.isEmpty()) {
                        log.debug("{} records to add", recordsToAdd.size());
                        if (recordsByPartition.containsKey(topicPartition)) {
                            recordsByPartition.get(topicPartition).addAll(recordsToAdd);
                        } else {
                            recordsByPartition.put(topicPartition, recordsToAdd);
                        }
                    } else {
                        log.debug("No records to add");
                    }
                }
            });
        }
        log.debug("Returning {} records in {} ms. against a timeout of {} ms.",
                totalCountOfRecords.get(), System.currentTimeMillis() - startTimeInMillis, timeout);
        return new ConsumerRecords<K, V>(recordsByPartition);
    }

    private ConsumerRecord<K, V> translateToConsumerRecord(String stream, EventRead<V> event) {
        // Refers to the offset that points to the record in a partition
        return new ConsumerRecord(stream, DUMMY_PARTITION_NUM, DUMMY_OFFSET, NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
                NULL_CHECKSUM, NULL_SIZE, NULL_SIZE, null, event.getEvent());
    }

    private ConsumerRecords invokeInterceptors(List<ConsumerInterceptor<K, V>> interceptors,
                                               ConsumerRecords consumerRecords) {
        ConsumerRecords processedRecords = consumerRecords;
        for (ConsumerInterceptor interceptor : interceptors) {
            try {
                processedRecords = interceptor.onConsume(processedRecords);
            } catch (Exception e) {
                log.warn("Encountered exception executing interceptor {}.", interceptor.getClass().getCanonicalName(),
                        e);
                // ignore
            }
        }
        return processedRecords;
    }

    @Override
    public void commitSync() {
        log.trace("commitSync()");
    }

    @Override
    public void commitSync(Duration timeout) {
        log.trace("commitSync(duration)");
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.trace("commitSync(offsets)");
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        log.trace("commitSync(offsets, timeout)");
    }

    @Override
    public void commitAsync() {
        log.trace("commitAsync()");
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        log.trace("commitAsync(callback)");
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        log.trace("commitAsync(offsets, callback)");
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        log.trace("seek(partition, offset)");
        throw new UnsupportedOperationException("Seek is not supported");
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        log.trace("seek(partition, offsetAndMetadata)");
        throw new UnsupportedOperationException("Seek is not supported");
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        log.trace("seekToBeginning(partitions) invoked");
        throw new UnsupportedOperationException("seekToBeginning is not supported");
    }

    @Override
    public void seekToEnd(@NonNull Collection<TopicPartition> partitions) {
        log.trace("seekToEnd(partitions)");
        if (partitions.size() < 1) {
            throw new IllegalArgumentException("Empty partitions specified");
        }
        if (this.readersByStream.size() < 1) {
            throw new IllegalStateException("This consumer instance is not subscribed to any topics");
        }

        // Check that each of the specified partitions is subscribed to already
        areSubscribed(partitions);
        seekToEndForEachReader(partitions);
    }

    private void seekToEndForEachReader(@NonNull Collection<TopicPartition> partitions) {
        // Multiple items of the specified collection may be for the same topic (stream in Pravega speak). Since we
        // maintain a map of readers by stream, we only need to call the reader's seekToEnd() once. This variable is
        // used to track the topics for which the reader's seekToEnd() is already invoked.
        final Set<String> processedTopics = new HashSet<>();

        for (TopicPartition topicPartition : partitions) {
            String stream = topicPartition.topic();
            if (this.readersByStream.containsKey(stream)) {
                Reader reader = this.readersByStream.get(stream);
                if (!processedTopics.contains(stream)) {
                    reader.seekToEnd();
                    processedTopics.add(topicPartition.topic());
                }
            } else {
                throw new IllegalStateException("This consumer instance is not subscribed to/assigned some of the "
                        + "specified topics");
            }
        }
    }

    private void areSubscribed(@NonNull Collection<TopicPartition> partitions) {
        partitions.forEach(topicPartition -> {
            if (this.readersByStream.get(topicPartition.topic()) == null) {
                throw new IllegalStateException("This consumer instance is not subscribed to/assigned some of the "
                        + "specified topics");
            }
        });
    }

    @Override
    public long position(TopicPartition partition) {
        log.trace("position(partition)");
        return position(partition, Duration.ofSeconds(Integer.MAX_VALUE));
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        log.trace("position(partition, duration)");
        return -1;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        log.trace("committed(partition)");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        log.trace("committed(partition, timeout)");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        log.trace("committed(partitions)");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
        log.trace("committed(partitions, duration)");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        log.trace("metrics()");
        // We don't throw an unsupported exception here so that clients such as Flink Kafka Connector don't encounter
        // an exception.
        return new HashMap<>();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        log.trace("partitionsFor(topic)");
        return partitionsFor(topic, Duration.ofMillis(200));
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        log.trace("partitionsFor(topic, timeout)");

        // This method is internally invoked by Flink Kafka adapter.
        PartitionInfo info = new PartitionInfo(topic, 0, null, null, null);
        List<PartitionInfo> result = new ArrayList<>();
        result.add(info);
        return result;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        log.trace("listTopics()");
        final Map<String, List<PartitionInfo>> result = new HashMap<>();
        this.readersByStream.keySet().stream().forEach(topic ->
                result.put(topic, Arrays.asList(
                        new PartitionInfo(topic, 0, null, null, null))));
        log.trace("Returning result = {}", result);
        return result;
    }

    @SneakyThrows
    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        log.trace("listTopics(timeout)");
        return SimpleTimeLimiter.create(asyncTasksExecutor).callWithTimeout(
                () -> listTopics(),
                timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public Set<TopicPartition> paused() {
        log.trace("paused()");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        log.trace("pause(partitions)");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        log.trace("resume(partitions)");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        log.trace("offsetsForTimes(timestampsToSearch)");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                                   Duration timeout) {
        log.trace("offsetsForTimes(timestampsToSearch, timeout)");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        log.trace("beginningOffsets(partitions)");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        log.trace("beginningOffsets(partitions, duration)");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        log.trace("endOffsets(partitions)");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        log.trace("endOffsets(partitions, duration)");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        log.trace("currentLag(topicPartition)");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        log.trace("groupMetadata()");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void enforceRebalance() {
        log.trace("enforceRebalance()");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void enforceRebalance(String reason) {
        log.trace("enforceRebalance(reason)");
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void close() {
        log.trace("close()");
        close(Duration.ofNanos(Long.MAX_VALUE));
    }

    @SneakyThrows
    private void close(long timeout, TimeUnit unit) {
        log.trace("close(timeout, timeunit");
        SimpleTimeLimiter.create(asyncTasksExecutor).runWithTimeout(() -> cleanup(), timeout, unit);
    }

    @Override
    public void close(Duration timeout) {
        log.trace("close(duration)");
        close(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void cleanup() {
        log.debug("Closing the consumer");
        if (!isClosed.get()) {
            closeAllReaders();
            isClosed.set(true);
        }
    }

    @Override
    public void wakeup() {
        log.trace("wakeup()");
        // Is invoked by Flink Kafka Connector
        isWoken.set(true);
    }

    private void ensureNotClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("This instance is closed already");
        }
    }

    private void ensureNotWokenUp() {
        if (this.isWoken.get()) {
            this.isWoken.set(true);
            throw new WakeupException();
        }
    }
}
