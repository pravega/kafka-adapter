package io.pravega.adapters.kafka.client.consumer;

import io.pravega.adapters.kafka.client.shared.PravegaKafkaConfig;
import io.pravega.adapters.kafka.client.shared.PravegaReader;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.ReinitializationRequiredException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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

@Slf4j
public class PravegaKafkaConsumer<K, V> implements Consumer<K, V> {

    private final ConsumerConfig consumerConfig;

    private final List<ConsumerInterceptor<K, V>> interceptors;

    private final String controllerUri;

    private final String scope;

    private final Map<String, PravegaReader> readersByStream = new HashMap<>();

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public PravegaKafkaConsumer(Properties kafkaConfigProperties) {
        consumerConfig = new ConsumerConfig(kafkaConfigProperties);
        interceptors = (List) consumerConfig.getConfiguredInstances(
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptor.class);

        controllerUri = PravegaKafkaConfig.extractEndpoints(kafkaConfigProperties, null);
        scope = PravegaKafkaConfig.extractScope(kafkaConfigProperties, PravegaKafkaConfig.DEFAULT_SCOPE);
    }

    @Override
    public Set<TopicPartition> assignment() {
        log.info("Entered assignment");
        return null;
    }

    @Override
    public Set<String> subscription() {
        log.info("Entered subscription");
        return null;
    }

    @Override
    public void subscribe(Collection<String> topics) {
        subscribe(topics, null);

    }

    @Override
    public void subscribe(@NonNull Collection<String> topics, ConsumerRebalanceListener callback) {
        log.info("Subscribing to topics: {}, with callback", topics);
        ensureNotClosed();

        for (String topic : topics) {
            PravegaReader reader = new PravegaReader(this.scope, topic, this.controllerUri);
            readersByStream.putIfAbsent(topic, reader);
        }
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        log.info("Assigning partitions: {}", partitions);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        throw new UnsupportedOperationException("Subscribing to topic(s) matching specified pattern is not supported");
    }

    @Override
    public void subscribe(Pattern pattern) {
        subscribe(pattern, null);
    }

    @Override
    public void unsubscribe() {
        log.info("Unsubscribing");

    }

    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        log.info("Polling with timeout {}", timeout);
        ConsumerRecords<String, String> consumerRecords = read(timeout);
        return invokeInterceptors(this.interceptors, consumerRecords);
    }

    private ConsumerRecords<String, String> read(long timeout) {
        // TODO: Generify and honor the timeout
        final Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsByPartition = new HashMap<>();

        this.readersByStream.entrySet().stream()
                .forEach(i -> {
                    log.debug("Reading data for stream {}", i.getKey());

                    String stream = i.getKey();
                    TopicPartition topicPartition = new TopicPartition(stream, 0);

                    PravegaReader reader = i.getValue();

                    List<ConsumerRecord<String, String>> records = new ArrayList<>();

                    EventRead<String> event = null;
                    do {
                        try {
                            event = reader.readNextEvent();
                            if (event.getEvent() != null) {
                                log.debug("Found a non-null event");
                                records.add(new ConsumerRecord(stream, 0, 0, null, event.getEvent()));
                            }
                        } catch (ReinitializationRequiredException e) {
                            throw e;
                        }
                    } while (event.getEvent() != null);


                    /*EventRead<String> readEvent = reader.readNextEvent();
                    while (readEvent != null) {
                        String readMessage = readEvent.getEvent();
                        if (readMessage != null) {
                            records.add(new ConsumerRecord(stream, 0, 0, null, readMessage));
                        }
                    }*/
                    if (!records.isEmpty()) {
                        // TODO: Handle the case where the partition is already there in the map.
                        recordsByPartition.put(topicPartition, records);
                    }
                });
        return new ConsumerRecords<String, String>(recordsByPartition);

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
    public ConsumerRecords<K, V> poll(Duration timeout) {
        return poll(timeout.toMillis());
    }

    @Override
    public void commitSync() {

    }

    @Override
    public void commitSync(Duration timeout) {

    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {

    }

    @Override
    public void commitAsync() {

    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {

    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {

    }

    @Override
    public void seek(TopicPartition partition, long offset) {

    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {

    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {

    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {

    }

    @Override
    public long position(TopicPartition partition) {
        return 0;
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return 0;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return null;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return null;
    }

    @Override
    public Set<TopicPartition> paused() {
        return null;
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {

    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {

    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                                   Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return null;
    }

    @Override
    public void close() {
        log.info("Closing the consumer");
        cleanup();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        log.info("Closing the consumer with timeout{} and timeunit: {}", timeout, unit);
        cleanup();
    }

    @Override
    public void close(Duration timeout) {
        log.info("Closing the consumer with timeout: {}", timeout);
        cleanup();
    }

    private void cleanup() {
        if (!isClosed.get()) {
            readersByStream.forEach((k, v) -> v.close());
            isClosed.set(true);
        }
    }

    @Override
    public void wakeup() {
        log.info("Waking up");
    }

    private void ensureNotClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("This PravegaKafkaConsumer instance is closed already");
        }
    }
}

