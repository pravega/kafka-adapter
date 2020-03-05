package io.pravega.adapters.kafka.client.consumer;

import io.pravega.adapters.kafka.client.shared.PravegaKafkaConfig;
import io.pravega.adapters.kafka.client.shared.PravegaReader;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Serializer;

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
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;

import static org.apache.kafka.clients.consumer.ConsumerRecord.NO_TIMESTAMP;
import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_CHECKSUM;
import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_SIZE;

@Slf4j
public class PravegaKafkaConsumer<K, V> implements Consumer<K, V> {

    private final ConsumerConfig consumerConfig;

    private final List<ConsumerInterceptor<K, V>> interceptors;

    private final String controllerUri;

    private final String scope;

    private final Map<String, PravegaReader> readersByStream = new HashMap<>();

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final Serializer serializer;

    public PravegaKafkaConsumer(Properties kafkaConfigProperties) {
        this(kafkaConfigProperties, null, null);
    }

    public PravegaKafkaConsumer(Properties kafkaConfigProperties,
                                Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        consumerConfig = new ConsumerConfig(kafkaConfigProperties);

        PravegaKafkaConfig config = new PravegaKafkaConfig(kafkaConfigProperties);
        controllerUri = config.serverEndpoints();
        scope = config.scope(PravegaKafkaConfig.DEFAULT_SCOPE);
        serializer = config.serializer();

        interceptors = (List) consumerConfig.getConfiguredInstances(
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptor.class);
    }

    /**
     * In Pravega manual assignment of segments is not applicable, as segments (or partitions) are not fixed and
     * can scale dynamically.
     *
     * @throws UnsupportedOperationException If invoked
     */
    @Override
    public Set<TopicPartition> assignment() {
        throw new UnsupportedOperationException(
                "Manually assigning list of partitions to this serialization is not supported");
    }

    /**
     * Fetches the topics/segments that the serialization is subscribed to.
     *
     * @return The set of segments that this serialization is subscribed to
     */
    @Override
    public Set<String> subscription() {
        log.trace("Returning subscriptions");
        return this.readersByStream.keySet();
    }

    @Override
    public void subscribe(Collection<String> topics) {
        subscribe(topics, null);
    }


    @Override
    public void subscribe(@NonNull Collection<String> topics, ConsumerRebalanceListener callback) {
        log.trace("Subscribing to topics: {}, with callback", topics);
        ensureNotClosed();

        if (readersByStream.size() > 0) {
            readersByStream.forEach((k, v) -> {
                readersByStream.remove(k);
                v.close();
            });
        }
        for (String topic : topics) {
            PravegaReader reader = new PravegaReader(this.scope, topic, this.controllerUri, this.serializer);
            readersByStream.putIfAbsent(topic, reader);
        }
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException("Assigning partitions not supported");
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

    /**
     * Returns a map of records by topic/stream.
     *
     * @param timeout
     * @return
     */
    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        if (timeout < -1) {
            throw new IllegalArgumentException("Specified timeout is a negative value");
        }

        if (!isSubscribedToAnyTopic()) {
            throw new IllegalStateException("This serialization is not subscribed to any topics/Pravega streams");
        }

        // TODO: Implement the poll business logic
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
        log.info("Polling with timeout {}", timeout);
        ConsumerRecords<K, V> consumerRecords = read(timeout);
        return invokeInterceptors(this.interceptors, consumerRecords);
    }

    private boolean isSubscribedToAnyTopic() {
        return this.readersByStream.size() > 0;
    }

    /**
     *
     * @param timeout
     * @return
     */
    private ConsumerRecords<K, V> read(long timeout) {


        // TODO: return immediately with values in the buffer if timeout is 0

        // TODO: Honor the timeout
        final Map<TopicPartition, List<ConsumerRecord<K, V>>> recordsByPartition = new HashMap<>();

        this.readersByStream.entrySet().stream()
                .forEach(i -> {
                    log.debug("Reading data for stream {}", i.getKey());

                    String stream = i.getKey();
                    TopicPartition topicPartition = new TopicPartition(stream, 0);
                    PravegaReader reader = i.getValue();

                    List<ConsumerRecord<K, V>> records = new ArrayList<>();
                    EventRead<String> event = null;
                    do {
                        try {
                            event = reader.readNextEvent();
                            if (event.getEvent() != null) {
                                log.debug("Found a non-null event");
                                records.add(translateToConsumerRecord(stream, event));
                            }
                        } catch (ReinitializationRequiredException e) {
                            throw e;
                        }
                    } while (event.getEvent() != null);
                    if (!records.isEmpty()) {
                        // TODO: Handle the case where the partition is already there in the map.
                        recordsByPartition.put(topicPartition, records);
                    }
                });
        return new ConsumerRecords<K, V>(recordsByPartition);

    }

    private ConsumerRecord translateToConsumerRecord(String stream, EventRead<String> event) {
        int partition = 0;

        // Refers to the offset that points to the record in a partition
        int offset = 0;

        return new ConsumerRecord(stream, partition, offset, NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
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
    public ConsumerRecords<K, V> poll(Duration timeout) {
        return poll(timeout.toMillis());
    }

    @Override
    public void commitSync() {
        // Pravega always "commits", nothing special to do.
    }

    @Override
    public void commitSync(Duration timeout) {
        // Pravega always "commits", nothing special to do.
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // Pravega always "commits", nothing special to do.
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        // Pravega always "commits", nothing special to do.
    }

    @Override
    public void commitAsync() {
        // Pravega always "commits", nothing special to do.
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        // Pravega always "commits", nothing special to do.
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        // Pravega always "commits", nothing special to do.
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
        log.info("Closing the serialization");
        cleanup();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        log.info("Closing the serialization with timeout{} and timeunit: {}", timeout, unit);
        cleanup();
    }

    @Override
    public void close(Duration timeout) {
        log.info("Closing the serialization with timeout: {}", timeout);
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

