package io.pravega.adapters.kafka.client.producer;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.adapters.kafka.client.common.ChecksumMaker;
import io.pravega.adapters.kafka.client.shared.PravegaKafkaConfig;
import io.pravega.adapters.kafka.client.shared.PravegaWriter;
import io.pravega.adapters.kafka.client.shared.Writer;
import io.pravega.client.stream.Serializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

@Slf4j
public class PravegaKafkaProducer<K, V> implements Producer<K, V> {

    private final Properties properties;

    private final ProducerInterceptors<K, V> interceptors;

    private final String controllerUri;

    private final String scope;

    private final Map<String, Writer<V>> writersByStream;

    private final Serializer<V> serializer;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final int numSegments;

    public PravegaKafkaProducer(Properties configProperties) {
        this(configProperties, new HashMap<>());
    }

    @VisibleForTesting
    PravegaKafkaProducer(Properties configProperties, Map<String, Writer<V>> writers) {
        if (configProperties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) == null) {
            throw new IllegalArgumentException(String.format("Property [%s] is not set",
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        }
        properties = configProperties;
        PravegaKafkaConfig config = new PravegaKafkaConfig(properties);

        controllerUri = config.serverEndpoints();
        scope = config.scope(PravegaKafkaConfig.DEFAULT_SCOPE);
        serializer = config.serializer();
        numSegments = config.numSegments();

        interceptors = new ProducerInterceptors<K, V>(new ArrayList<>());
        config.populateProducerInterceptors(interceptors);

        writersByStream = writers;
    }

    @Override
    public void initTransactions() {
        ensureNotClosed();
        log.debug("Initializing transactions");
        // TODO: implementation
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        ensureNotClosed();
        log.debug("Beginning transaction");
        // TODO: implementation
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
            throws ProducerFencedException {
        throw new UnsupportedOperationException("Sending offsets to transaction is not supported");
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        ensureNotClosed();
        log.debug("Committing transaction");
        // TODO: implementation
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        ensureNotClosed();
        log.debug("Aborting transaction");
        // TODO: implementation
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(@NonNull ProducerRecord<K, V> record, Callback callback) {
        log.trace("Arguments: record={}, callback={}", record, callback);
        ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
        ensureNotClosed();
        return doSend(interceptedRecord, callback);
    }

    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        if (record.topic() == null || record.value() == null) {
            throw new IllegalArgumentException("Specified record is not valid");
        }
        String stream = record.topic();
        Writer<V> writer;
        if (this.writersByStream.containsKey(stream)) {
            writer = this.writersByStream.get(stream);

        } else {
            writer = new PravegaWriter(scope, stream, controllerUri, serializer, numSegments);
            this.writersByStream.putIfAbsent(stream, writer);
        }

        final V message = translateToPravegaMessage(record);
        CompletableFuture<RecordMetadata> cf = writer.writeEvent(message)
                .handle((v, ex) -> {
                    if (ex != null) {
                        log.error("Writing event failed", ex);
                        return null;
                    } else {
                        log.trace("Write event message {} to stream {}", message, stream);
                        return prepareRecordMetadata(record);
                    }
                });

        cf.handle((rm, t) -> {
            if (callback != null) {
                log.debug("Callback is not null, invoking it");
                Exception exception = t != null ? new Exception(t) : null;
                callback.onCompletion(rm, exception);
            } else {
                log.trace("Callback is null");
            }
            return null;
        });
        return cf;
    }

    private RecordMetadata prepareRecordMetadata(ProducerRecord<K, V> producerRecord) {
        // TODO: Note that Pravega doesn't return these values upon write, so we are returning dummy values.
        return new RecordMetadata(new TopicPartition(producerRecord.topic(), -1), -1, -1,
                System.currentTimeMillis(),
                ChecksumMaker.computeCRC32Checksum(producerRecord.value().toString()),
                0, 0);
    }

    private V translateToPravegaMessage(ProducerRecord<K, V> record) {
        // TODO: Oversimplification right now. What about the key?
        return record.value();
    }

    @Override
    public void flush() {
        ensureNotClosed();
        log.trace("Flushing");
        this.writersByStream.values().stream().forEach(i -> i.flush());
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        log.trace("Returning empty partitions for topic: {}", topic);
        return new ArrayList<>();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        log.trace("Returning empty metrics map");
        return new HashMap<>();
    }

    @Override
    public void close() {
        log.trace("Closing the producer");
        cleanup();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        log.trace("Closing the producer with timeout{} and timeunit: {}", timeout, unit);
        cleanup();
    }

    @Override
    public void close(Duration timeout) {
        log.trace("Closing the producer with timeout: {}", timeout);
        cleanup();
    }

    private void cleanup() {
        isClosed.set(true);
        writersByStream.forEach((k, v) -> {
            try {
                v.close();
            } catch (Exception e) {
                log.warn("Exception in closing the writer", e);
            }
        });
    }

    private void ensureNotClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("This instance is closed already");
        }
    }
}
