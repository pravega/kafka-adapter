package io.pravega.adapters.kafka.client.producer;

import io.pravega.adapters.kafka.client.shared.PravegaKafkaConfig;
import io.pravega.adapters.kafka.client.shared.PravegaWriter;
import io.pravega.client.stream.Serializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

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

    private final Map<String, PravegaWriter> writersByStream = new HashMap<>();

    private final Serializer serializer;

    public PravegaKafkaProducer(Properties kafkaConfigProperties) {
        properties = kafkaConfigProperties;

        controllerUri = PravegaKafkaConfig.extractEndpoints(kafkaConfigProperties, null);
        scope = PravegaKafkaConfig.extractScope(kafkaConfigProperties, PravegaKafkaConfig.DEFAULT_SCOPE);
        serializer = PravegaKafkaConfig.extractSerializer(kafkaConfigProperties);

        interceptors = new ProducerInterceptors<>(Arrays.asList(new FakeKafkaProducerInterceptor<>()));
    }

    @Override
    public void initTransactions() {
        log.debug("Initializing transactions");
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        log.debug("Beginning transaction");
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
            throws ProducerFencedException {
        log.debug("Arguments: offsets={}, consumerGroupId={}", offsets, consumerGroupId);
        log.debug("sending offsets to transaction");
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        log.debug("Committing transaction");
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        log.debug("Aborting transacton");
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        log.debug("Arguments: record={}, callback={}", record, callback);
        log.debug("Sending producer record");
        ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
        return doSend(interceptedRecord, callback);
    }

    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        String stream = record.topic();
        PravegaWriter<V> writer;
        if (this.writersByStream.containsKey(stream)) {
            writer = this.writersByStream.get(stream);

        } else {
            writer = new PravegaWriter(scope, stream, controllerUri, serializer);
            this.writersByStream.putIfAbsent(stream, writer);
        }

        final V message = translateToPravegaMessage(record);
        CompletableFuture<RecordMetadata> cf = writer.writeEvent(message)
                .exceptionally(ex -> {
                    log.error("Writing event failed", ex);
                    return null;
                })
                .thenApply(i -> {
                    log.debug("Done writing event message {} to stream {}", message, stream);
                    return prepareRecordMetadata();
                });

        cf.handle((rm, t) -> {
            if (callback != null) {
                log.debug("Callback is not null, invoking it");
                Exception exception = t != null ? new Exception(t) : null;
                callback.onCompletion(rm, exception);
            } else {
                log.debug("Callback is null");
            }
            return null;
        });
        return cf;
    }

    private RecordMetadata prepareRecordMetadata() {
        // TODO: Note that Pravega doesn't return these values upon write, so we are returning dummy values.
        return new RecordMetadata(null, -1, -1, System.currentTimeMillis(),
                null, 0, 0);
    }

    private V translateToPravegaMessage(ProducerRecord<K, V> record) {
        // TODO: Oversimplification right now. What about the key?
        return record.value();
    }

    @Override
    public void flush() {
        log.debug("Flushing");

    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        log.debug("Returning empty partitions for topic: {}", topic);
        return new ArrayList<>();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        log.debug("Returning empty metrics map");
        return new HashMap<>();
    }

    @Override
    public void close() {
        log.debug("Closing the producer");
        cleanup();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        log.debug("Closing the producer with timeout{} and timeunit: {}", timeout, unit);
        cleanup();
    }

    @Override
    public void close(Duration timeout) {
        log.debug("Closing the producer with timeout: {}", timeout);
        cleanup();
    }

    private void cleanup() {
        writersByStream.forEach((k, v) -> v.close());
    }
}
