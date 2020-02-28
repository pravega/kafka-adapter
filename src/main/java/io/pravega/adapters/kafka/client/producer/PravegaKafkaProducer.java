package io.pravega.adapters.kafka.client.producer;

import io.pravega.adapters.kafka.client.shared.ConfigConstants;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
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

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PravegaKafkaProducer<K, V> implements Producer<K, V> {

    private final Properties properties;

    private final ProducerInterceptors<K, V> interceptors;

    private final ClientConfig clientConfig;

    private final String scope;

    private final StreamManager streamManager;

    private final EventStreamClientFactory clientFactory;

    private final Map<String, EventStreamWriter<String>> writersByStream = new HashMap<>();

    public PravegaKafkaProducer(Properties kafkaConfigProperties) {
        properties = kafkaConfigProperties;
        interceptors = new ProducerInterceptors<>(Arrays.asList(new FakeKafkaProducerInterceptor<>()));

        String controllerURI = this.properties.getProperty(ConfigConstants.CONTROLLER_URI);
        clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(controllerURI))
                .build();

        streamManager = StreamManager.create(clientConfig);
        log.debug("Created a stream manager");

        scope = properties.getProperty(ConfigConstants.SCOPE, "migrated_from_kafka");
        streamManager.createScope(scope);
        log.debug("Created a scope [{}]", scope);

        clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
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
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {
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
        log.debug("Stream: {}", stream);

        // int numSegments = record.partition();
        int numSegments = 1;

        boolean isStreamCreated = streamManager.createStream(scope, stream, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments))
                .build());
        log.debug("Created a stream with name [{}]", stream);

        if (isStreamCreated && writersByStream.get(stream) == null) {
            EventStreamWriter<String> writer = clientFactory.createEventWriter(stream, new JavaSerializer<String>(),
                    EventWriterConfig.builder().build());
            writersByStream.putIfAbsent(stream, writer);
        }

        EventStreamWriter<String> writer = writersByStream.get(stream);
        String message = translateToPravegaMessage(record);
        writer.writeEvent(message).join();
        log.debug("Done writing event message {} to stream {}", message, stream);
        writer.flush();


        CompletableFuture<RecordMetadata> result = new CompletableFuture<>();
        result.complete(prepareRecordMetadata());
        return result;

        /*TopicPartition topicPartition = new TopicPartition("foobar", 0);
        long timestamp = System.currentTimeMillis();
        int keySize = 2;
        int valueSize = 4;
        Long checksum = ChecksumUtils.computeCRC32Checksum(record.toString());

        RecordMetadata recordMetadata = new RecordMetadata(topicPartition, -1L, -1L,
                timestamp, checksum, keySize, valueSize);
        return CompletableFuture.completedFuture(recordMetadata);
        */
    }

    private RecordMetadata prepareRecordMetadata() {
        // TODO: Fix
        return new RecordMetadata(null, -1, -1, System.currentTimeMillis(),
                null, 0, 0);
    }

    private String translateToPravegaMessage(ProducerRecord<K, V> record) {
        // TODO: Oversimplification right now.
        return (String) record.value();
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
        streamManager.close();
        writersByStream.forEach((k, v) -> v.close());
    }
}
