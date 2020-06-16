/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.adapters.kafka.client.producer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import io.pravega.adapters.kafka.client.common.ChecksumMaker;
import io.pravega.adapters.kafka.client.config.PravegaConfig;
import io.pravega.adapters.kafka.client.config.PravegaProducerConfig;
import io.pravega.adapters.kafka.client.dataaccess.PravegaWriter;
import io.pravega.adapters.kafka.client.dataaccess.Writer;
import io.pravega.client.stream.Serializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.ThreadSafe;

import lombok.NonNull;
import lombok.SneakyThrows;
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

@ThreadSafe
@Slf4j
public class PravegaKafkaProducer<K, V> implements Producer<K, V> {

    private final ProducerInterceptors<K, V> interceptors;

    private final String controllerUri;

    private final String scope;

    private final Map<String, Writer<V>> writersByStream;

    private final Serializer<V> serializer;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final int numSegments;

    public PravegaKafkaProducer(Properties configProperties) {
        this(configProperties, new ConcurrentHashMap<>());
    }

    @VisibleForTesting
    PravegaKafkaProducer(@NonNull final Properties configProperties, Map<String, Writer<V>> writers) {
        PravegaProducerConfig config = new PravegaProducerConfig(configProperties);
        controllerUri = config.evaluateServerEndpoints();
        scope = config.getScope() != null ? config.getScope() : PravegaConfig.DEFAULT_SCOPE;
        serializer = config.getSerializer();
        numSegments = config.getNumSegments();
        interceptors = config.getInterceptors();
        writersByStream = writers;
    }

    @Override
    public void initTransactions() {
        log.trace("initTransactions()");
        ensureNotClosed();
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        log.trace("beginTransaction()");
        ensureNotClosed();
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
            throws ProducerFencedException {
        log.trace("sendOffsetsToTransaction(...)");
        throw new UnsupportedOperationException("Sending offsets to transaction is not supported");
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        log.trace("commitTransaction()");
        ensureNotClosed();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        log.trace("abortTransaction()");
        ensureNotClosed();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        log.trace("send(record)");
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(@NonNull ProducerRecord<K, V> record, Callback callback) {
        log.trace("send(record, callback)");
        ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
        ensureNotClosed();
        return doSend(interceptedRecord, callback);
    }

    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        if (record.topic() == null || record.value() == null) {
            throw new IllegalArgumentException("Specified record is not valid");
        }
        String stream = record.topic();
        final Writer<V> writer;
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
        // Note that Pravega doesn't return these values upon write, so we are returning dummy values.
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
        log.trace("flush()");
        ensureNotClosed();

        this.writersByStream.values().stream().forEach(i -> i.flush());
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        log.trace("partitionsFor(topic)");
        return new ArrayList<>();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        log.trace("metrics()");
        return new HashMap<>();
    }

    @Override
    public void close() {
        log.trace("close()");
        cleanup();
    }

    @SneakyThrows
    @Override
    public void close(long timeout, TimeUnit unit) {
        log.trace("Closing the producer with timeout{} and timeunit: {}", timeout, unit);
        SimpleTimeLimiter.create(Executors.newSingleThreadExecutor()).runUninterruptiblyWithTimeout(() -> cleanup(),
                timeout, unit);
    }

    @Override
    public void close(Duration timeout) {
        close(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void cleanup() {
        isClosed.set(true);
        writersByStream.forEach((k, v) -> {
            try {
                v.close();
            } catch (Exception e) {
                log.warn("Exception in closing the writer", e);
                // ignore
            }
        });
    }

    private void ensureNotClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("This instance is closed already");
        }
    }
}
