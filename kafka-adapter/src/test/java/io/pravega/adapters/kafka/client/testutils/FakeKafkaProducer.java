/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.adapters.kafka.client.testutils;


import io.pravega.adapters.kafka.client.common.ChecksumMaker;

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
public class FakeKafkaProducer<K, V> implements Producer<K, V> {

    private final ProducerInterceptors<K, V> interceptors;

    public FakeKafkaProducer(Properties kafkaConfigProperties) {
        //List<ProducerInterceptor<K, V>> interceptors = new ArrayList<>();
        //interceptors.add(new FakeKafkaProducerInterceptor<>());
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
        TopicPartition topicPartition = new TopicPartition("foobar", 0);
        long timestamp = System.currentTimeMillis();
        int keySize = 2;
        int valueSize = 4;
        Long checksum = ChecksumMaker.computeCRC32Checksum(record.toString());

        RecordMetadata recordMetadata = new RecordMetadata(topicPartition, -1L, -1L,
                timestamp, checksum, keySize, valueSize);
        return CompletableFuture.completedFuture(recordMetadata);
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
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        log.debug("Closing the producer with timeout{} and timeunit: {}", timeout, unit);
    }

    @Override
    public void close(Duration timeout) {
        log.debug("Closing the producer with timeout: {}", timeout);
    }
}
