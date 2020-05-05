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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import io.pravega.adapters.kafka.client.dataaccess.Reader;
import io.pravega.adapters.kafka.client.testutils.FakeEvent;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.Test;

import static io.pravega.adapters.kafka.client.testutils.TestUtils.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class PravegaKafkaConsumerTests {

    @Test(expected = IllegalArgumentException.class)
    public void instantiationFailsIfBootstrapServersIsImpty() {
        Properties consumerProps = new Properties();
        new PravegaKafkaConsumer<>(consumerProps);
    }

    @Test
    public void instantiationSucceedsWithMinimalConfig() {
        new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());
    }

    @Test
    public void subscribeChangesSubscription() {
        Consumer<String, Object> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());
        assertEquals(0, consumer.subscription().size());

        consumer.subscribe(Arrays.asList("topic-1", "topic-2"));

        Set<String> subscribedTopics = consumer.subscription();
        assertTrue(subscribedTopics.contains("topic-1"));
        assertTrue(subscribedTopics.contains("topic-2"));
    }

    @Test
    public void resubscribeMakesLatestTopicsEffective() {
        Consumer<String, Object> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());
        consumer.subscribe(Arrays.asList("topic-1", "topic-2"));
        consumer.subscribe(Arrays.asList("topic-3", "topic-4"));

        Set<String> subscribedTopics = consumer.subscription();
        assertTrue(subscribedTopics.contains("topic-3"));
        assertTrue(subscribedTopics.contains("topic-4"));
    }

    @Test
    public void resubscribeDoesNotReuseExistingReaders() {
        PravegaKafkaConsumer<String, Object> consumer =
                new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());
        consumer.subscribe(Arrays.asList("topic-1", "topic-2"));

        Reader reusablePravegaReader = consumer.getReadersByStream().get("topic-2");

        // Resubscribe with one of existing topics
        consumer.subscribe(Arrays.asList("topic-2", "topic-3"));

        assertNotSame(reusablePravegaReader, consumer.getReadersByStream().get("topic-2"));
    }

    @Test
    public void unsubscribingEmptiesSubscriptions() {
        PravegaKafkaConsumer<String, Object> consumer =
                new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());
        consumer.subscribe(Arrays.asList("topic-1", "topic-2"));

        assertEquals(2, consumer.subscription().size());
        consumer.unsubscribe();
        assertEquals(0, consumer.subscription().size());
    }

    @Test(expected =  IllegalArgumentException.class)
    public void negativePollTimeoutThrowsException() {
        PravegaKafkaConsumer<String, Object> consumer =
                new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());
        consumer.subscribe(Arrays.asList("topic-1"));

        consumer.poll(-1L);
    }

    @Test(expected =  IllegalStateException.class)
    public void pollWithoutSubscribeThrowsException() {
        PravegaKafkaConsumer<String, Object> consumer =
                new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());
        consumer.poll(1L);
    }

    @Test
    public void testUnsupportedOperationsThrowExceptions() {
        PravegaKafkaConsumer<String, Object> consumer =
                new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());

        Pattern pattern = null;
        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.subscribe(pattern),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.subscribe(pattern, null),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.seek(null, null),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.seek(null, 1L),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.seekToBeginning(null),
                e -> e instanceof UnsupportedOperationException);

        /*assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.seekToEnd(null),
                e -> e instanceof UnsupportedOperationException);*/

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.committed(new TopicPartition("topic", 1)),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.pause(null),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.paused(),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.resume(null),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.offsetsForTimes(null),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.offsetsForTimes(null, null),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.beginningOffsets(null),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.beginningOffsets(null, null),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.endOffsets(null),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.endOffsets(null, null),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.committed(new HashSet<>()),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.committed(new TopicPartition("topic", 1), Duration.ofMillis(1000)),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.committed(new HashSet<>(), Duration.ofMillis(1000)),
                e -> e instanceof UnsupportedOperationException);
    }

    /*@Test
    public void closeInvokesReaderClose() throws Exception {
        PravegaKafkaConsumer<String, String> consumer =
                new PravegaKafkaConsumer<>(prepareDummyCompleteConsumerConfig());

        Reader<String> reader1 = mock(Reader.class);
        Reader<String> reader2 = mock(Reader.class);
        Map<String, Reader<String>> readers = new HashMap<>();
        readers.put("topic-1", reader1);
        readers.put("topic-2", reader2);
        consumer.setReadersByStream(readers);

        consumer.close();

        verify(reader1, times(1)).close();
        verify(reader2, times(1)).close();
    }*/

    @Test(expected = IllegalStateException.class)
    public void pollAfterClosedThrowsException() {
        PravegaKafkaConsumer<String, String> consumer =
                new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());
        consumer.close();
        consumer.poll(1000);
    }

    @Test
    public void invalidConfigRejectedDuringConstruction() {
        assertThrows("Didn't encounter expected exception when bootstrap server was not specified.",
                () -> {
                    // Bootstrap server not configured
                    Properties props = new Properties();
                    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                            "io.pravega.client.stream.impl.JavaSerializer");
                    new PravegaKafkaConsumer<String, String>(props);
                },
                e -> e instanceof IllegalArgumentException);

        assertThrows("Didn't encounter exception when value deserializer wasn't specified.",
                () -> {
                    // Deserializer config not specified
                    Properties props = new Properties();
                    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "whatever");
                    new PravegaKafkaConsumer<String, String>(props);
                },
                e -> e instanceof IllegalArgumentException);
    }

    @Test(expected = TimeoutException.class)
    public void closeTimesOutWhenTimeoutIsTooSmall() {
        Consumer<String, String> consumer =
                new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());
        Collection<String> topics = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            topics.add("topic" + i);
        }
        consumer.subscribe(topics);
        consumer.close(Duration.ofNanos(1));
    }

    @Test
    public void listTopicsReturnsPartitionsByTopic() {
        Consumer<String, Object> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());
        Collection<String> topics = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            topics.add("topic" + i);
        }
        consumer.subscribe(topics);
        Map<String, List<PartitionInfo>> partitionsByTopic = consumer.listTopics();
        partitionsByTopic.containsKey("topic1");
        partitionsByTopic.containsKey("topic5");
    }

    @Test(expected = TimeoutException.class)
    public void listTopicsTimesOurWhenTimeoutIsTooLow() {
        try (Consumer<String, Object> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig())) {
            Collection<String> topics = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                topics.add("topic" + i);
            }
            consumer.subscribe(topics);
            consumer.listTopics(Duration.ofNanos(1));
        }
        log.info("Done listing topics");
    }

    @Test
    public void metricsReturnsEmptyList() {
        try (Consumer<String, Object> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig())) {
            assertNotNull(consumer.metrics());
            assertEquals(0, consumer.metrics().size());
        }
    }

    @Test
    public void partitionsForReturnsDummyData() {
        @Cleanup
        Consumer<String, String> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());
        List<PartitionInfo> partitions = consumer.partitionsFor("test-topic", Duration.ofMillis(50));
        assertSame(1, partitions.size());

        PartitionInfo firstPartition = partitions.get(0);
        assertNotNull(firstPartition);
        assertSame(0, firstPartition.partition());
        assertNull(firstPartition.leader());
        assertNull(firstPartition.inSyncReplicas());
    }

    @Test
    public void wakeupOnLongPollThrowsException() throws InterruptedException, ExecutionException {
        @Cleanup
        Consumer<String, String> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());

        Callable<Boolean> consumerRunner = () -> {
            boolean isWakeupExceptionThrown = false;
            try {
                consumer.subscribe(Arrays.asList("test-topic"));
                while (true) {
                    consumer.poll(Duration.ofMillis(100));
                }
            } catch (WakeupException e) {
                isWakeupExceptionThrown = true;
            }
            return isWakeupExceptionThrown;
        };

        ExecutorService service =  Executors.newSingleThreadExecutor();
        consumer.wakeup();
        boolean isWakeupExceptionCalled = service.submit(consumerRunner).get();
        assertTrue(isWakeupExceptionCalled);
        service.shutdownNow();
    }

    @Test
    public void assignmentReturnsEmptyResult() {
        @Cleanup
        Consumer<String, String> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());

        Set<TopicPartition> topicPartitions = consumer.assignment();
        assertNotNull(topicPartitions);
        assertSame(0, topicPartitions.size());
    }

    @Test
    public void assignmentReturnsNonEmptyResultIfAssignmentIsMade() {
        @Cleanup
        Consumer<String, String> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());

        consumer.assign(Arrays.asList(new TopicPartition("test-topic-1", 0),
                new TopicPartition("test-topic-2", 0)));

        Set<TopicPartition> topicPartitions = consumer.assignment();
        assertNotNull(topicPartitions);
        assertEquals(2, topicPartitions.size());
    }

    @Test
    public void assignThrowsExceptionWhenInputIsNullOrEmpty() {
        @Cleanup
        Consumer<String, String> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());

        assertThrows("Didn't encounter NullPointerException.",
                () -> consumer.assign(null),
                e -> e instanceof NullPointerException);

        assertThrows("Didn't encounter IllegalArgumentException.",
                () -> consumer.assign(Arrays.asList()),
                e -> e instanceof IllegalArgumentException);
    }

    @Test
    public void assignResultsInSubscription() {
        @Cleanup
        Consumer<String, String> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());
        consumer.assign(Arrays.asList(new TopicPartition("test-topic-1", 0),
                new TopicPartition("test-topic-2", 0)));
        Set<String> subscription = consumer.subscription();
        assertNotNull(subscription);
        assertSame(2, subscription.size());
    }

    @Test
    public void dummyOperationsSucceed() {
        @Cleanup
        PravegaKafkaConsumer<String, String> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());
        Duration timeoutDuration = Duration.ofMillis(10);

        consumer.commitSync();
        consumer.commitSync(timeoutDuration);
        consumer.commitSync(new HashMap<>());
        consumer.commitSync(null, timeoutDuration);

        consumer.commitAsync();
        consumer.commitAsync(null);
        consumer.commitAsync(null, null);

        assertEquals(-1, consumer.position(null));
        assertEquals(-1, consumer.position(null, timeoutDuration));
    }

    @Test
    public void seekToEndThrowsExceptionForInvalidInputAndState() {
        @Cleanup
        Consumer<String, String> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());

        assertThrows("Didn't encounter NullPointerException.",
                () -> consumer.seekToEnd(null),
                e -> e instanceof NullPointerException);

        assertThrows("Didn't encounter IllegalArgumentException.",
                () -> consumer.seekToEnd(Arrays.asList()),
                e -> e instanceof IllegalArgumentException);

        assertThrows("Didn't encounter IllegalStateException.",
                () -> consumer.seekToEnd(Arrays.asList(new TopicPartition("topic1", 0))),
                e -> e instanceof IllegalStateException);

        assertThrows("Didn't encounter IllegalStateException.",
                () -> {
                   consumer.subscribe(Arrays.asList("topic1"));
                   consumer.seekToEnd(Arrays.asList(new TopicPartition("topic2", 0)));
                   consumer.unsubscribe();
                }, e -> e instanceof IllegalStateException);

        assertThrows("Didn't encounter IllegalStateException.",
                () -> {
                    consumer.subscribe(Arrays.asList("topic1", "topic2"));
                    consumer.seekToEnd(Arrays.asList(new TopicPartition("topic2", 0),
                            new TopicPartition("topic3", 0)));
                    consumer.unsubscribe();
                }, e -> e instanceof IllegalStateException);
    }

    @Test
    public void seekToEndInvokesReadersSeekToEnd() {
        @Cleanup
        PravegaKafkaConsumer<String, String> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());

        Reader<String> reader = mock(Reader.class);
        Map<String, Reader<String>> readerByStream = new HashMap<>();
        readerByStream.put("test-topic", reader);

        consumer.setReadersByStream(readerByStream);
        consumer.seekToEnd(Arrays.asList(new TopicPartition("test-topic", 0)));
        verify(reader, times(1)).seekToEnd();
    }

    @Test
    public void readRecordsWhenDataSourceHasEvents() {
        @Cleanup
        PravegaKafkaConsumer<String, String> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());

        Reader<String> reader = mock(Reader.class);
        Map<String, Reader<String>> readerByStream = new HashMap<>();
        readerByStream.put("test-topic", reader);
        consumer.setReadersByStream(readerByStream);

        when(reader.readNextEvent(anyLong())).thenReturn(new FakeEvent<String>("event message"));

        ConsumerRecords<String, String> records = consumer.read(100, 5);
        assertNotNull(records);
        assertTrue(records.count() > 0);
    }

    @Test
    public void readRecordsWhenDataSourceHasNoEvents() {
        @Cleanup
        PravegaKafkaConsumer<String, String> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());

        Reader<String> reader = mock(Reader.class);
        Map<String, Reader<String>> readerByStream = new HashMap<>();
        readerByStream.put("test-topic", reader);
        consumer.setReadersByStream(readerByStream);

        when(reader.readNextEvent(anyLong())).thenReturn(null);

        ConsumerRecords<String, String> records = consumer.read(100, 5);
        assertNotNull(records);
        assertEquals(0, records.count());
    }

    @Test
    public void readRecordsWhenDataSourceHasEventsWithNullMessages() {
        @Cleanup
        PravegaKafkaConsumer<String, String> consumer = new PravegaKafkaConsumer<>(prepareDummyConsumerConfig());

        Reader<String> reader = mock(Reader.class);
        Map<String, Reader<String>> readerByStream = new HashMap<>();
        readerByStream.put("test-topic", reader);
        consumer.setReadersByStream(readerByStream);

        when(reader.readNextEvent(anyLong())).thenReturn(new FakeEvent(null));

        ConsumerRecords<String, String> records = consumer.read(100, 5);
        assertNotNull(records);
        assertEquals(0, records.count());
    }



    //region Private methods
    private Properties prepareDummyConsumerConfig() {
        Properties result = new Properties();
        result.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
        result.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        result.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "io.pravega.client.stream.impl.JavaSerializer");
        return result;
    }

    //endregion
}
