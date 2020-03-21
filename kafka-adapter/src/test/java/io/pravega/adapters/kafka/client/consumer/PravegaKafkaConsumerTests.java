package io.pravega.adapters.kafka.client.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import io.pravega.adapters.kafka.client.shared.Reader;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static io.pravega.adapters.kafka.client.utils.TestUtils.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PravegaKafkaConsumerTests {

    @Test(expected = IllegalArgumentException.class)
    public void instantiationFailsIfBootstrapServersIsImpty() {
        Properties consumerProps = new Properties();
        new PravegaKafkaConsumer<>(consumerProps);
    }

    @Test
    public void instantiationSucceedsWithMinimalConfig() {
        new PravegaKafkaConsumer<>(prepareDummyCompleteConsumerConfig());
    }

    @Test
    public void subscribeChangesSubscription() {
        Consumer<String, Object> consumer = new PravegaKafkaConsumer<>(prepareDummyCompleteConsumerConfig());
        assertEquals(0, consumer.subscription().size());

        consumer.subscribe(Arrays.asList("topic-1", "topic-2"));

        Set<String> subscribedTopics = consumer.subscription();
        assertTrue(subscribedTopics.contains("topic-1"));
        assertTrue(subscribedTopics.contains("topic-2"));
    }

    @Test
    public void resubscribeMakesLatestTopicsEffective() {
        Consumer<String, Object> consumer = new PravegaKafkaConsumer<>(prepareDummyCompleteConsumerConfig());
        consumer.subscribe(Arrays.asList("topic-1", "topic-2"));
        consumer.subscribe(Arrays.asList("topic-3", "topic-4"));

        Set<String> subscribedTopics = consumer.subscription();
        assertTrue(subscribedTopics.contains("topic-3"));
        assertTrue(subscribedTopics.contains("topic-4"));
    }

    @Test
    public void resubscribeDoesNotReuseExistingReaders() {
        PravegaKafkaConsumer<String, Object> consumer =
                new PravegaKafkaConsumer<>(prepareDummyCompleteConsumerConfig());
        consumer.subscribe(Arrays.asList("topic-1", "topic-2"));

        Reader reusablePravegaReader = consumer.getReadersByStream().get("topic-2");

        // Resubscribe with one of existing topics
        consumer.subscribe(Arrays.asList("topic-2", "topic-3"));

        assertNotSame(reusablePravegaReader, consumer.getReadersByStream().get("topic-2"));
    }

    @Test
    public void unsubscribingEmptiesSubscriptions() {
        PravegaKafkaConsumer<String, Object> consumer =
                new PravegaKafkaConsumer<>(prepareDummyCompleteConsumerConfig());
        consumer.subscribe(Arrays.asList("topic-1", "topic-2"));

        assertEquals(2, consumer.subscription().size());
        consumer.unsubscribe();
        assertEquals(0, consumer.subscription().size());
    }

    @Test(expected =  IllegalArgumentException.class)
    public void negativePollTimeoutThrowsException() {
        PravegaKafkaConsumer<String, Object> consumer =
                new PravegaKafkaConsumer<>(prepareDummyCompleteConsumerConfig());
        consumer.subscribe(Arrays.asList("topic-1"));

        consumer.poll(-1L);
    }

    @Test(expected =  IllegalStateException.class)
    public void pollWithoutSubscribeThrowsException() {
        PravegaKafkaConsumer<String, Object> consumer =
                new PravegaKafkaConsumer<>(prepareDummyCompleteConsumerConfig());
        consumer.poll(1L);
    }

    @Test
    public void testUnsupportedOperationsThrowExceptions() {
        PravegaKafkaConsumer<String, Object> consumer =
                new PravegaKafkaConsumer<>(prepareDummyCompleteConsumerConfig());

        /*assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.assign(null),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.assignment(),
                e -> e instanceof UnsupportedOperationException);*/

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

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.seekToEnd(null),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.committed(new TopicPartition("topic", 1)),
                e -> e instanceof UnsupportedOperationException);

        /*assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.metrics(),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.partitionsFor("topic"),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.partitionsFor("topic", null),
                e -> e instanceof UnsupportedOperationException);*/

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.pause(null),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.paused(),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.resume(null),
                e -> e instanceof UnsupportedOperationException);

        /*assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.listTopics(),
                e -> e instanceof UnsupportedOperationException);

        assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.listTopics(null),
                e -> e instanceof UnsupportedOperationException);*/

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

        /*assertThrows("Didn't encounter UnsupportedOperationException.",
                () -> consumer.wakeup(),
                e -> e instanceof UnsupportedOperationException);*/

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

    @Test
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
    }

    @Test
    public void commitSyncOperationsSucceed() {

    }

    @Test(expected = IllegalStateException.class)
    public void pollAfterClosedThrowsException() {
        PravegaKafkaConsumer<String, String> consumer =
                new PravegaKafkaConsumer<>(prepareDummyCompleteConsumerConfig());
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

    private Properties prepareDummyCompleteConsumerConfig() {
        Properties result = new Properties();
        result.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
        result.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        result.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "io.pravega.client.stream.impl.JavaSerializer");
        return result;
    }
}
