/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.samplesapps.flinkconnector;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Flink data source that pulls dummy data continuously.
 *
 */
@Slf4j
public class FlinkSourceShowcase {

    public static void main(String[] args) throws Exception {
        log.info("Starting the app");
        SourceFunction dataSource = new SampleFlinkSource("message", 1000);
        // SourceFunction dataSource = new SampleRichParallelFlinkSource("message", 1000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env.addSource(dataSource);
        stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return true;
            }
        }).print();

        env.execute();

        Thread.sleep(20 * 1000);
        dataSource.cancel();
        // dataSource.close();

        log.info("Exiting App...");
    }
}

@Slf4j
class SampleFlinkSource implements SourceFunction<String> {

    private final long interval;

    private final String messagePrefix;

    private volatile boolean isRunning = true;

    private AtomicInteger counter = new AtomicInteger(0);

    public SampleFlinkSource(String messagePrefix, long intervalInMilliseconds) {
        this.interval = intervalInMilliseconds;
        this.messagePrefix = messagePrefix;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                // ignore
            }
            String message = messagePrefix + ": " + this.counter.incrementAndGet();
            log.debug("Returning message: {}", message);

            // See other operations over ctx here: https://ci.apache.org/projects/flink/flink-docs-release-1.7/
            // api/java/org/apache/flink/streaming/api/functions/source/SourceFunction.SourceContext.html
            // ctx.collect(message);
            ctx.collectWithTimestamp(message, System.currentTimeMillis());
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

@Slf4j
class SampleRichParallelFlinkSource extends RichParallelSourceFunction<String> {

    private final long interval;

    private final String messagePrefix;

    private volatile boolean isRunning = true;

    private AtomicInteger counter = new AtomicInteger(0);

    public SampleRichParallelFlinkSource(String messagePrefix, long intervalInMilliseconds) {
        this.interval = intervalInMilliseconds;
        this.messagePrefix = messagePrefix;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                // ignore
            }
            String message = messagePrefix + ": " + this.counter.incrementAndGet();
            log.debug("Returning message: {}", message);

            // See other operations over ctx here: https://ci.apache.org/projects/flink/flink-docs-release-1.7/
            // api/java/org/apache/flink/streaming/api/functions/source/SourceFunction.SourceContext.html
            // ctx.collect(message);
            ctx.collectWithTimestamp(message, System.currentTimeMillis());
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
