package io.pravega.adapters.kafka.client.integrationtests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({
        AdapterUsageBasicExamples.class,
        AdapterUsageAdvancedExamples.class,
        TransactionExamples.class,
        CustomSerializationUsageExamples.class,
        ReaderAndWriterUsageExamples.class
})

public class IntegrationTestSuite {
}
