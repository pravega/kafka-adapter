/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
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
