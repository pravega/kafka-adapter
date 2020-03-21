package org.example.kafkaclient.shared;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.example.kafkaclient.sampleapps.ProducerAndConsumerAppWithMinimalKafkaConfig;

@Slf4j
public class Utils {

    @SuppressWarnings("checkstyle:regexp")
    public static void waitForEnterToContinue(String message) {
        System.out.println("\n" + message + "\n");
        try {
            System.in.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Properties loadConfigFromClasspath(String propertiesFileName) {
        Properties props = new Properties();
        try (InputStream input = ProducerAndConsumerAppWithMinimalKafkaConfig.class.getClassLoader()
                .getResourceAsStream(propertiesFileName)) {
            if (input == null) {
                log.error("Unable to find app.properties in classpath");
            }
            props.load(input);
        } catch (IOException e) {
            log.error("Unable to load app.properties from classpath");
            throw new RuntimeException(e);
        }
        return props;
    }
}
