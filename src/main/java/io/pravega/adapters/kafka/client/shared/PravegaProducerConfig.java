package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.stream.Serializer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
public class PravegaProducerConfig extends PravegaKafkaConfig {

    public static final String VALUE_SERIALIZER =
            org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

    @Getter
    private final Serializer serializer;

    @Getter
    private final int numSegments;

    @Getter
    private ProducerInterceptors interceptors;

    public PravegaProducerConfig(Properties props) {
        super(props);
        String valueSerializerFqcn = props.getProperty(VALUE_SERIALIZER);
        if (valueSerializerFqcn == null || valueSerializerFqcn.trim().equals("")) {
            throw new IllegalArgumentException("Value serializer not specified");
        } else {
            serializer = this.instantiateSerde(props.getProperty(VALUE_SERIALIZER));
        }
        numSegments = this.getPravegaConfig().getNumSegments();
        interceptors = instantiateInterceptors(props);
    }

    private static <K, V> ProducerInterceptors<K, V> instantiateInterceptors(Properties properties) {
        List<ProducerInterceptor<K, V>> ic = new ArrayList<ProducerInterceptor<K, V>>();
        ProducerInterceptors result = new ProducerInterceptors<K, V>(ic);

        String producerInterceptorClass = properties.getProperty(
                org.apache.kafka.clients.producer.ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);

        if (producerInterceptorClass != null) {
            try {
                ProducerInterceptor<K, V> interceptor =
                        (ProducerInterceptor) Class.forName(producerInterceptorClass).newInstance();
                ic.add(interceptor);
                log.debug("Adding interceptor [{}] to the producer interceptor list", interceptor);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                log.error("Unable to instantiate producer interceptor with name [{}]", producerInterceptorClass, e);
                throw new IllegalStateException(e);
            }
        }
        return result;
    }


    /*@SuppressFBWarnings(value = "IP_PARAMETER_IS_DEAD_BUT_OVERWRITTEN", justification = "No choice here")
    public <K, V> void populateProducerInterceptors(ProducerInterceptors<K, V> interceptors) {
        List<ProducerInterceptor<K, V>> ic = new ArrayList<ProducerInterceptor<K, V>>();

        // TODO: Support multiple producer interceptors?
        String producerInterceptorClass = this.getProperties().getProperty(
                org.apache.kafka.clients.producer.ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
        if (producerInterceptorClass != null) {
            try {
                ProducerInterceptor<K, V> interceptor =
                        (ProducerInterceptor) Class.forName(producerInterceptorClass).newInstance();
                ic.add(interceptor);
                log.debug("Adding interceptor [{}] to the producer interceptor list", interceptor);
                interceptors = new ProducerInterceptors<K, V>(ic);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                log.error("Unable to instantiate producer interceptor with name [{}]", producerInterceptorClass, e);
                throw new IllegalStateException(e);
            }
        }
    }*/
}
