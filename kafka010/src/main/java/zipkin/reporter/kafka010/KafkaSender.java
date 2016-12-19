/**
 * Copyright 2016 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.reporter.kafka010;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import zipkin.internal.LazyCloseable;
import zipkin.reporter.BytesMessageEncoder;
import zipkin.reporter.Callback;
import zipkin.reporter.Encoding;
import zipkin.reporter.Sender;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This sends (usually TBinaryProtocol big-endian) encoded spans to a Kafka topic.
 *
 * <p>This sender is thread-safe.
 *
 * <p>This sender has been updated to work with Kafka 0.9 and 0.10 using the new Producer and Consumer APIs.
 * A corresponding update to the Kafka Zipkin Collectors will be necessary.
 */
public class KafkaSender extends LazyCloseable<KafkaProducer<byte[], byte[]>> implements Sender {

    private static final String MAX_MESSAGE_BYTES_CONFIG = "max.message.bytes";

    private static final Map<String, String> ZIPKIN_PRODUCER_DEFAULT_PROPERTIES;
    
    static {
        Map<String, String> map = new HashMap<>();
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        map.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000"); // 5 seconds
        map.put(ProducerConfig.ACKS_CONFIG, "0");
        map.put(MAX_MESSAGE_BYTES_CONFIG, "1000000");
        ZIPKIN_PRODUCER_DEFAULT_PROPERTIES = Collections.unmodifiableMap(map);
    }
            

    private static final String DEFAULT_TOPIC = "zipkin";

    private final Properties producerProperties;
    private final String topic;

    private final Encoding encoding;
    private final BytesMessageEncoder messageEncoder;
    private final int maxMessageBytes;

    /** close is typically called from a different thread */
    transient volatile boolean closeCalled;

    public KafkaSender(String topic, Properties producerProperties, Encoding encoding) {
        this.topic = checkNotNull(topic, "topic");
        this.producerProperties = new Properties();
        this.producerProperties.putAll(checkNotNull(producerProperties, "producerProperties"));

        this.encoding = checkNotNull(encoding, "encoding");
        this.messageEncoder = BytesMessageEncoder.forEncoding(encoding);
        this.maxMessageBytes = Integer.parseInt(this.producerProperties.getProperty(MAX_MESSAGE_BYTES_CONFIG, "10000"));
    }

    public Properties getProducerProperties() {
        return producerProperties;
    }

    public BytesMessageEncoder getMessageEncoder() {
        return messageEncoder;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public int messageMaxBytes() {
        return maxMessageBytes;
    }

    @Override
    public Encoding encoding() {
        return encoding;
    }

    /**
     * Ensures there are no problems reading metadata about the topic.
     */
    @Override
    public CheckResult check() {
        try {
            get().partitionsFor(getTopic()); // make sure we can query the metadata
            return CheckResult.OK;
        } catch (RuntimeException e) {
            return CheckResult.failed(e);
        }
    }

    @Override
    protected KafkaProducer<byte[], byte[]> compute() {
        return new KafkaProducer<>(getProducerProperties());
    }

    @Override
    public int messageSizeInBytes(List<byte[]> encodedSpans) {
        return encoding().listSizeInBytes(encodedSpans);
    }

    /**
     * This sends all of the spans as a single message.
     *
     * <p>NOTE: this blocks until the metadata server is available.
     */
    @Override
    public void sendSpans(List<byte[]> encodedSpans, Callback callback) {
        if (closeCalled) {
            throw new IllegalStateException("KafkaProducer has already been closed");
        }
        try {
            final byte[] message = getMessageEncoder().encode(encodedSpans);
            get().send(new ProducerRecord<>(getTopic(), message), (metadata, exception) -> {
                if (exception == null) {
                    callback.onComplete();
                } else {
                    callback.onError(exception);
                }
            });
        } catch (Throwable e) {
            callback.onError(e);
            // avoid rethrowing RuntimeException, but Errors are special in terms of impact determination
            if (e instanceof Error) {
                throw (Error) e;
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (closeCalled) {
            return;
        }
        closeCalled = true;
        super.close();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    @Nonnull
    public static Builder builder() {
        return new Builder()
            .encoding(Encoding.THRIFT)
            .topic(DEFAULT_TOPIC)
            .messageMaxBytes(1000000);
    }

    /**
     * Creates a Zipkin KafkaProducer with the given server set, using entirely default configuration properties.
     * @param bootstrapServers the bootstrap server list
     */
    @Nonnull
    public static KafkaSender create(@Nonnull String bootstrapServers) {
        return builder()
            .bootstrapServers(bootstrapServers)
            .build();
    }

    /**
     * TODO FIXME(dgarson): switch to using AutoValue generators once that is working for me in IntelliJ. There is not
     * really any functional difference, just maintainability / readability.
     */
    public static final class Builder {

        /** The properties to use for initialization of the Kafka Producer */
        private Properties properties = null;

        /** Topic zipkin spans will be send to. Defaults to "zipkin" */
        private String topic = DEFAULT_TOPIC;

        /** The encoding scheme to use for Kafka messages */
        private Encoding encoding;

        Builder(KafkaSender sender) {
            this.properties = sender.producerProperties;
            this.topic = sender.topic;
            this.encoding = sender.encoding;
        }

        Builder() {
        }

        /**
         * Specifies the configuration properties that will be used to initialize the Kafka Producer for this Sender.
         * @param properties the Kafka Producer properties to use
         */
        public Builder properties(@Nonnull Properties properties) {
            this.properties = checkNotNull(properties, "properties");
            return this;
        }

        @Nonnull
        public Properties getProperties() {
            if (properties == null) {
                properties = new Properties();
                properties.putAll(ZIPKIN_PRODUCER_DEFAULT_PROPERTIES);
            }
            return properties;
        }

        /**
         * Specifies the topic that Zipkin Spans/Traces will be sent to. Default topic is <tt>zipkin</tt>
         * @see #DEFAULT_TOPIC
         */
        public Builder topic(@Nonnull String topic) {
            this.topic = checkNotNull(topic, "topic");
            return this;
        }

        public String getTopic() {
            return topic;
        }


        /**
         * Initial set of kafka servers to connect to, rest of cluster will be discovered (comma
         * separated). No default
         *
         * @see org.apache.kafka.clients.producer.ProducerConfig#BOOTSTRAP_SERVERS_CONFIG
         */
        public final Builder bootstrapServers(String bootstrapServers) {
            getProperties().put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                checkNotNull(bootstrapServers, "bootstrapServers"));
            return this;
        }

        /**
         * Maximum size of a message. Must be equal to or less than the server's "message.max.bytes".
         * Default 1000000.
         */
        public final Builder messageMaxBytes(int messageMaxBytes) {
            getProperties().put(MAX_MESSAGE_BYTES_CONFIG, messageMaxBytes);
            return this;
        }

        /**
         * By default, a producer will be created, targeted to {@link #bootstrapServers(String)} with 0
         * required {@link ProducerConfig#ACKS_CONFIG acks}. Any properties set here will affect the
         * producer config.
         *
         * <p>For example: Reduce the timeout blocking on metadata from one minute to 5 seconds.
         * <pre>{@code
         * Map<String, String> overrides = new LinkedHashMap<>();
         * overrides.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, "5000");
         * builder.overrides(overrides);
         * }</pre>
         *
         * @see ProducerConfig
         */
        public final Builder overrides(Map<String, String> overrides) {
            getProperties().putAll(checkNotNull(overrides, "overrides"));
            return this;
        }

        public Builder encoding(Encoding encoding) {
            this.encoding = encoding;
            return this;
        }

        Encoding getEncoding() {
            return encoding;
        }

        public final KafkaSender build() {
            // ensure we have a client id if one is not explicitly provided
            if (!getProperties().containsKey(ProducerConfig.CLIENT_ID_CONFIG)) {
                getProperties().put(ProducerConfig.CLIENT_ID_CONFIG, checkNotNull(topic, "topic"));
            }
            KafkaSender sender = new KafkaSender(getTopic(), getProperties(), getEncoding());
            // any additional init logic goes here
            return sender;
        }

    }

    private static <T> T checkNotNull(T value, String label) {
        if (value == null) {
            throw new NullPointerException(label);
        }
        return value;
    }
}
