/**
 * DataCleaner (community edition)
 * Copyright (C) 2013 Human Inference
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.datacleaner.extension.kafka;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Named;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.datacleaner.api.Analyzer;
import org.datacleaner.api.Categorized;
import org.datacleaner.api.Close;
import org.datacleaner.api.Configured;
import org.datacleaner.api.Description;
import org.datacleaner.api.ExternalDocumentation;
import org.datacleaner.api.Initialize;
import org.datacleaner.api.InputColumn;
import org.datacleaner.api.InputRow;
import org.datacleaner.api.MappedProperty;
import org.datacleaner.api.StringProperty;
import org.datacleaner.api.ExternalDocumentation.DocumentationLink;
import org.datacleaner.api.ExternalDocumentation.DocumentationType;
import org.datacleaner.beans.writers.WriteDataResult;
import org.datacleaner.beans.writers.WriteDataResultImpl;
import org.datacleaner.components.categories.WriteSuperCategory;
import org.datacleaner.util.StringUtils;

import com.google.common.base.Strings;

@Named("Send message to Kafka topic")
@Categorized(superCategory = WriteSuperCategory.class)
@Description("Sends messages to an Apache Kafka topic.\n"
        + "Each record in the stream will be transformed into a message using the supplied template and values. You can enter a template string for both key and value of the message. In those templates references to variable names from your data stream will be search/replaced to make up the individual messages.")
@ExternalDocumentation(@DocumentationLink(title = "Apache Kafka", type = DocumentationType.REFERENCE, url = "http://kafka.apache.org/", version = "0.9"))
public class KafkaProducerAnalyzer implements Analyzer<WriteDataResult> {

    public static final String PROPERTY_VARIABLE_COLUMNS = "Template variable columns";
    public static final String PROPERTY_VARIABLE_NAMES = "Template variable names";
    public static final String PROPERTY_TOPIC = "Topic";
    public static final String PROPERTY_CONNECTION_PROPERTIES = "Connection properties";
    public static final String PROPERTY_VALUE_TEMPLATE = "Value (template)";
    public static final String PROPERTY_KEY_TEMPLATE = "Key (template)";

    @Configured(order = 1, value = PROPERTY_VARIABLE_COLUMNS)
    @Description("Values of the record which will be replaced into the message templates.")
    InputColumn<?>[] variableColumns;

    @Configured(order = 2, value = PROPERTY_VARIABLE_NAMES)
    @MappedProperty(PROPERTY_VARIABLE_COLUMNS)
    @Description("Names of the record values as they will be searched for within the message templates.")
    String[] variableNames;

    @Configured(order = 10, value = PROPERTY_TOPIC)
    @Description("Name of the 'topic' to publish messages to.")
    String topic;

    @StringProperty(emptyString = true)
    @Configured(order = 11, value = PROPERTY_KEY_TEMPLATE)
    @Description("Template text to be used for the message key (optional).")
    String keyTemplate;

    @StringProperty(multiline = true)
    @Configured(order = 12, value = PROPERTY_VALUE_TEMPLATE)
    @Description("Template text to be used for the message value.")
    String valueTemplate;

    @Configured(order = 20, value = PROPERTY_CONNECTION_PROPERTIES)
    @Description("Properties used to set up the connection to the Kafka broker(s). See the Apache Kafka documentation for details.")
    Map<String, String> connectionProperties = createDefaultConnectionProperties();

    private KafkaProducer<String, String> _producer;
    private AtomicInteger _messageCounter;

    private static Map<String, String> createDefaultConnectionProperties() {
        final Map<String, String> props = new LinkedHashMap<String, String>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");
        return props;
    }

    @Initialize
    public void init() {
        final Properties props = new Properties();
        props.putAll(connectionProperties);
        _producer = new KafkaProducer<>(props);
        _messageCounter = new AtomicInteger();
    }

    @Close
    public void close() {
        if (_producer != null) {
            _producer.close();
            _producer = null;
        }
    }

    @Override
    public void run(InputRow row, int distinctCount) {
        final String key = createValueFromTemplate(keyTemplate, row);
        final String value = createValueFromTemplate(valueTemplate, row);
        for (int i = 0; i < distinctCount; i++) {
            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            _producer.send(record);
            _messageCounter.incrementAndGet();
        }
    }

    private String createValueFromTemplate(String template, InputRow row) {
        if (Strings.isNullOrEmpty(template)) {
            return template;
        }
        for (int i = 0; i < variableNames.length; i++) {
            final String varName = variableNames[i];
            final InputColumn<?> varColumn = variableColumns[i];
            final Object varValue = row.getValue(varColumn);
            final String replacementValue = varValue == null ? "" : varValue.toString();
            template = StringUtils.replaceAll(template, varName, replacementValue);
        }
        return template;
    }

    @Override
    public WriteDataResult getResult() {
        return new WriteDataResultImpl(_messageCounter.get(), 0, 0);
    }

}
