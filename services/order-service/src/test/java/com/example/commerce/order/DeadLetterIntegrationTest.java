package com.example.commerce.order;

import com.example.commerce.events.Topics;
import com.example.commerce.events.payload.InventoryReserved;
import com.example.commerce.events.payload.StockLine;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Mensajes que no se pueden procesar terminan en {@code <topic>.DLT} con cabeceras de diagnóstico.
 */
class DeadLetterIntegrationTest extends OrderServiceIntegrationTest {

    @Test
    void unreadableMessageGoesToTheDeadLetterTopic() {
        String key = "garbage-" + UUID.randomUUID();

        events.sendRaw(Topics.INVENTORY_RESERVED, key, "{ this is not json");

        ConsumerRecord<String, String> dead = topics.awaitRecord(Topics.deadLetterOf(Topics.INVENTORY_RESERVED), key);
        assertThat(dead.value()).isEqualTo("{ this is not json");
        assertThat(header(dead, "kafka_dlt-exception-cause-fqcn")).endsWith("InvalidEventException");
        assertThat(header(dead, "kafka_dlt-original-topic")).isEqualTo(Topics.INVENTORY_RESERVED);
    }

    @Test
    void unsupportedSchemaVersionGoesToTheDeadLetterTopic() {
        long orderId = uniqueId();
        var event = events.envelope(new InventoryReserved(orderId, 1L, List.of(new StockLine(1L, 1))), orderId, "cid");
        String v2 = events.toJson(event).replace("\"eventVersion\":1", "\"eventVersion\":2");

        events.sendRaw(Topics.INVENTORY_RESERVED, String.valueOf(orderId), v2);

        ConsumerRecord<String, String> dead = topics.awaitRecord(Topics.deadLetterOf(Topics.INVENTORY_RESERVED), orderId);
        assertThat(header(dead, "kafka_dlt-exception-message")).contains("Unsupported version 2");
    }

    @Test
    void eventForUnknownOrderIsAPermanentErrorAndGoesToTheDeadLetterTopic() {
        long unknownOrder = uniqueId();

        events.publish(new InventoryReserved(unknownOrder, 1L, List.of(new StockLine(1L, 1))), unknownOrder);

        ConsumerRecord<String, String> dead = topics.awaitRecords(Topics.deadLetterOf(Topics.INVENTORY_RESERVED),
                record -> String.valueOf(unknownOrder).equals(record.key()), 1, Duration.ofSeconds(20)).getFirst();
        assertThat(header(dead, "kafka_dlt-exception-message")).contains("Unknown order " + unknownOrder);
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
