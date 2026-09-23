package com.example.commerce.platform.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Clock;

/**
 * Métricas y limpieza del outbox.
 * <ul>
 *     <li>Gauge {@code outbox.events{status}} en Actuator: un número creciente de PENDING indica que
 *     Kafka no está disponible; cualquier FAILED requiere atención.</li>
 *     <li>Los eventos PUBLISHED se conservan para auditoría durante {@code commerce.outbox.retention}.</li>
 * </ul>
 */
public class OutboxMaintenance implements MeterBinder {

    private static final Logger log = LoggerFactory.getLogger(OutboxMaintenance.class);

    private final OutboxStore store;
    private final OutboxProperties properties;
    private final Clock clock;

    public OutboxMaintenance(OutboxStore store, OutboxProperties properties, Clock clock) {
        this.store = store;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        for (OutboxStatus status : OutboxStatus.values()) {
            Gauge.builder("outbox.events", store, s -> s.countByStatus(status))
                    .description("Events in the transactional outbox by status")
                    .tag("status", status.name())
                    .register(registry);
        }
    }

    @Scheduled(cron = "${commerce.outbox.cleanup-cron:0 0 3 * * *}")
    public void deleteExpiredPublishedEvents() {
        int deleted = store.deletePublishedBefore(clock.instant().minus(properties.retention()));
        if (deleted > 0) {
            log.info("Outbox cleanup removed {} published events older than {}", deleted, properties.retention());
        }
    }
}
