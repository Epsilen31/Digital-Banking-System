package com.banking.accountservice.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Tracks Kafka events that have already been processed.
 * Prevents double-crediting/double-blocking when Kafka
 * replays messages (e.g. on service restart with earliest offset,
 * or on consumer rebalance).
 *
 * Acts as an idempotency key store at the DB level.
 */
@Entity
@Table(name = "processed_events")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedEvent {

    @Id
    @Column(nullable = false, unique = true)
    private String eventId;      // transactionId + ":" + eventType

    @Column(nullable = false)
    private LocalDateTime processedAt;
}
