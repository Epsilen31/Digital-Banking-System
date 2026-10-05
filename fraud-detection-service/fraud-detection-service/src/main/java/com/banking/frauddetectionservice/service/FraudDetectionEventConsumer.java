package com.banking.frauddetectionservice.service;

import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionEventConsumer {

    private final FraudDetectionService fraudDetectionService;


    @KafkaListener(
            topics = "transaction.initiated",
            groupId = "fraud-detection-group"
    )
    public void consumeTransactionInitiated(
            @Payload Map<String, Object> payload
    ) {

        String transactionId = (String) payload.get("transactionId");

        log.info("──────────────────────────────────────────────");
        log.info("[FRAUD-CHECK] Kafka event received: transaction.initiated");
        log.info("[FRAUD-CHECK] transactionId={}", transactionId);
        log.info("[FRAUD-CHECK] sender={}, amount={}",
                payload.get("senderAccountNumber"),
                payload.get("amount"));

        try {

            fraudDetectionService.checkTransaction(payload);

            log.info("[FRAUD-CHECK] checkTransaction() completed for transactionId={}", transactionId);

        } catch (Exception e) {
            log.error("[FRAUD-CHECK] ERROR running fraud check for transactionId={}", transactionId, e);
            throw e;
        }

        log.info("──────────────────────────────────────────────");
    }
}
