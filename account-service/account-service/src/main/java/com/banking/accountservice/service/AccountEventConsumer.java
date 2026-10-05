package com.banking.accountservice.service;

import com.banking.accountservice.entity.ProcessedEvent;
import com.banking.accountservice.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {

    private final AccountService accountService;
    private final ProcessedEventRepository processedEventRepository;


    // =========================================================
    // transaction.completed → Credit receiver
    // =========================================================

    @KafkaListener(
            topics = "transaction.completed",
            groupId = "account-service-group"
    )
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload
    ) {

        log.info("──────────────────────────────────────────────");
        log.info("[CREDIT] Kafka event received: transaction.completed");
        log.info("[CREDIT] payload={}", payload);

        try {

            String transactionId         = (String) payload.get("transactionId");
            String receiverAccountNumber = (String) payload.get("receiverAccountNumber");
            Object amountObject          = payload.get("amount");

            // Validate
            if (transactionId == null || transactionId.isBlank()) {
                throw new IllegalArgumentException(
                        "transactionId is missing from transaction.completed event");
            }

            if (receiverAccountNumber == null || receiverAccountNumber.isBlank()) {
                throw new IllegalArgumentException(
                        "receiverAccountNumber is missing from transaction.completed event");
            }

            if (amountObject == null) {
                throw new IllegalArgumentException(
                        "amount is missing from transaction.completed event");
            }

            // Idempotency check — skip if already processed
            String eventId = transactionId + ":transaction.completed";

            if (processedEventRepository.existsByEventId(eventId)) {
                log.warn("[CREDIT] DUPLICATE — transactionId={} already processed. Skipping to prevent double-credit.",
                        transactionId);
                log.info("──────────────────────────────────────────────");
                return;
            }

            BigDecimal amount;
            try {
                amount = new BigDecimal(amountObject.toString());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid transaction amount: " + amountObject, e);
            }

            log.info("[CREDIT] Crediting receiver account={}, amount={}, transactionId={}",
                    receiverAccountNumber, amount, transactionId);

            accountService.creditBalance(receiverAccountNumber, amount);

            // Mark as processed AFTER successful credit
            processedEventRepository.save(new ProcessedEvent(eventId, LocalDateTime.now()));

            log.info("[CREDIT] *** SUCCESS *** Receiver account={} credited with {}. transactionId={}",
                    receiverAccountNumber, amount, transactionId);

        } catch (Exception e) {
            log.error("[CREDIT] *** FAILED *** Could not credit receiver. payload={}", payload, e);
            throw e;
        }

        log.info("──────────────────────────────────────────────");
    }


    // =========================================================
    // fraud.detected → Block sender account
    // =========================================================

    @KafkaListener(
            topics = "fraud.detected",
            groupId = "account-service-fraud-group"
    )
    public void consumeFraudDetected(
            @Payload Map<String, Object> payload
    ) {

        log.info("──────────────────────────────────────────────");
        log.info("[BLOCK] Kafka event received: fraud.detected");
        log.info("[BLOCK] payload={}", payload);

        try {

            String transactionId       = (String) payload.get("transactionId");
            String senderAccountNumber = (String) payload.get("senderAccountNumber");
            String reason              = (String) payload.get("reason");

            log.warn("[BLOCK] Fraud event. transactionId={}, senderAccount={}, reason={}",
                    transactionId, senderAccountNumber, reason);

            if (senderAccountNumber == null || senderAccountNumber.isBlank()) {
                throw new IllegalArgumentException(
                        "senderAccountNumber is missing from fraud.detected event");
            }

            // Idempotency check
            String eventId = transactionId + ":fraud.detected";

            if (processedEventRepository.existsByEventId(eventId)) {
                log.warn("[BLOCK] DUPLICATE — transactionId={} already processed. Skipping.",
                        transactionId);
                log.info("──────────────────────────────────────────────");
                return;
            }

            log.warn("[BLOCK] Blocking sender account: {}", senderAccountNumber);

            accountService.blockAccount(senderAccountNumber);

            processedEventRepository.save(new ProcessedEvent(eventId, LocalDateTime.now()));

            log.warn("[BLOCK] *** BLOCKED *** Sender account={} blocked. transactionId={}",
                    senderAccountNumber, transactionId);

        } catch (Exception e) {
            log.error("[BLOCK] *** FAILED *** Could not block sender account. payload={}", payload, e);
            throw e;
        }

        log.info("──────────────────────────────────────────────");
    }
}
