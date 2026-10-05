package com.banking.transactionservice.service;

import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventConsumer {

    private final TransactionRepository transactionRepository;

    private final TransactionService transactionService;

    @Qualifier("otpRedisTemplate")
    private final RedisTemplate<String, String> redisTemplate;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final SecureRandom secureRandom = new SecureRandom();

    private static final long OTP_EXPIRY_MINUTES = 5;

    private static final String TRANSACTION_OTP_GENERATED_TOPIC =
            "transaction.otp.generated";


    // =========================================================
    // verification.required → Generate OTP
    // =========================================================

    @KafkaListener(
            topics = "verification.required",
            groupId = "transaction-service-verification-group"
    )
    public void consumeVerificationRequired(
            @Payload Map<String, Object> payload
    ) {

        String transactionId = (String) payload.get("transactionId");

        log.info("──────────────────────────────────────────────");
        log.info("[OTP-GEN] Kafka event received: verification.required");
        log.info("[OTP-GEN] transactionId={}", transactionId);
        log.info("[OTP-GEN] reason={}", payload.get("reason"));

        try {

            String accountNumber = (String) payload.get("accountNumber");
            String reason        = (String) payload.get("reason");

            Transaction transaction =
                    transactionRepository
                            .findByTransactionId(transactionId)
                            .orElseThrow(() ->
                                    new RuntimeException("Transaction not found: " + transactionId));

            log.info("[OTP-GEN] Transaction found. currentStatus={}", transaction.getStatus());

            // Status guard — must be PROCESSING to generate OTP
            if (transaction.getStatus() != TransactionStatus.PROCESSING) {
                log.warn("[OTP-GEN] SKIPPED — Expected status=PROCESSING but got status={}. transactionId={}",
                        transaction.getStatus(), transactionId);
                log.info("──────────────────────────────────────────────");
                return;
            }

            // Generate OTP
            String otp = String.format("%06d", secureRandom.nextInt(900000) + 100000);
            String otpKey = "verification:otp:" + transactionId;

            log.info("[OTP-GEN] Generated OTP for transactionId={}. Storing in Redis with key={}",
                    transactionId, otpKey);

            redisTemplate.opsForValue().set(otpKey, otp, Duration.ofMinutes(OTP_EXPIRY_MINUTES));

            log.info("[OTP-GEN] OTP stored in Redis. Expires in {} minutes.", OTP_EXPIRY_MINUTES);

            // Update status
            transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepository.save(transaction);

            log.info("[OTP-GEN] Transaction status updated to PENDING_VERIFICATION. transactionId={}",
                    transactionId);

            // Publish OTP event (for notification-service to send via email/SMS)
            Map<String, Object> otpEvent = new HashMap<>();
            otpEvent.put("transactionId", transactionId);
            otpEvent.put("accountNumber", accountNumber);
            otpEvent.put("reason", reason);
            otpEvent.put("otp", otp);
            otpEvent.put("amount", payload.get("amount"));

            kafkaTemplate.send(TRANSACTION_OTP_GENERATED_TOPIC, transactionId, otpEvent);

            log.info("[OTP-GEN] Published to Kafka topic: {}. transactionId={}",
                    TRANSACTION_OTP_GENERATED_TOPIC, transactionId);
            log.info("[OTP-GEN] DONE — OTP={} (waiting for user to call verify-otp endpoint)", otp);

        } catch (Exception e) {
            log.error("[OTP-GEN] ERROR processing verification.required. transactionId={}", transactionId, e);
            throw e;
        }

        log.info("──────────────────────────────────────────────");
    }


    // =========================================================
    // fraud.check.clean → Complete transaction directly
    // =========================================================

    @KafkaListener(
            topics = "fraud.check.clean",
            groupId = "transaction-service-clean-group"
    )
    public void consumeFraudCheckCleanResult(
            @Payload Map<String, Object> payload
    ) {

        String transactionId = (String) payload.get("transactionId");

        log.info("──────────────────────────────────────────────");
        log.info("[FRAUD-CLEAN] Kafka event received: fraud.check.clean");
        log.info("[FRAUD-CLEAN] transactionId={}", transactionId);

        try {

            if (transactionId == null || transactionId.isBlank()) {
                throw new IllegalArgumentException(
                        "transactionId is missing from fraud.check.clean event");
            }

            log.info("[FRAUD-CLEAN] Transaction passed fraud checks — completing directly (no OTP needed).");

            transactionService.processCleanResult(transactionId);

            log.info("[FRAUD-CLEAN] processCleanResult completed. transactionId={}", transactionId);

        } catch (Exception e) {
            log.error("[FRAUD-CLEAN] ERROR processing fraud.check.clean. transactionId={}", transactionId, e);
            throw e;
        }

        log.info("──────────────────────────────────────────────");
    }
}
