package com.banking.transactionservice.service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;

    private final AccountServiceClient accountServiceClient;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /*
     * Must use the SAME template as TransactionEventConsumer uses to WRITE
     * the OTP. Both must use otpRedisTemplate (String/String serializers)
     * so the key lookup is consistent.
     */
    @Qualifier("otpRedisTemplate")
    private final RedisTemplate<String, String> redisTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC =
            "transaction.initiated";

    private static final String TRANSACTION_COMPLETED_TOPIC =
            "transaction.completed";

    private static final String TRANSACTION_REFUNDED_TOPIC =
            "transaction.refunded";

    private static final String FRAUD_DETECTED_TOPIC =
            "fraud.detected";


    // =========================================================
    // TRANSFER
    // =========================================================

    @Transactional
    public TransactionResponse transfer(TransferRequest request) {

        log.info("──────────────────────────────────────────────");
        log.info("[TRANSFER] STEP 1/4 — Request received");
        log.info("[TRANSFER] sender={}, receiver={}, amount={}",
                request.getSenderAccountNumber(),
                request.getReceiverAccountNumber(),
                request.getAmount());

        // 1. Deduct money from sender
        log.info("[TRANSFER] STEP 2/4 — Debiting sender account: {}",
                request.getSenderAccountNumber());

        accountServiceClient.deductBalance(
                request.getSenderAccountNumber(),
                request.getAmount()
        );

        log.info("[TRANSFER] STEP 2/4 — Sender debited successfully. amount={}",
                request.getAmount());

        // 2. Create transaction
        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(request.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(request.getReceiverAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setTransactionType(TransactionType.TRANSFER);
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setDescription(request.getDescription());
        transaction.setTransactionId(UUID.randomUUID().toString());
        transaction.setCreatedAt(LocalDateTime.now());

        // 3. Save transaction
        log.info("[TRANSFER] STEP 3/4 — Saving transaction to DB with status=PROCESSING");

        Transaction savedTransaction = transactionRepository.save(transaction);

        log.info("[TRANSFER] STEP 3/4 — Transaction saved. transactionId={}, dbId={}, status={}",
                savedTransaction.getTransactionId(),
                savedTransaction.getId(),
                savedTransaction.getStatus());

        // 4. Publish transaction.initiated
        log.info("[TRANSFER] STEP 4/4 — Publishing to Kafka topic: {}",
                TRANSACTION_INITIATED_TOPIC);

        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                savedTransaction.getTransactionId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()
        );

        kafkaTemplate.send(
                TRANSACTION_INITIATED_TOPIC,
                savedTransaction.getTransactionId(),
                event
        );

        log.info("[TRANSFER] STEP 4/4 — Published transaction.initiated. transactionId={}",
                savedTransaction.getTransactionId());
        log.info("[TRANSFER] DONE — Waiting for fraud-detection-service to pick up the event.");
        log.info("──────────────────────────────────────────────");

        return mapToResponse(savedTransaction);
    }


    // =========================================================
    // VERIFY OTP
    // =========================================================

    @Transactional
    public TransactionResponse verifyOTP(
            String txnId,
            String otp
    ) {

        log.info("──────────────────────────────────────────────");
        log.info("[VERIFY-OTP] STEP 1/3 — Request received. transactionId={}", txnId);

        Transaction transaction =
                transactionRepository
                        .findByTransactionId(txnId)
                        .orElseThrow(() ->
                                new RuntimeException("Transaction not found: " + txnId));

        log.info("[VERIFY-OTP] STEP 1/3 — Transaction found. status={}", transaction.getStatus());

        String otpKey = "verification:otp:" + txnId;

        // Guard: only PENDING_VERIFICATION transactions can be verified
        if (transaction.getStatus() != TransactionStatus.PENDING_VERIFICATION) {
            log.warn("[VERIFY-OTP] REJECTED — Expected status=PENDING_VERIFICATION but got status={}. transactionId={}",
                    transaction.getStatus(), txnId);
            log.info("──────────────────────────────────────────────");
            return mapToResponse(transaction);
        }

        log.info("[VERIFY-OTP] STEP 2/3 — Reading OTP from Redis. key={}", otpKey);

        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if (storedOtp == null) {
            log.warn("[VERIFY-OTP] STEP 2/3 — OTP NOT FOUND in Redis (expired or never written). key={}",
                    otpKey);
            log.warn("[VERIFY-OTP] Initiating compensation (refund) for transactionId={}", txnId);

            compensateTransaction(
                    transaction,
                    "OTP expired - transaction cancelled and amount refunded"
            );

            log.info("──────────────────────────────────────────────");
            return mapToResponse(transaction);
        }

        log.info("[VERIFY-OTP] STEP 2/3 — OTP found in Redis. Comparing with provided OTP.");

        if (!storedOtp.equals(otp)) {
            log.warn("[VERIFY-OTP] STEP 2/3 — OTP MISMATCH. transactionId={}", txnId);
            log.warn("[VERIFY-OTP] Initiating block + compensation for transactionId={}", txnId);

            redisTemplate.delete(otpKey);

            blockAccountAndCompensate(
                    transaction,
                    "Wrong OTP entered - transaction cancelled and account blocked for security"
            );

            log.info("──────────────────────────────────────────────");
            return mapToResponse(transaction);
        }

        log.info("[VERIFY-OTP] STEP 2/3 — OTP matched successfully. transactionId={}", txnId);
        log.info("[VERIFY-OTP] STEP 3/3 — Deleting OTP from Redis and completing transaction.");

        redisTemplate.delete(otpKey);

        completeTransaction(transaction);

        log.info("[VERIFY-OTP] DONE — Transaction completed. transactionId={}", txnId);
        log.info("──────────────────────────────────────────────");

        return mapToResponse(transaction);
    }


    // =========================================================
    // GET TRANSACTION
    // =========================================================

    @Transactional(readOnly = true)
    public TransactionResponse getTransaction(String txnId) {
        Transaction transaction =
                transactionRepository
                        .findByTransactionId(txnId)
                        .orElseThrow(() ->
                                new RuntimeException("Transaction not found: " + txnId));
        return mapToResponse(transaction);
    }


    // =========================================================
    // GET HISTORY
    // =========================================================

    @Transactional(readOnly = true)
    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        return transactionRepository
                .findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                .stream()
                .map(this::mapToResponse)
                .toList();
    }


    // =========================================================
    // PROCESS CLEAN FRAUD RESULT
    // =========================================================

    public void processCleanResult(String transactionId) {

        log.info("──────────────────────────────────────────────");
        log.info("[CLEAN-RESULT] Received clean fraud result. transactionId={}", transactionId);

        Transaction transaction =
                transactionRepository
                        .findByTransactionId(transactionId)
                        .orElseThrow(() ->
                                new RuntimeException("Transaction not found: " + transactionId));

        log.info("[CLEAN-RESULT] Transaction current status={}", transaction.getStatus());

        // Guard against double-complete on Kafka redelivery
        if (transaction.getStatus() != TransactionStatus.PROCESSING) {
            log.warn("[CLEAN-RESULT] SKIPPED — Expected status=PROCESSING but got status={}. transactionId={}",
                    transaction.getStatus(), transactionId);
            log.info("──────────────────────────────────────────────");
            return;
        }

        log.info("[CLEAN-RESULT] Transaction is PROCESSING — completing directly (no OTP needed).");

        completeTransaction(transaction);

        log.info("──────────────────────────────────────────────");
    }


    // =========================================================
    // COMPLETE TRANSACTION (internal)
    // =========================================================

    private void completeTransaction(Transaction transaction) {

        log.info("[COMPLETE] Setting status=COMPLETED for transactionId={}",
                transaction.getTransactionId());

        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transaction.setReferenceNumber(
                "TXN-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase()
        );

        transactionRepository.save(transaction);

        log.info("[COMPLETE] Saved. transactionId={}, referenceNumber={}",
                transaction.getTransactionId(),
                transaction.getReferenceNumber());

        TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(
                transaction.getTransactionId(),
                transaction.getSenderAccountNumber(),
                transaction.getReceiverAccountNumber(),
                transaction.getAmount(),
                transaction.getDescription()
        );

        kafkaTemplate.send(
                TRANSACTION_COMPLETED_TOPIC,
                transaction.getTransactionId(),
                completedEvent
        );

        log.info("[COMPLETE] Published to Kafka topic: {}. transactionId={}. Waiting for account-service to credit receiver={}",
                TRANSACTION_COMPLETED_TOPIC,
                transaction.getTransactionId(),
                transaction.getReceiverAccountNumber());
    }


    // =========================================================
    // COMPENSATE (internal)
    // =========================================================

    private void compensateTransaction(
            Transaction transaction,
            String message
    ) {

        log.info("[COMPENSATE] Starting compensation for transactionId={}, currentStatus={}",
                transaction.getTransactionId(), transaction.getStatus());

        if (transaction.getStatus() != TransactionStatus.PENDING
                && transaction.getStatus() != TransactionStatus.PROCESSING
                && transaction.getStatus() != TransactionStatus.PENDING_VERIFICATION) {

            log.warn("[COMPENSATE] SKIPPED — status={} not eligible for refund. transactionId={}",
                    transaction.getStatus(), transaction.getTransactionId());
            return;
        }

        log.info("[COMPENSATE] Refunding sender account: {}, amount: {}",
                transaction.getSenderAccountNumber(), transaction.getAmount());

        try {
            accountServiceClient.creditBalance(
                    transaction.getSenderAccountNumber(),
                    transaction.getAmount()
            );
        } catch (Exception e) {
            log.error("[COMPENSATE] *** REFUND FAILED *** transactionId={}, sender={} — MANUAL INTERVENTION REQUIRED",
                    transaction.getTransactionId(),
                    transaction.getSenderAccountNumber(), e);

            transaction.setStatus(TransactionStatus.FAILED);
            transaction.setFailureReason(message + " | refund failed, manual review required");
            transactionRepository.save(transaction);
            return;
        }

        log.info("[COMPENSATE] Sender refunded successfully. transactionId={}",
                transaction.getTransactionId());

        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(
                message + " - SAGA compensation executed. Amount refunded at " + LocalDateTime.now()
        );
        transactionRepository.save(transaction);

        Map<String, Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getTransactionId());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("reason", message);

        kafkaTemplate.send(
                TRANSACTION_REFUNDED_TOPIC,
                transaction.getTransactionId(),
                refundEvent
        );

        log.info("[COMPENSATE] Published transaction.refunded. transactionId={}, finalStatus=FLAGGED",
                transaction.getTransactionId());
    }


    // =========================================================
    // BLOCK + COMPENSATE (internal)
    // =========================================================

    private void blockAccountAndCompensate(
            Transaction transaction,
            String message
    ) {

        log.info("[BLOCK+COMPENSATE] Starting for transactionId={}", transaction.getTransactionId());

        /*
         * IMPORTANT: Refund sender FIRST via synchronous REST call,
         * THEN publish fraud.detected to block the account via Kafka.
         *
         * If we published fraud.detected first, the Kafka consumer in
         * account-service could block the sender before our creditBalance
         * call arrives, causing "Account is not active" and a failed refund.
         */
        log.info("[BLOCK+COMPENSATE] STEP 1/2 — Refunding sender before publishing block event.");

        compensateTransaction(transaction, message);

        log.info("[BLOCK+COMPENSATE] STEP 2/2 — Publishing fraud.detected to block sender account: {}",
                transaction.getSenderAccountNumber());

        Map<String, Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId", transaction.getTransactionId());
        fraudEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        fraudEvent.put("reason", message);

        kafkaTemplate.send(
                FRAUD_DETECTED_TOPIC,
                transaction.getSenderAccountNumber(),
                fraudEvent
        );

        log.info("[BLOCK+COMPENSATE] DONE — fraud.detected published. transactionId={}",
                transaction.getTransactionId());
    }


    // =========================================================
    // MAP TO RESPONSE
    // =========================================================

    private TransactionResponse mapToResponse(Transaction transaction) {
        return TransactionResponse.builder()
                .id(transaction.getId())
                .transactionId(transaction.getTransactionId())
                .senderAccountNumber(transaction.getSenderAccountNumber())
                .receiverAccountNumber(transaction.getReceiverAccountNumber())
                .amount(transaction.getAmount())
                .transactionType(transaction.getTransactionType())
                .status(transaction.getStatus())
                .description(transaction.getDescription())
                .failureReason(transaction.getFailureReason())
                .referenceNumber(transaction.getReferenceNumber())
                .createdAt(transaction.getCreatedAt())
                .completedAt(transaction.getCompletedAt())
                .build();
    }
}
