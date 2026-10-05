package com.banking.frauddetectionservice.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.frauddetectionservice.model.FraudCheckResult;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {

    private static final String VERIFICATION_REQUIRED_TOPIC  = "verification.required";
    private static final String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";
    private static final Duration AVG_AMOUNT_TTL = Duration.ofDays(30);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${fraud.max-transactions-per-minute}")
    private int maxTransactionPerMinute;

    @Value("${fraud.suspicious-amount-multiplier}")
    private BigDecimal suspiciousAmountMultiplier;


    public void checkTransaction(Map<String, Object> payload) {

        String transactionId = (String) payload.get("transactionId");
        String accountNumber = (String) payload.get("senderAccountNumber");
        Object amountObject  = payload.get("amount");

        if (transactionId == null || transactionId.isBlank()) {
            throw new IllegalArgumentException("transactionId is missing from fraud check payload");
        }

        if (accountNumber == null || accountNumber.isBlank()) {
            throw new IllegalArgumentException("senderAccountNumber is missing from fraud check payload");
        }

        if (amountObject == null) {
            throw new IllegalArgumentException("amount is missing from fraud check payload");
        }

        BigDecimal amount;
        try {
            amount = new BigDecimal(amountObject.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid transaction amount: " + amountObject, e);
        }

        log.info("[FRAUD] Starting checks. transactionId={}, account={}, amount={}",
                transactionId, accountNumber, amount);

        FraudCheckResult result = performFraudChecks(accountNumber, amount);

        // --------------------------------------------------
        // SUSPICIOUS — require OTP verification
        // --------------------------------------------------

        if (result.isFraud()) {

            log.warn("[FRAUD] *** SUSPICIOUS *** transactionId={}, reason={}",
                    transactionId, result.getReason());
            log.warn("[FRAUD] Publishing to Kafka topic: {}", VERIFICATION_REQUIRED_TOPIC);

            Map<String, Object> verificationEvent = new HashMap<>();
            verificationEvent.put("transactionId", transactionId);
            verificationEvent.put("accountNumber", accountNumber);
            verificationEvent.put("amount", amount);
            verificationEvent.put("reason", result.getReason());

            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, verificationEvent);

            log.warn("[FRAUD] Published verification.required. transactionId={}. OTP will be sent to user.",
                    transactionId);
            return;
        }

        // --------------------------------------------------
        // CLEAN — complete directly
        // --------------------------------------------------

        log.info("[FRAUD] *** CLEAN *** transactionId={} passed all fraud checks.", transactionId);
        log.info("[FRAUD] Publishing to Kafka topic: {}", FRAUD_CHECK_CLEAN_RESULT_TOPIC);

        Map<String, Object> cleanEvent = new HashMap<>();
        cleanEvent.put("transactionId", transactionId);
        cleanEvent.put("isFraud", false);
        cleanEvent.put("reason", null);

        kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC, transactionId, cleanEvent);

        log.info("[FRAUD] Published fraud.check.clean. transactionId={}", transactionId);
    }


    // =========================================================
    // FRAUD CHECKS
    // =========================================================

    private FraudCheckResult performFraudChecks(String accountNumber, BigDecimal amount) {

        log.info("[FRAUD] Running checks for account={}, amount={}", accountNumber, amount);

        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            return new FraudCheckResult(true, "Transaction amount must be greater than zero");
        }

        if (isVelocityExceeded(accountNumber)) {
            return new FraudCheckResult(
                    true,
                    "Too many transactions in 60 seconds - velocity limit exceeded"
            );
        }

        if (isAmountSuspicious(accountNumber, amount)) {
            return new FraudCheckResult(
                    true,
                    "Unusual transaction amount - exceeds " + suspiciousAmountMultiplier + "x your average"
            );
        }

        log.info("[FRAUD] All checks PASSED for account={}", accountNumber);

        return new FraudCheckResult(false, null);
    }


    // =========================================================
    // VELOCITY CHECK
    // =========================================================

    private boolean isVelocityExceeded(String accountNumber) {

        String key   = "fraud-velocity:" + accountNumber;
        Long   count = redisTemplate.opsForValue().increment(key);

        if (count != null && count == 1) {
            redisTemplate.expire(key, 60, TimeUnit.SECONDS);
        }

        boolean exceeded = count != null && count >= maxTransactionPerMinute;

        log.info("[FRAUD][VELOCITY] account={}, count={}, limit={}, exceeded={}",
                accountNumber, count, maxTransactionPerMinute, exceeded);

        if (exceeded) {
            log.warn("[FRAUD][VELOCITY] *** EXCEEDED *** account={} has {} transactions in the last 60s (limit={})",
                    accountNumber, count, maxTransactionPerMinute);
        }

        return exceeded;
    }


    // =========================================================
    // AMOUNT CHECK
    // =========================================================

    private boolean isAmountSuspicious(String accountNumber, BigDecimal amount) {

        String key    = "fraud-avg-amount:" + accountNumber;
        String avgStr = redisTemplate.opsForValue().get(key);

        if (avgStr == null) {
            redisTemplate.opsForValue().set(key, amount.toString(), AVG_AMOUNT_TTL);

            log.info("[FRAUD][AMOUNT] First transaction for account={}. Average initialized to {}. NOT suspicious.",
                    accountNumber, amount);

            return false;
        }

        BigDecimal averageAmount = new BigDecimal(avgStr);
        BigDecimal threshold     = averageAmount.multiply(suspiciousAmountMultiplier);
        boolean    suspicious    = amount.compareTo(threshold) > 0;

        log.info("[FRAUD][AMOUNT] account={}, amount={}, storedAverage={}, threshold={}x{}={}, suspicious={}",
                accountNumber, amount, averageAmount, suspiciousAmountMultiplier, averageAmount, threshold, suspicious);

        if (suspicious) {
            log.warn("[FRAUD][AMOUNT] *** SUSPICIOUS *** account={}: amount {} exceeds threshold {}",
                    accountNumber, amount, threshold);
        }

        if (!suspicious) {
            BigDecimal newAverage = averageAmount
                    .add(amount)
                    .divide(BigDecimal.valueOf(2), 2, RoundingMode.HALF_UP);

            redisTemplate.opsForValue().set(key, newAverage.toString(), AVG_AMOUNT_TTL);

            log.info("[FRAUD][AMOUNT] Average updated for account={}: {} → {}",
                    accountNumber, averageAmount, newAverage);
        }

        return suspicious;
    }
}
