package com.banking.notificationservice.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@AllArgsConstructor
public class NotificationService {

    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOtpGenerated(
            @Payload Map<String, Object> payload) {

        try {
            String accountNumber = (String) payload.get("accountNumber");
            String transactionId = (String) payload.get("transactionId");
            String otp = (String) payload.get("otp");
            String reason = (String) payload.get("reason");

            sendAlert(
                    accountNumber,
                    "TRANSACTION_VERIFICATION_REQUIRED",
                    String.format(
                            "Suspicious activity was detected on your account. " +
                                    "Please verify transaction %s using the OTP sent to you " +
                                    "within %d minutes. If you did not initiate this transaction, " +
                                    "please ignore this message.",
                            transactionId,
                            5
                    )
            );

        } catch (Exception ex) {
            log.error("Error while processing OTP notification", ex);
        }
    }


    @KafkaListener(topics = "transaction.compeleted")
    public void ConsumeTransactionCompleted(@Payload Map<String, Object> payload){

        try {
            String senderAccount = (String) payload.get("senderAccountNumber");
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            String amount = (String) payload.get("ammount").toString();

//            Debit Alert

            sendAlert(senderAccount, "DEBIT_ALERT", String.format("%s debited from account %s", amount, senderAccount));

//            credit Alert

            sendAlert(receiverAccount, "CREDIT_ALERT", String.format("%s credited from account %s", amount, receiverAccount));
        }
        catch (Exception ex) {
            log.error("Error while processing OTP notification", ex);
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber , "SUSPICIOUS ACTIVITY DETECTED", String.format("%s suspicious activity %s", reason, accountNumber));

        }
        catch (Exception ex) {
            log.error("Error while processing OTP notification", ex);
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void TransactionRefunded(@Payload Map<String, Object> payload) {
        try {
            String senderAccount = (String) payload.get("senderAccountNumber");
            String amount = (String) payload.get("amount");
            String reason = (String) payload.get("reason");

            sendAlert(senderAccount , "REFUND PROCESS" , String.format("%s refunded from account %s", amount, reason, senderAccount));

        }
        catch (Exception ex) {
            log.error("Error while processing OTP notification", ex);
        }
    }

    @KafkaListener(topics = "payment.completed")
    public void ConsumePaymentCompleted(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String amount = (String) payload.get("amount");

            sendAlert(accountNumber , "PAYMENT SUCCESSFUL" , String.format(
                    "Payment of %s completed. " + "Razorpay ID: %s", amount, payload.get("razorpayPaymentId")
            ) );

        }
        catch (Exception ex) {
            log.error("Error while processing OTP notification", ex);
        }
    }

    public void ConsumePaymentFailed(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String amount = (String) payload.get("amount");

            sendAlert(accountNumber , "PAYMENT FAILED", String.format("Payment of %s failed.", amount));
        }
        catch (Exception ex) {
            log.error("Error while processing OTP notification", ex);
        }
    }



    private void sendAlert(
            String accountNumber,
            String subject,
            String message) {

        // Simulating SMS/Email notification
        log.info(
                """
                ===== TRANSACTION ALERT =====
                Account   : {}
                Subject   : {}
                Message   : {}
                ==============================
                """,
                accountNumber,
                subject,
                message
        );


    }
}