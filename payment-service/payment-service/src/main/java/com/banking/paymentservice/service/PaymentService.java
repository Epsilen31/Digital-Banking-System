package com.banking.paymentservice.service;

import com.banking.paymentservice.dto.PaymentOrderResponse;
import com.banking.paymentservice.dto.PaymentRequest;
import com.banking.paymentservice.entity.Payment;
import com.banking.paymentservice.entity.PaymentStatus;
import com.banking.paymentservice.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;

@Slf4j
@Service
@AllArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment-completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment-failed";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.key-secret}")
    private String keySecret;

    public PaymentOrderResponse createPaymentOrder(PaymentRequest request)  {
        try {
            RazorpayClient  razorpayClient = new RazorpayClient(keyId, keySecret);

            JSONObject orderRequest = new JSONObject();
            orderRequest.put("amount", request.getAmount().multiply(BigDecimal.valueOf(100)).intValue());
            orderRequest.put("currency", "INR");
            orderRequest.put("receipt", "txn_" + System.currentTimeMillis());

            Order order = razorpayClient.orders.create(orderRequest);

            Payment payment = new Payment();
            payment.setRazorpayOrderId(order.get("id").toString());
            payment.setAccountNumber(request.getAccountNumber());
            payment.setAmount(request.getAmount());
            payment.setCurrency("INR");
            payment.setPaymentStatus(PaymentStatus.CREATED);
            payment.setDescription(request.getDescription());

            Payment saved = paymentRepository.save(payment);
            return toResponse(saved);

        } catch (Exception ex) {
            log.error("Failed to create Razorpay order for account={}", request.getAccountNumber(), ex);
            throw new RuntimeException("Failed to create payment order", ex);
        }
    }

    public void handleWebhook(Map<String, Object> payload) {
        String eventType = (String) payload.get("event");
        if ("payment.captured".equals(eventType)) {
            handlePaymentSuccess(payload);
        } else if ("payment.failed".equals(eventType)) {
            handlePaymentFailure(payload);
        }
    }

    private void handlePaymentSuccess(Map<String, Object> payload) {
        try {
            Map<String, Object> paymentEntity = extractPaymentEntity(payload);
            String razorpayOrderId = (String) paymentEntity.get("order_id");
            String razorpayPaymentId = (String) paymentEntity.get("id");

            Payment payment = paymentRepository.findByRazorpayOrderId(razorpayOrderId)
                    .orElseThrow(()-> new RuntimeException(" payment not found for " + razorpayPaymentId));

            payment.setRazorpayPaymentId(razorpayPaymentId);
            payment.setPaymentStatus(PaymentStatus.COMPLETED);
            paymentRepository.save(payment);

//          publish to kafa

            Map<String, Object> event = new HashMap<>();

            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("paymentStatus", PaymentStatus.COMPLETED);
            event.put("amount", payment.getAmount());
            event.put("currency", payment.getCurrency());
            event.put("razorpayPaymentId", razorpayPaymentId);

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId(), event);

        }
        catch (Exception ex) {

            log.error("error handling payment", ex);

        }
    }

    private void handlePaymentFailure(Map<String, Object> payload) {

        try {
            Map<String, Object> paymentEntity = extractPaymentEntity(payload);
            String razorpayOrderId = (String) paymentEntity.get("order_id");

            Payment payment = paymentRepository.findByRazorpayOrderId(razorpayOrderId)
                    .orElseThrow(()-> new RuntimeException(" payment not found for " + razorpayOrderId));

            payment.setPaymentStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment failed via razorpay");
            paymentRepository.save(payment);


            Map<String, Object> event = new HashMap<>();

            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("reason", "Payment failed via razorpay");

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC, payment.getId(), event);

        }
        catch (Exception ex) {
            log.error("error handling payment", ex);
        }

    }


    @SuppressWarnings("unchecked")
    private Map<String, Object> extractPaymentEntity(Map<String, Object> payload) {
        Map<String, Object> payloadWrapper = (Map<String, Object>) payload.get("payload");
        Map<String, Object> paymentWrapper = (Map<String, Object>) payloadWrapper.get("payment");
        return (Map<String, Object>) paymentWrapper.get("entity");
    }

    private PaymentOrderResponse toResponse(Payment payment) {
        return PaymentOrderResponse.builder()
                .paymentId(payment.getId())
                .razorpayOrderId(payment.getRazorpayOrderId())
                .amount(payment.getAmount())
                .currency(payment.getCurrency())
                .status(payment.getPaymentStatus().name())
                .razorpayKeyId(keyId)
                .build();
    }
}