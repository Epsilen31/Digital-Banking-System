package com.banking.transactionservice.controller;

import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.service.TransactionService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/transactions")
public class TransactionController {

    private final TransactionService transactionService;

    public TransactionController(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @PostMapping("/transfer")
    public ResponseEntity<TransactionResponse> transfer(@Valid @RequestBody TransferRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(transactionService.transfer(request));
    }

    @GetMapping("/{txnId}")
    public ResponseEntity<TransactionResponse> getTransaction(@PathVariable String txnId) {
        return ResponseEntity.ok(transactionService.getTransaction(txnId));
    }

    @GetMapping("/account/{accountNumber}")
    public ResponseEntity<List<TransactionResponse>> getTransactionHistory(@PathVariable String accountNumber) {
        return ResponseEntity.ok(transactionService.getTransactionHistory(accountNumber));
    }

    @PostMapping("/{txnId}/verify-otp")
    public ResponseEntity<TransactionResponse> verifyOTP(
            @PathVariable String txnId,
            @RequestParam String otp) {
        return ResponseEntity.ok(transactionService.verifyOTP(txnId, otp));
    }
}