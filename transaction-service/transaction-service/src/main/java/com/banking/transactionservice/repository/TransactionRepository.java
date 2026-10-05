package com.banking.transactionservice.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.banking.transactionservice.entity.Transaction;

public interface TransactionRepository extends JpaRepository<Transaction, String> {

    List<Transaction> findBySenderAccountNumberOrderByCreatedAtDesc(
            String senderAccountNumber
    );

    Optional<Transaction> findByTransactionId(String transactionId);
}