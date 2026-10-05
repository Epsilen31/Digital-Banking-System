package com.banking.accountservice.service;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import com.banking.accountservice.repository.AccountRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class AccountService {

    private final AccountRepository accountRepository;

    AccountService(AccountRepository accountRepository) {
        this.accountRepository = accountRepository;
    }


    @Transactional
    public AccountResponse createAccount(CreateAccountRequest request) {

        if (accountRepository.existsByEmail(request.getEmail())) {
            throw new RuntimeException("Account already exists with this email");
        }

        Account account = new Account();

        account.setAccountNumber(generateAccountNumber());
        account.setAccountHolderName(request.getAccountHolderName());
        account.setEmail(request.getEmail());
        account.setPhone(request.getPhone());
        account.setAccountType(request.getAccountType());
        account.setAccountStatus(AccountStatus.ACTIVE);
        account.setBalance(request.getInitialDeposit());

        account.setDailyTransactionLimit( request.getAccountType() == AccountType.SAVING
                        ? new BigDecimal("100000.00")
                        : new BigDecimal("500000.00")
                );

        Account savedAccount = accountRepository.save(account);

        return mapToResponse(savedAccount);
    }

    @Transactional(readOnly = true)
    public AccountResponse getAccount(String accountNumber) {

        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() ->
                        new RuntimeException("Account not found: " + accountNumber));

        return mapToResponse(account);
    }

    @Transactional(readOnly = true)
    public BigDecimal getBalance(String accountNumber) {

        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() ->
                        new RuntimeException("Account not found: " + accountNumber));

        return account.getBalance();
    }

    @Transactional
    public void blockAccount(String accountNumber) {

        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() ->
                        new RuntimeException("Account not found: " + accountNumber));

        if (account.getAccountStatus() == AccountStatus.BLOCKED) {
            throw new RuntimeException("Account is already blocked");
        }

        if (account.getAccountStatus() == AccountStatus.CLOSED) {
            throw new RuntimeException("Closed account cannot be blocked");
        }

        account.setAccountStatus(AccountStatus.BLOCKED);

        accountRepository.save(account);
    }

    @Transactional
    public void deductBalance(String accountNumber, BigDecimal deductAmount) {

        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() ->
                        new RuntimeException("Account not found: " + accountNumber));

        if (account.getAccountStatus() != AccountStatus.ACTIVE) {
            throw new RuntimeException("Account is not active");
        }

        if (deductAmount == null || deductAmount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new RuntimeException("Deduct amount must be greater than zero");
        }

        if (account.getBalance().compareTo(deductAmount) < 0) {
            throw new RuntimeException("Insufficient balance");
        }

        account.setBalance(account.getBalance().subtract(deductAmount));

        accountRepository.save(account);
    }

    @Transactional
    public void creditBalance(String accountNumber, BigDecimal creditAmount) {

        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() ->
                        new RuntimeException("Account not found: " + accountNumber));

        if (account.getAccountStatus() != AccountStatus.ACTIVE) {
            throw new RuntimeException("Account is not active");
        }

        if (creditAmount == null || creditAmount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new RuntimeException("Credit amount must be greater than zero");
        }

        account.setBalance(account.getBalance().add(creditAmount));

        accountRepository.save(account);
    }

    private String generateAccountNumber() {
        return UUID.randomUUID()
                .toString()
                .replace("-", "")
                .substring(0, 12)
                .toUpperCase();
    }

    private AccountResponse mapToResponse(Account account) {

        AccountResponse response = new AccountResponse();

        response.setId(account.getId());
        response.setAccountNumber(account.getAccountNumber());
        response.setAccountHolderName(account.getAccountHolderName());
        response.setEmail(account.getEmail());
        response.setPhone(account.getPhone());
        response.setAccountType(account.getAccountType());
        response.setAccountStatus(account.getAccountStatus());
        response.setBalance(account.getBalance());
        response.setDailyTransactionLimit(account.getDailyTransactionLimit());
        response.setCreatedAt(account.getCreatedAt());

        return response;
    }
}