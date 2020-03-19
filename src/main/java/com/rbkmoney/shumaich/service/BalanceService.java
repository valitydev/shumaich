package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.converter.CommonConverter;
import com.rbkmoney.shumaich.dao.BalanceDao;
import com.rbkmoney.shumaich.domain.Account;
import com.rbkmoney.shumaich.domain.Balance;
import com.rbkmoney.shumaich.domain.OperationLog;
import com.rbkmoney.shumaich.exception.DaoException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class BalanceService {

    private final BalanceDao balanceDao;
    private final PlanService planService;
    private final TransactionDB rocksDB;

    public void createNewBalance(Account account) {
        balanceDao.put(Balance.builder()
                .accountId(getKey(account))
                .currencySymbolicCode(account.getCurrencySymbolicCode())
                .amount(0L)
                .minAmount(0L)
                .maxAmount(0L)
                .build()
        );
    }

    //todo maybe we should do batch processing
    public void proceedHold(OperationLog operationLog) {
        if (planService.operationLogExists(operationLog)) {
            return;
        }
        WriteOptions writeOptions = new WriteOptions().setSync(true);
        Transaction transaction = rocksDB.beginTransaction(writeOptions);
        try {
            formHoldTransaction(operationLog, transaction);
            transaction.commit();
        } catch (RocksDBException e) {
            log.error("Error in proceedHold, operationLog: {}", operationLog);
            rollbackTransaction(writeOptions, transaction);
            throw new DaoException("Error in proceedHold, operationLog: " + operationLog, e);
        } finally {
            transaction.close();
            writeOptions.close();
        }
    }

    private void rollbackTransaction(WriteOptions writeOptions, Transaction transaction) {
        try {
            transaction.rollback();
            writeOptions.close();
        } catch (RocksDBException ex) {
            log.error("Can't rollback transaction, lol", ex);
        }
    }

    private void formHoldTransaction(OperationLog operationLog, Transaction transaction) throws RocksDBException {
        Balance balanceForUpdate = balanceDao.getForUpdate(transaction, getKey(operationLog.getAccount()));
        Balance balance = calculateBalance(balanceForUpdate, operationLog);
        balanceDao.putInTransaction(transaction, balance);

        planService.processPlanModification(transaction, operationLog);
    }

    private String getKey(Account account) {
        return account.getId();
    }

    private Balance calculateBalance(Balance balance, OperationLog operationLog) {
        Long amount = operationLog.getAmountWithSign();
        switch (operationLog.getOperationType()) {
            case HOLD:
                if (amount > 0) {
                    balance.setMaxAmount(balance.getMaxAmount() + amount);
                } else {
                    balance.setMinAmount(balance.getMinAmount() + amount);
                }
                break;
            case COMMIT:
                balance.setAmount(balance.getAmount() + amount);
                // добавление min/max amount захолдированной суммы (по протоколу)
                if (amount > 0) {
                    balance.setMinAmount(balance.getMinAmount() + amount);
                } else {
                    balance.setMaxAmount(balance.getMaxAmount() + amount);
                }
                break;
            case ROLLBACK:
                if (amount > 0) {
                    balance.setMaxAmount(balance.getMaxAmount() - amount);
                } else {
                    balance.setMinAmount(balance.getMinAmount() - amount);
                }
                break;
        }
        return balance;
    }

    public boolean balanceExists(String accountId) {
        return balanceDao.get(accountId) != null;
    }
}
