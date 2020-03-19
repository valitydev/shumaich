package com.rbkmoney.shumaich.dao;

import com.rbkmoney.shumaich.converter.CommonConverter;
import com.rbkmoney.shumaich.domain.Account;
import com.rbkmoney.shumaich.domain.Balance;
import com.rbkmoney.shumaich.domain.OperationLog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class BalanceDao extends RocksDbDao {

    private final static String COLUMN_FAMILY_NAME = "balance";

    private final PlanDao planDao;

    @Override
    public byte[] getColumnFamilyName() {
        return COLUMN_FAMILY_NAME.getBytes();
    }

//    @PreDestroy
//    public void destroy() {
//        super.destroyColumnFamilyHandle();
//    }

    public void createNewBalance(Account account) {
        try {
            rocksDB.put(columnFamilyHandle, account.getId().getBytes(),
                    CommonConverter.toBytes(Balance.builder()
                            .accountId(account.getId())
                            .currencySymbolicCode(account.getCurrencySymbolicCode())
                            .amount(0L)
                            .minAmount(0L)
                            .maxAmount(0L)
                            .build()
                    )
            );
        } catch (RocksDBException e) {
            log.error("Can't create account with ID: {}", account.getId(), e);
            throw new RuntimeException("Can't create account with ID: " + account.getId());
        }
    }

    @Nullable
    public Balance getBalance(String accountId) {
        try {
            return CommonConverter.fromBytes(
                    rocksDB.get(columnFamilyHandle, accountId.getBytes()),
                    Balance.class
            );
        } catch (RocksDBException e) {
            log.error("Can't create account with ID: {}", accountId, e);
            throw new RuntimeException("Can't create account with ID: " + accountId);
        }
    }

    public void proceedHold(OperationLog operationLog) {
        if (planDao.operationLogExists(operationLog)) {
            return;
        }
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.setSync(true);
        Transaction transaction = rocksDB.beginTransaction(writeOptions);
        try {
            byte[] key = operationLog.getAccount().getId().getBytes();
            byte[] balanceBytes = transaction.getForUpdate(new ReadOptions(), columnFamilyHandle,
                    key, true);
            Balance balance = calculateBalance(
                    CommonConverter.fromBytes(balanceBytes, Balance.class),
                    operationLog
            );
            transaction.put(columnFamilyHandle, key, CommonConverter.toBytes(balance));
            planDao.planModificationReceived(transaction, operationLog);
            transaction.commit();
            transaction.close();
            writeOptions.close();
        } catch (RocksDBException e) {
            //todo
            log.error("Error in proceedHold");
            e.printStackTrace();
            try {
                transaction.rollback();
            } catch (RocksDBException ex) {
                log.error("Can't rollback transaction, lol");
            }
        } finally {
            transaction.close();
        }
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

    public ColumnFamilyHandle getColumnFamilyHandle() {
        return columnFamilyHandle;
    }

}
