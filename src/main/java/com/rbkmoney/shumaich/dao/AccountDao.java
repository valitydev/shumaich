package com.rbkmoney.shumaich.dao;

import lombok.RequiredArgsConstructor;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Component
@RequiredArgsConstructor
// todo is even needed? Could be replaced with balanceDao
public class AccountDao {

    private final static String COLUMN_FAMILY_NAME = "account";
    private ColumnFamilyHandle columnFamilyHandle;

    private final RocksDB rocksDB;

    @PostConstruct
    public void initializeColumnFamily() throws RocksDBException {
        this.columnFamilyHandle = rocksDB.createColumnFamily(new ColumnFamilyDescriptor(COLUMN_FAMILY_NAME.getBytes()));
    }

    @PreDestroy
    public void closeColumnFamily() {
        this.columnFamilyHandle.close();
    }

}
