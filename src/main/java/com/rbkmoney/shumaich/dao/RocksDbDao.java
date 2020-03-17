package com.rbkmoney.shumaich.dao;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.TransactionDB;

public abstract class RocksDbDao {

    protected ColumnFamilyHandle columnFamilyHandle;

    protected TransactionDB rocksDB;

    public abstract ColumnFamilyDescriptor getColumnFamilyDescriptor();

    public void initColumnFamilyHandle(ColumnFamilyHandle columnFamilyHandle) {
        this.columnFamilyHandle = columnFamilyHandle;
    }

    public void destroyColumnFamilyHandle() {
        this.columnFamilyHandle.close();
    };
}
