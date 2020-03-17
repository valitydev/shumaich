package com.rbkmoney.shumaich.config;

import com.rbkmoney.shumaich.dao.RocksDbDao;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Configuration
public class RocksDbConfiguration {

    @Bean
    TransactionDB rocksDB(@Value("${rocksdb.name}") String name,
                          @Value("${rocksdb.dir}") String dbDir,
                          List<RocksDbDao> daoList) throws RocksDBException {
        TransactionDB.loadLibrary();
        final DBOptions options = new DBOptions();
        options.setCreateIfMissing(true);
        File dbFile = new File(dbDir, name);
        ArrayList<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        try {
            TransactionDBOptions transactionDbOptions = new TransactionDBOptions();
            TransactionDB open = TransactionDB.open(options, transactionDbOptions, dbFile.getAbsolutePath(),
                    getColumnFamilyDescriptors(daoList), columnFamilyHandles);
            mapHandlesToDaos(columnFamilyHandles, daoList);
            return open;
        } catch (RocksDBException ex) {
            log.error("Error initializing RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
                    ex.getCause(), ex.getMessage(), ex.getStackTrace());
            throw ex;
        }
    }


    private List<ColumnFamilyDescriptor> getColumnFamilyDescriptors(List<RocksDbDao> daoList) {
        List<ColumnFamilyDescriptor> descriptors = daoList.stream().map(RocksDbDao::getColumnFamilyDescriptor).collect(Collectors.toList());
        descriptors.add(new ColumnFamilyDescriptor("default".getBytes()));
        return descriptors;
    }

    private void mapHandlesToDaos(List<ColumnFamilyHandle> columnFamilyHandles, List<RocksDbDao> daoList) throws RocksDBException {
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            for (RocksDbDao rocksDbDao : daoList) {
                if (columnFamilyHandle.getDescriptor().equals(rocksDbDao.getColumnFamilyDescriptor())) {
                    rocksDbDao.initColumnFamilyHandle(columnFamilyHandle);
                }
            }
        }
    }

}
