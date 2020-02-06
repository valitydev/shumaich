package com.rbkmoney.shumaich.config;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;

@Slf4j
@Configuration
public class RocksDbConfiguration {

    @Bean
    RocksDB rocksDB(@Value("${rocksdb.name}") String name,
                    @Value("${rocksdb.dir}") String dbDir) throws RocksDBException {
        RocksDB.loadLibrary();
        final Options options = new Options();
        options.setCreateIfMissing(true);
        File dbFile = new File(dbDir, name);
        try {
            return RocksDB.open(options, dbFile.getAbsolutePath());
        } catch(RocksDBException ex) {
            log.error("Error initializing RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
                    ex.getCause(), ex.getMessage(), ex.getStackTrace());
            throw ex;
        }
    }

}
