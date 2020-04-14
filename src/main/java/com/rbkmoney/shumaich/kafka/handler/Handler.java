package com.rbkmoney.shumaich.kafka.handler;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface Handler<K, V> {

    void handle(ConsumerRecords<K, V> records);
}
