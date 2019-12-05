package com.rbkmoney.shumaich.service;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface Handler<V> {

    void handle(ConsumerRecords<?, V> records);
}
