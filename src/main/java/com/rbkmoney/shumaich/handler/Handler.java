package com.rbkmoney.shumaich.handler;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface Handler<V> {

    void handle(ConsumerRecords<?, V> records);
}
