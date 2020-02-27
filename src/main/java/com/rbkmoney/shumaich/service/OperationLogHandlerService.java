package com.rbkmoney.shumaich.service;

import com.rbkmoney.shumaich.dao.AccountDao;
import com.rbkmoney.shumaich.dao.BalanceDao;
import com.rbkmoney.shumaich.dao.KafkaOffsetDao;
import com.rbkmoney.shumaich.domain.OperationLog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OperationLogHandlerService implements Handler<OperationLog> {

    private final AccountDao accountDao;
    private final BalanceDao balanceDao;
    private final KafkaOffsetDao kafkaOffsetDao;

    @Override
    public void handle(ConsumerRecords<?, OperationLog> records) {
//todo log rbk library        log.info("Received records: {}", records);
        for (ConsumerRecord<?, OperationLog> record : records) {
            OperationLog operationLog = record.value();
            switch (operationLog.getOperationType()) {
                case HOLD:
                    processHold(operationLog);
                    break;
                case COMMIT:
                case ROLLBACK:
                    processFinalOperation(operationLog);
                    break;
                default:
                    throw new RuntimeException("Not supported operation: " + operationLog);
            }
        }

    }

    private void processHold(OperationLog operationLog) {
        if (balanceDao.getBalance(operationLog.getAccount().getId()) == null) {
            balanceDao.createNewBalance(operationLog.getAccount());
        }
            balanceDao.proceedHold(operationLog);
    }

    private void processFinalOperation(OperationLog operationLog) {
        //todo
    }
}
