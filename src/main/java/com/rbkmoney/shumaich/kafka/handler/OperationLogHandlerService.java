package com.rbkmoney.shumaich.kafka.handler;


import com.rbkmoney.damsel.shumaich.OperationLog;
import com.rbkmoney.kafka.common.util.LogUtil;
import com.rbkmoney.shumaich.service.BalanceService;
import com.rbkmoney.shumaich.utils.MdcUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OperationLogHandlerService implements Handler<String, OperationLog> {

    private final BalanceService balanceService;

    @Override
    public void handle(ConsumerRecords<String, OperationLog> records) {
        log.info("Received records: {}", LogUtil.toSummaryString(records));
        for (ConsumerRecord<?, OperationLog> record : records) {
            OperationLog operationLog = record.value();
            try {
                MdcUtils.setMdc(operationLog);
                log.debug("{} processing started", operationLog.getOperationType());
                switch (operationLog.getOperationType()) {
                    case HOLD:
                        processHold(operationLog);
                        break;
                    case COMMIT:
                    case ROLLBACK:
                        processFinalOperation(operationLog);
                        break;
                    default:
                        throw new IllegalArgumentException("Not supported operation: " + operationLog);
                }
                log.debug("{} processing finished", operationLog.getOperationType());
            } finally {
                MdcUtils.clearMdc();
            }
        }

    }

    private void processHold(OperationLog operationLog) {
        if (!balanceService.balanceExists(operationLog.getAccount().getId())) {
            balanceService.createNewBalance(operationLog.getAccount());
        }
        balanceService.proceedHold(operationLog);
    }

    private void processFinalOperation(OperationLog operationLog) {
        if (operationLog.getValidationError() != null) {
            return;
        }
        balanceService.proceedFinalOp(operationLog);
    }

}
