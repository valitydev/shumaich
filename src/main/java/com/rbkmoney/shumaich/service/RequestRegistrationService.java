package com.rbkmoney.shumaich.service;

import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.converter.PostingPlanChangeToRequestLogConverter;
import com.rbkmoney.shumaich.converter.PostingPlanToRequestLogConverter;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.domain.RequestLog;
import com.rbkmoney.shumaich.utils.ClockTokenFormatter;
import com.rbkmoney.shumaich.utils.VectorClockSerializer;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class RequestRegistrationService {

    private final KafkaTemplate<String, RequestLog> requestLogKafkaTemplate;
    private final PostingPlanChangeToRequestLogConverter postingPlanChangeToRequestLogConverter;
    private final PostingPlanToRequestLogConverter postingPlanToRequestLogConverter;

    public Clock registerHold(PostingPlanChange postingPlanChange) {
        RequestLog requestLog = postingPlanChangeToRequestLogConverter.convert(postingPlanChange);
        requestLogKafkaTemplate.sendDefault(requestLog.getPlanId(), requestLog);
        return Clock.vector(VectorClockSerializer.serialize(ClockTokenFormatter.getClock(requestLog)));
    }

    public Clock registerCommit(PostingPlan postingPlan) {
        RequestLog requestLog = postingPlanToRequestLogConverter.convert(postingPlan, OperationType.COMMIT);
        requestLogKafkaTemplate.sendDefault(requestLog.getPlanId(), requestLog);
        return Clock.vector(VectorClockSerializer.serialize(ClockTokenFormatter.getClock(requestLog)));
    }

    public Clock registerRollback(PostingPlan postingPlan) {
        RequestLog requestLog = postingPlanToRequestLogConverter.convert(postingPlan, OperationType.ROLLBACK);
        requestLogKafkaTemplate.sendDefault(requestLog.getPlanId(), requestLog);
        return Clock.vector(VectorClockSerializer.serialize(ClockTokenFormatter.getClock(requestLog)));
    }

}
