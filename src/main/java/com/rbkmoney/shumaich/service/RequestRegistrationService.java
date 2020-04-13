package com.rbkmoney.shumaich.service;

import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.converter.PostingPlanChangeToPostingPlanOperationConverter;
import com.rbkmoney.shumaich.converter.PostingPlanToPostingPlanOperationConverter;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.domain.PostingPlanOperation;
import com.rbkmoney.shumaich.utils.VectorClockSerde;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class RequestRegistrationService {

    private final PostingPlanChangeToPostingPlanOperationConverter holdConverter;
    private final PostingPlanToPostingPlanOperationConverter finalOpConverter;
    private final WriterService writerService;
    private final ClockService clockService;
    private final ValidationService validationService;

    public Clock registerHold(PostingPlanChange postingPlanChange) {
        validationService.validatePostings(postingPlanChange);

        PostingPlanOperation postingPlanOperation = holdConverter.convert(postingPlanChange);
        return writeToTopic(postingPlanOperation);
    }

    public Clock registerCommit(PostingPlan postingPlan) {
        validationService.validatePostings(postingPlan);
        PostingPlanOperation postingPlanOperation = finalOpConverter.convert(postingPlan, OperationType.COMMIT);
        validationService.validateCommit(postingPlanOperation);

        return writeToTopic(postingPlanOperation);
    }

//
//    public Clock registerRollback(PostingPlan postingPlan) {
//
//    }

    private Clock writeToTopic(PostingPlanOperation postingPlanOperation) {
        List<RecordMetadata> partitionsMetadata = writerService.write(postingPlanOperation);
        String clock = clockService.formClock(partitionsMetadata);
        return Clock.vector(VectorClockSerde.serialize(clock));
    }

}
