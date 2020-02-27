package com.rbkmoney.shumaich.service;

import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.converter.PostingPlanChangeToPostingPlanOperationConverter;
import com.rbkmoney.shumaich.utils.VectorClockSerde;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class RequestTransformationService {

    private final PostingPlanChangeToPostingPlanOperationConverter holdConverter;
    private final PostingPlanChangeToPostingPlanOperationConverter finalOpConverter;
    private final WriterService writerService;
    private final ClockService clockService;
    private final ValidationService validationService;

    public Clock registerHold(PostingPlanChange postingPlanChange) {
//        validationService.validatePostings(postingPlanChange)
//        validationService.validateNotDuplicate(postingPlanChange)

        List<RecordMetadata> partitionsMetadata = writerService.write(holdConverter.convert(postingPlanChange));
        String clock = clockService.formClock(partitionsMetadata);
        return Clock.vector(VectorClockSerde.serialize(clock));
    }

//    public Clock registerCommit(PostingPlan postingPlan) {
//
//    }
//
//    public Clock registerRollback(PostingPlan postingPlan) {
//
//    }


}
