package com.rbkmoney.shumaich.domain;

import com.rbkmoney.damsel.shumpune.PostingBatch;

import java.util.List;

public class RequestLog {
    String planId;
    OperationType operationType;
    List<PostingBatch> postingBatches;
}
