package com.rbkmoney.shumaich.handler;

import com.rbkmoney.damsel.shumpune.*;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.service.ClockService;
import com.rbkmoney.shumaich.service.RequestRegistrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShumaichServiceHandler implements AccounterSrv.Iface {

    private final ClockService clockService;
    private final RequestRegistrationService service;

    @Override
    public Clock hold(PostingPlanChange postingPlanChange, Clock clock) throws InvalidPostingParams, TException {
        clockService.softCheckClockTimeline(clock);
        return service.registerHold(postingPlanChange);
    }

    @Override
    public Clock commitPlan(PostingPlan postingPlan, Clock clock) throws InvalidPostingParams, NotReady, TException {
        clockService.hardCheckClockTimeline(clock);
        return service.registerFinalOp(postingPlan, OperationType.COMMIT);
    }

    @Override
    public Clock rollbackPlan(PostingPlan postingPlan, Clock clock) throws InvalidPostingParams, NotReady, TException {
        clockService.hardCheckClockTimeline(clock);
        return service.registerFinalOp(postingPlan, OperationType.ROLLBACK);
    }

    @Override
    public Balance getBalanceByID(String s, Clock clock) throws AccountNotFound, NotReady, TException {
        clockService.hardCheckClockTimeline(clock);
        return null;
    }

    @Override
    public Account getAccountByID(String s, Clock clock) throws AccountNotFound, NotReady, TException {
        clockService.softCheckClockTimeline(clock);
        return null;
    }
}
