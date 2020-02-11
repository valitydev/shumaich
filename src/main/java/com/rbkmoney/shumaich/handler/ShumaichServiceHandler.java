package com.rbkmoney.shumaich.handler;

import com.rbkmoney.damsel.shumpune.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShumaichServiceHandler implements AccounterSrv.Iface {

    @Override
    public Clock hold(PostingPlanChange postingPlanChange, Clock clock) throws InvalidPostingParams, TException {
        return null;
    }

    @Override
    public Clock commitPlan(PostingPlan postingPlan, Clock clock) throws InvalidPostingParams, NotReady, TException {
        return null;
    }

    @Override
    public Clock rollbackPlan(PostingPlan postingPlan, Clock clock) throws InvalidPostingParams, NotReady, TException {
        return null;
    }

    @Override
    public Balance getBalanceByID(String s, Clock clock) throws AccountNotFound, NotReady, TException {
        return null;
    }

    @Override
    public Account getAccountByID(String s, Clock clock) throws AccountNotFound, NotReady, TException {
        return null;
    }
}
