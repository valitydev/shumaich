package com.rbkmoney.shumaich.handler;

import com.rbkmoney.damsel.shumpune.*;
import com.rbkmoney.woody.api.flow.error.WUnavailableResultException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShumaichServiceHandler implements AccounterSrv.Iface {

    @Override
    public ClockState hold(PostingPlanChange postingPlanChange) throws TException {
        log.info("Start hold postingPlanChange: {}", postingPlanChange);
        try {
            return null;
        } catch (Exception ex) {
            // retryable
            log.error("Some exception", ex);
            throw new WUnavailableResultException(ex);
        } catch (Throwable e) {
            //not
            log.error("Failed e", e);
            throw new TException(e);
        }
    }

    @Override
    public ClockState commitPlan(PostingPlan postingPlan, ClockState state) throws TException {
        log.info("Start commitPlan postingPlan: {}", postingPlan);
        try {
            return null;
        } catch (Exception ex) {
            // retryable
            log.error("Some exception", ex);
            throw new WUnavailableResultException(ex);
        } catch (Throwable e) {
            //not
            log.error("Failed e", e);
            throw new TException(e);
        }
    }

    @Override
    public ClockState rollbackPlan(PostingPlan postingPlan, ClockState state) throws TException {
        log.info("Start rollbackPlan postingPlan: {}", postingPlan);
        try {
            return null;
        } catch (Exception ex) {
            // retryable
            log.error("Some exception", ex);
            throw new WUnavailableResultException(ex);
        } catch (Throwable e) {
            //not
            log.error("Failed e", e);
            throw new TException(e);
        }
    }

    @Override
    public Account getAccountByID(long accountId, ClockState state) throws TException {
        log.info("Start getAccountByID accountId: {}", accountId);
        try {
            //todo code
            return null;
        } catch (Exception ex) {
            // retryable
            log.error("Some exception", ex);
            throw new WUnavailableResultException(ex);
        } catch (Throwable e) {
            //not
            log.error("Failed e", e);
            throw new TException(e);
        }
    }

    @Override
    public Balance getBalanceByID(long accountId, ClockState state) throws TException {
        log.info("Start getBalanceByID accountId: {} clock: {}", accountId, state);
        try {
            //todo code
            return null;
        } catch (Exception ex) {
            // retryable
            log.error("Some exception", ex);
            throw new WUnavailableResultException(ex);
        } catch (Throwable e) {
            //not
            log.error("Failed e", e);
            throw new TException(e);
        }
    }

}
