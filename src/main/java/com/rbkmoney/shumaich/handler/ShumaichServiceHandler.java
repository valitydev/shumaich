package com.rbkmoney.shumaich.handler;

import com.rbkmoney.damsel.shumpune.*;
import com.rbkmoney.shumaich.service.RequestRegistrationService;
import com.rbkmoney.woody.api.flow.error.WUnavailableResultException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShumaichServiceHandler implements AccounterSrv.Iface {

    private final RequestRegistrationService requestRegistrationService;

    @Override
    public Clock hold(PostingPlanChange postingPlanChange) throws TException {
        log.info("Start hold postingPlanChange: {}", postingPlanChange);
        try {
            return requestRegistrationService.registerHold(postingPlanChange);
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
    public Clock commitPlan(PostingPlan postingPlan) throws TException {
        log.info("Start commitPlan postingPlan: {}", postingPlan);
        try {
            return requestRegistrationService.registerCommit(postingPlan);
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
    public Clock rollbackPlan(PostingPlan postingPlan) throws TException {
        log.info("Start rollbackPlan postingPlan: {}", postingPlan);
        try {
            return requestRegistrationService.registerRollback(postingPlan);
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
    public PostingPlan getPlan(String planId) throws PlanNotFound, TException {
        log.info("Start getPlan planId: {}", planId);
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
    public Account getAccountByID(long accountId) throws TException {
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
    public Balance getBalanceByID(long accountId, Clock clock) throws TException {
        log.info("Start getBalanceByID accountId: {} clock: {}", accountId, clock);
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
    public long createAccount(AccountPrototype accountPrototype) throws TException {
        log.info("Start createAccount prototype: {}", accountPrototype);
        try {
            //todo code
            return 0L;
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
