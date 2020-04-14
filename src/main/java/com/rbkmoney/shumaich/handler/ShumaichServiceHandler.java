package com.rbkmoney.shumaich.handler;

import com.rbkmoney.damsel.shumpune.*;
import com.rbkmoney.shumaich.domain.OperationType;
import com.rbkmoney.shumaich.exception.*;
import com.rbkmoney.shumaich.service.BalanceService;
import com.rbkmoney.shumaich.service.ClockService;
import com.rbkmoney.shumaich.service.RequestRegistrationService;
import com.rbkmoney.shumaich.utils.MdcUtils;
import com.rbkmoney.woody.api.flow.error.WErrorDefinition;
import com.rbkmoney.woody.api.flow.error.WRuntimeException;
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
    private final BalanceService balanceService;

    @Override
    public Clock hold(PostingPlanChange postingPlanChange, Clock clock) throws InvalidPostingParams, TException {
        try {
            MdcUtils.setMdc(postingPlanChange);
            log.info("Received hold operation");
            clockService.softCheckClockTimeline(clock);
            return service.registerHold(postingPlanChange);
        } catch (NotReadyException e) {
            log.info("Previous hold is not read yet", e);
            throw new NotReady();
        } catch (CurrencyInPostingsNotConsistentException
                | AccountsInPostingsAreEqualException
                | AccountsHaveDifferentCurrenciesException e) {
            log.warn("Postings are invalid!", e);
            throw new InvalidPostingParams();
        } catch (Exception e) {
            log.error("Some error happened", e);
            throw new TException(e);
        } finally {
            MdcUtils.clearMdc();
        }
    }

    @Override
    public Clock commitPlan(PostingPlan postingPlan, Clock clock) throws InvalidPostingParams, NotReady, TException {
        try {
            MdcUtils.setMdc(postingPlan);
            log.info("Received commit operation");
            clockService.hardCheckClockTimeline(clock);
            return service.registerFinalOp(postingPlan, OperationType.COMMIT);
        } catch (NotReadyException e) {
            log.info("Hold is not read yet", e);
            throw new NotReady();
        } catch (CurrencyInPostingsNotConsistentException
                | AccountsInPostingsAreEqualException
                | AccountsHaveDifferentCurrenciesException
                | HoldChecksumMismatchException e) {
            log.warn("Postings are invalid!", e);
            throw new InvalidPostingParams();
        } catch (HoldNotExistException e) {
            log.info("Hold not exist, maybe it is already cleared", e);
            throw new WRuntimeException("Hold can be already cleared", new WErrorDefinition()); //todo discuss with erlang team
        } catch (Exception e) {
            log.error("Some error happened", e);
            throw new TException(e);
        } finally {
            MdcUtils.clearMdc();
        }

    }

    @Override
    public Clock rollbackPlan(PostingPlan postingPlan, Clock clock) throws InvalidPostingParams, NotReady, TException {
        try {
            MdcUtils.setMdc(postingPlan);
            log.info("Received rollback operation");
            clockService.hardCheckClockTimeline(clock);
            return service.registerFinalOp(postingPlan, OperationType.ROLLBACK);
        } catch (NotReadyException e) {
            log.info("Hold is not read yet", e);
            throw new NotReady();
        } catch (CurrencyInPostingsNotConsistentException
                | AccountsInPostingsAreEqualException
                | AccountsHaveDifferentCurrenciesException
                | HoldChecksumMismatchException e) {
            log.warn("Postings are invalid!", e);
            throw new InvalidPostingParams();
        } catch (HoldNotExistException e) {
            log.info("Hold not exist, maybe it is already cleared", e);
            throw new WRuntimeException("Hold can be already cleared", new WErrorDefinition()); //todo discuss with erlang team
        } catch (Exception e) {
            log.error("Some error happened", e);
            throw new TException(e);
        } finally {
            MdcUtils.clearMdc();
        }

    }

    @Override
    public Balance getBalanceByID(String accountId, Clock clock) throws AccountNotFound, NotReady, TException {
        try {
            MdcUtils.setMdc(accountId);
            clockService.hardCheckClockTimeline(clock);
            return null;

        } catch (Exception e) {
            return null;
        } finally {
            MdcUtils.clearMdc();
        }
    }

    @Override
    public Account getAccountByID(String accountId, Clock clock) throws AccountNotFound, NotReady, TException {
        try {
            MdcUtils.setMdc(accountId);
            clockService.softCheckClockTimeline(clock);
            return null;

        } catch (Exception e) {
            return null;
        } finally {
            MdcUtils.clearMdc();
        }
    }
}
