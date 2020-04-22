package com.rbkmoney.shumaich.helpers;

import com.rbkmoney.damsel.shumpune.*;

import java.util.ArrayList;
import java.util.List;

public class PostingGenerator {

    public static final long BATCH_ID = 1L;


    public static PostingBatch createBatch(String providerAcc, String systemAcc, String merchantAcc, Long batchId) {
        PostingBatch batch = new PostingBatch();
        batch.setId(batchId);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(100)
                .setFromAccount(new Account(providerAcc, "RUB"))
                .setToAccount(new Account(merchantAcc, "RUB"))
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(3)
                .setFromAccount(new Account(merchantAcc, "RUB"))
                .setToAccount(new Account(systemAcc, "RUB"))
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(1)
                .setFromAccount(new Account(systemAcc, "RUB"))
                .setToAccount(new Account(providerAcc, "RUB"))
                .setDescription("qwe"));
        batch.setPostings(postings);
        return batch;
    }

    public static PostingBatch createBatch(String providerAcc, String systemAcc, String merchantAcc) {
        return createBatch(providerAcc, systemAcc, merchantAcc, BATCH_ID);
    }

    public static PostingBatch createBatchWithFixedAmount(String providerAcc, String systemAcc, String merchantAcc, Long amount) {
        PostingBatch batch = new PostingBatch();
        batch.setId(BATCH_ID);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(amount)
                .setFromAccount(new Account(providerAcc, "RUB"))
                .setToAccount(new Account(merchantAcc, "RUB"))
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(amount)
                .setFromAccount(new Account(merchantAcc, "RUB"))
                .setToAccount(new Account(systemAcc, "RUB"))
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(amount)
                .setFromAccount(new Account(systemAcc, "RUB"))
                .setToAccount(new Account(providerAcc, "RUB"))
                .setDescription("qwe"));
        batch.setPostings(postings);
        return batch;
    }

    public static PostingPlanChange createPostingPlanChange(String planId, String providerAcc, String systemAcc, String merchantAcc) {
        PostingPlanChange postingPlanChange = new PostingPlanChange();
        PostingBatch batch = PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc);
        postingPlanChange.setBatch(batch)
                .setId(planId);
        return postingPlanChange;
    }

    public static PostingPlanChange createPostingPlanChange(String planId, String providerAcc, String systemAcc, String merchantAcc, Long amount) {
        PostingPlanChange postingPlanChange = new PostingPlanChange();
        PostingBatch batch = PostingGenerator.createBatchWithFixedAmount(providerAcc, systemAcc, merchantAcc, amount);
        postingPlanChange.setBatch(batch)
                .setId(planId);
        return postingPlanChange;
    }

    public static PostingPlan createPostingPlan(String planId, String providerAcc, String systemAcc, String merchantAcc) {
        PostingPlan postingPlan = new PostingPlan();
        postingPlan.setBatchList(List.of(
                PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc, BATCH_ID)
        ));
        postingPlan.setId(planId);
        return postingPlan;
    }

    public static Posting createPosting() {
        return new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(1)
                .setFromAccount(new Account("1", "RUB"))
                .setToAccount(new Account("2", "RUB"))
                .setDescription("test");
    }

    public static PostingPlanChange createPostingPlanChangeTwoAccs(String planId, String firstAcc, String secondAcc, long amount) {
        return new PostingPlanChange()
                .setId(planId)
                .setBatch(new PostingBatch()
                        .setId(BATCH_ID)
                        .setPostings(List.of(new Posting()
                                .setCurrencySymCode("RUB")
                                .setAmount(amount)
                                .setFromAccount(new Account(firstAcc, "RUB"))
                                .setToAccount(new Account(secondAcc, "RUB"))
                                .setDescription("qwe"))));
    }
}
