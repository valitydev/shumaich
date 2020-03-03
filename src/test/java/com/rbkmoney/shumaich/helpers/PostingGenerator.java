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
                .setFromAccount(new Account("" + providerAcc, "RUB"))
                .setToAccount(new Account("" + merchantAcc, "RUB"))
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(3)
                .setFromAccount(new Account("" + merchantAcc, "RUB"))
                .setToAccount(new Account("" + systemAcc, "RUB"))
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(1)
                .setFromAccount(new Account("" + systemAcc, "RUB"))
                .setToAccount(new Account("" + providerAcc, "RUB"))
                .setDescription("qwe"));
        batch.setPostings(postings);
        return batch;
    }

    public static PostingBatch createBatch(String providerAcc, String systemAcc, String merchantAcc) {
        return createBatch(providerAcc, systemAcc, merchantAcc, BATCH_ID);
    }

//  todo delete if not needed
//
//    batch with amount
//    public static PostingBatch createBatch(Long providerAcc, Long systemAcc, Long merchantAcc, Long amount) {
//        PostingBatch batch = new PostingBatch();
//        batch.setId(BATCH_ID);
//        ArrayList<Posting> postings = new ArrayList<>();
//        postings.add(new Posting()
//                .setCurrencySymCode("RUB")
//                .setAmount(amount)
//                .setFromAccount(new Account("" + providerAcc, "RUB"))
//                .setToAccount(new Account("" + merchantAcc, "RUB"))
//                .setDescription("qwe"));
//        postings.add(new Posting()
//                .setCurrencySymCode("RUB")
//                .setAmount(amount)
//                .setFromAccount(new Account("" + merchantAcc, "RUB"))
//                .setToAccount(new Account("" + systemAcc, "RUB"))
//                .setDescription("qwe"));
//        postings.add(new Posting()
//                .setCurrencySymCode("RUB")
//                .setAmount(amount)
//                .setFromAccount(new Account("" + systemAcc, "RUB"))
//                .setToAccount(new Account("" + providerAcc, "RUB"))
//                .setDescription("qwe"));
//        batch.setPostings(postings);
//        return batch;
//    }
//
//    // real production batches
//    public static PostingBatch createBatch(Long firstAcc, Long secAcc, Long thirdAcc, Long fourthAcc, int multiplier) {
//        PostingBatch batch = new PostingBatch();
//        batch.setId(BATCH_ID);
//        ArrayList<Posting> postings = new ArrayList<>();
//        postings.add(new Posting()
//                .setCurrencySymCode("RUB")
//                .setAmount(2800 * multiplier)
//                .setFromAccount(new Account("" + firstAcc, "RUB"))
//                .setToAccount(new Account("" + secAcc, "RUB"))
//                .setDescription("1->2"));
//        postings.add(new Posting()
//                .setCurrencySymCode("RUB")
//                .setAmount(4000 * multiplier)
//                .setFromAccount(new Account("" + firstAcc, "RUB"))
//                .setToAccount(new Account("" + thirdAcc, "RUB"))
//                .setDescription("1->3"));
//        postings.add(new Posting()
//                .setCurrencySymCode("RUB")
//                .setAmount(80000 * multiplier)
//                .setFromAccount(new Account("" + fourthAcc, "RUB"))
//                .setToAccount(new Account("" + firstAcc, "RUB"))
//                .setDescription("4->1"));
//        postings.add(new Posting()
//                .setCurrencySymCode("RUB")
//                .setAmount(1760 * multiplier)
//                .setFromAccount(new Account("" + secAcc, "RUB"))
//                .setToAccount(new Account("" + fourthAcc, "RUB"))
//                .setDescription("4->2"));
//        batch.setPostings(postings);
//        return batch;
//    }


    public static PostingPlanChange createPostingPlanChange(String planId, String providerAcc, String systemAcc, String merchantAcc) {
        PostingPlanChange postingPlanChange = new PostingPlanChange();
        PostingBatch batch = PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc);
        postingPlanChange.setBatch(batch)
                .setId(planId);
        return postingPlanChange;
    }

// todo delete if not needed
//
//    public static PostingPlanChange createPostingPlanChange(String planId, Long providerAcc, Long systemAcc, Long merchantAcc, Long amount) {
//        PostingPlanChange postingPlanChange = new PostingPlanChange();
//        PostingBatch batch = PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc, amount);
//        postingPlanChange.setBatch(batch)
//                .setId(planId);
//        return postingPlanChange;
//    }
//
//
//    public static PostingPlanChange createPostingPlanChange(String planId,
//                                                            Long providerAcc,
//                                                            Long systemAcc,
//                                                            Long merchantAcc,
//                                                            Long garantAcc,
//                                                            int multiplier) {
//        PostingPlanChange postingPlanChange = new PostingPlanChange();
//        PostingBatch batch = PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc, garantAcc, multiplier);
//        postingPlanChange.setBatch(batch)
//                .setId(planId);
//        return postingPlanChange;
//    }

    public static PostingPlan createPostingPlan(String planId, String providerAcc, String systemAcc, String merchantAcc) {
        PostingPlan postingPlan = new PostingPlan();
        postingPlan.setBatchList(List.of(
                PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc, 1L),
                PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc, 2L)
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
}
