package com.rbkmoney.shumaich;

import com.rbkmoney.damsel.shumpune.Posting;
import com.rbkmoney.damsel.shumpune.PostingBatch;
import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.damsel.shumpune.PostingPlanChange;

import java.util.ArrayList;
import java.util.List;

public class PostingGenerator {

    public static final long BATCH_ID = 1L;


    public static PostingBatch createBatch(Long providerAcc, Long systemAcc, Long merchantAcc) {
        PostingBatch batch = new PostingBatch();
        batch.setId(BATCH_ID);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(300000)
                .setFromId(providerAcc)
                .setToId(merchantAcc)
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(9000)
                .setFromId(merchantAcc)
                .setToId(systemAcc)
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(6000)
                .setFromId(systemAcc)
                .setToId(providerAcc)
                .setDescription("qwe"));
        batch.setPostings(postings);
        return batch;
    }


    public static PostingBatch createBatch(Long providerAcc, Long systemAcc, Long merchantAcc, Long amount) {
        PostingBatch batch = new PostingBatch();
        batch.setId(BATCH_ID);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(amount)
                .setFromId(providerAcc)
                .setToId(merchantAcc)
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(amount)
                .setFromId(merchantAcc)
                .setToId(systemAcc)
                .setDescription("qwe"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(amount)
                .setFromId(systemAcc)
                .setToId(providerAcc)
                .setDescription("qwe"));
        batch.setPostings(postings);
        return batch;
    }


    public static PostingBatch createBatch(Long firstAcc, Long secAcc, Long thirdAcc, Long fourthAcc, int multiplier) {
        PostingBatch batch = new PostingBatch();
        batch.setId(BATCH_ID);
        ArrayList<Posting> postings = new ArrayList<>();
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(2800 * multiplier)
                .setFromId(firstAcc)
                .setToId(secAcc)
                .setDescription("1->2"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(4000 * multiplier)
                .setFromId(firstAcc)
                .setToId(thirdAcc)
                .setDescription("1->3"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(80000 * multiplier)
                .setFromId(fourthAcc)
                .setToId(firstAcc)
                .setDescription("4->1"));
        postings.add(new Posting()
                .setCurrencySymCode("RUB")
                .setAmount(1760 * multiplier)
                .setFromId(secAcc)
                .setToId(fourthAcc)
                .setDescription("4->2"));
        batch.setPostings(postings);
        return batch;
    }


    public static PostingPlanChange createPostingPlanChange(String planId, Long providerAcc, Long systemAcc, Long merchantAcc) {
        PostingPlanChange postingPlanChange = new PostingPlanChange();
        PostingBatch batch = PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc);
        postingPlanChange.setBatch(batch)
                .setId(planId);
        return postingPlanChange;
    }


    public static PostingPlanChange createPostingPlanChange(String planId, Long providerAcc, Long systemAcc, Long merchantAcc, Long amount) {
        PostingPlanChange postingPlanChange = new PostingPlanChange();
        PostingBatch batch = PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc, amount);
        postingPlanChange.setBatch(batch)
                .setId(planId);
        return postingPlanChange;
    }


    public static PostingPlanChange createPostingPlanChange(String planId,
                                                            Long providerAcc,
                                                            Long systemAcc,
                                                            Long merchantAcc,
                                                            Long garantAcc,
                                                            int multiplier) {
        PostingPlanChange postingPlanChange = new PostingPlanChange();
        PostingBatch batch = PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc, garantAcc, multiplier);
        postingPlanChange.setBatch(batch)
                .setId(planId);
        return postingPlanChange;
    }

    public static PostingPlan createPostingPlan(String planId, Long providerAcc, Long systemAcc, Long merchantAcc) {
        PostingPlan postingPlan = new PostingPlan();
        postingPlan.setBatchList(List.of(
                PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc),
                PostingGenerator.createBatch(providerAcc, systemAcc, merchantAcc)
        ));
        postingPlan.setId(planId);
        return postingPlan;
    }
}
