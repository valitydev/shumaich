package com.rbkmoney.shumaich.utils;

import com.rbkmoney.shumaich.domain.Posting;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.codec.digest.MurmurHash2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HashUtils {

    public static long computeHash(List<Posting> postings) {
        return MurmurHash2.hash64(String.join("_", convertPostings(postings)));
    }

    public static boolean areHashesEqual(List<Posting> postings, Long batchHash) {
        return computeHash(postings) == batchHash;
    }

    private static List<String> convertPostings(List<Posting> postings) {
        ArrayList<Posting> postingsCopy = new ArrayList<>(postings);
        postingsCopy.sort(Comparator.comparing(Posting::getAmount)
                .thenComparing((Posting posting) -> posting.getFromAccount().getId())
                .thenComparing((Posting posting) -> posting.getToAccount().getId()));

        List<String> propsToHash = new ArrayList<>();
        for (Posting posting : postingsCopy) {
            propsToHash.add(posting.getAmount().toString());
            propsToHash.add(Long.toString(posting.getFromAccount().getId()));
            propsToHash.add(Long.toString(posting.getToAccount().getId()));
        }
        return propsToHash;
    }
}
