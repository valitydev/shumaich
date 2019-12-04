package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.shumaich.domain.RequestLog;
import org.springframework.core.convert.converter.Converter;

public class PostingPlanToRequestLogConverter implements Converter<PostingPlan, RequestLog> {

    @Override
    public RequestLog convert(PostingPlan source) {
        return null;
    }
}
