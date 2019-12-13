package com.rbkmoney.shumaich.converter;

import com.rbkmoney.damsel.shumpune.PostingPlanChange;
import com.rbkmoney.shumaich.domain.RequestLog;
import org.springframework.core.convert.converter.Converter;

public class PostingPlanChangeToRequestLogConverter implements Converter<PostingPlanChange, RequestLog> {

    @Override
    public RequestLog convert(PostingPlanChange source) {
        return null;
    }
}
