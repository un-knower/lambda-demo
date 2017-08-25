package com.datacenter.lambda.common;

import com.datastax.driver.extras.codecs.enums.EnumNameCodec;

/**
 * Created by zuoc on 2017/7/25.
 */
public class DimensionEnumCodec extends EnumNameCodec<DimensionEnum> {

    public DimensionEnumCodec() {
        super(DimensionEnum.class);
    }
}
