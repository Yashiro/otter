package com.alibaba.otter.node.etl.common.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.collect.Maps;
import lombok.Data;

import java.util.Map;

/**
 * @author Andy Zhou
 * @date 2017/8/15
 */
@Data
public class QueueContent {

    @JSONField(ordinal = 1)
    private String type;

    @JSONField(ordinal = 2)
    private String id;

    @JSONField(ordinal = 3)
    private Map<String, String> columns;

}
