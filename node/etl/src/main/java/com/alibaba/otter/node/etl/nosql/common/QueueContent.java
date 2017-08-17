package com.alibaba.otter.node.etl.nosql.common;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.util.Date;
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

    @JSONField(ordinal = 4)
    private Date currentTime;

}