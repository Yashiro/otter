package com.alibaba.otter.node.etl.common.db.dialect;

/**
 * @author Andy Zhou
 * @date 2017/8/11
 */
import com.alibaba.otter.shared.etl.model.EventData;

import java.util.List;

public interface NoSqlTemplate {
    /**
     * 批量执行dml数据操作，增，删，改
     *
     * @param events
     * @return 执行失败的记录集合返回,失败原因消息保存在exeResult字段中
     */
    public List<EventData> batchEventDatas(List<EventData> events);

    /**
     * 插入行数据
     *
     * @param event
     * @return 记录返回,失败原因消息保存在exeResult字段中
     */
    public EventData insertEventData(EventData event);

    /**
     * 更新行数句
     *
     * @param event
     * @return 记录返回,失败原因消息保存在exeResult字段中
     */
    public EventData updateEventData(EventData event);

    /**
     * 删除记录
     *
     * @param event
     * @return 记录返回,失败原因消息保存在exeResult字段中
     */
    public EventData deleteEventData(EventData event);

    /**
     * 建立表
     *
     * @param event
     * @return
     */
    public EventData createTable(EventData event);

    /**
     * 修改表
     *
     * @param event
     * @return
     */
    public EventData alterTable(EventData event);

    /**
     * 删除表
     *
     * @param event
     * @return
     */
    public EventData eraseTable(EventData event);

    /**
     * 清空表
     *
     * @param event
     * @return
     */
    public EventData truncateTable(EventData event);

    /**
     * 改名表
     *
     * @param event
     * @return
     */
    public EventData renameTable(EventData event);
}
