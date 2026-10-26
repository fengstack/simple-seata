package io.spring2go.seata.rm.datasource.undo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

/**
 * Created by william on May, 2020
 */
public class JSONBasedUndoLogParser implements UndoLogParser {
    static {
        // WriteDateUseDateFormat 的默认格式只到秒，毫秒会被静默截断。
        // 回滚时的防脏写校验要求镜像与数据库当前值完全对上，日期必须保留毫秒精度
        JSON.DEFFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    }

    @Override
    public String encode(BranchUndoLog branchUndoLog) {
        return JSON.toJSONString(branchUndoLog, SerializerFeature.WriteDateUseDateFormat);
    }

    @Override
    public BranchUndoLog decode(String text) {
        return JSON.parseObject(text, BranchUndoLog.class);
    }
}
