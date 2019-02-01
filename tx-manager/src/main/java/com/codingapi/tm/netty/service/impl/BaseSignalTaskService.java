package com.codingapi.tm.netty.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.lorne.core.framework.utils.task.ConditionUtils;
import com.lorne.core.framework.utils.task.IBack;
import com.lorne.core.framework.utils.task.Task;

/**
 * create by lorne on 2017/11/13
 */
public class BaseSignalTaskService {


    public String execute(String channelAddress, String key, JSONObject params) {

        //p{"a":"t","k":"tm 阻塞的key",{"d":"res 0,1,2"}}
        String res = "";
        final String data = params.getString("d");
        Task task = ConditionUtils.getInstance().getTask(key);
        if (task != null) {
            task.setBack(new IBack() {
                @Override
                public Object doing(Object... objs) throws Throwable {
                    return data;
                }
            });
            task.signalTask(); //唤醒tm 阻塞的task
        }
        return res;
    }
}
