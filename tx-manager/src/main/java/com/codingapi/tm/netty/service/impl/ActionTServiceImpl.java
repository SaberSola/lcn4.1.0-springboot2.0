package com.codingapi.tm.netty.service.impl;

import com.codingapi.tm.netty.service.IActionService;
import org.springframework.stereotype.Service;

/**
 * 通知事务回调
 * create by lorne on 2017/11/11
 */
@Service(value = "t")
public class ActionTServiceImpl extends BaseSignalTaskService implements IActionService {


    /**
     *
     * notifyTransactionMsg  p{"a":"t","k":"tm 阻塞的key",{"d":"res 0,1,2"}}
     */

}
