package com.codingapi.tx.control.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.codingapi.tx.control.service.IActionService;
import com.codingapi.tx.control.service.TransactionControlService;
import com.codingapi.tx.framework.utils.SocketUtils;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

/**
 * create by lorne on 2017/11/11
 */
@Service
public class TransactionControlServiceImpl implements TransactionControlService{

    private Logger logger = LoggerFactory.getLogger(TransactionControlServiceImpl.class);


    @Autowired
    private ApplicationContext spring;



    @Override
    public void notifyTransactionMsg(ChannelHandlerContext ctx,JSONObject resObj, String json) {


        String action = resObj.getString("a");
        String key = resObj.getString("k");  //key 是tm 阻塞的key

        IActionService actionService = spring.getBean(action, IActionService.class);

        String res = actionService.execute(resObj, json);


        JSONObject data = new JSONObject();
        data.put("k", key);    // key 是tm 阻塞的key
        data.put("a", action); // a 是 t 就是事务

        JSONObject params = new JSONObject();
        params.put("d", res);
        data.put("p", params); // p{"a":"t","k":"tm 阻塞的key",{"d":"res 0,1,2"}}

        SocketUtils.sendMsg(ctx, data.toString()); // 发送给txManger

        logger.info("send notify data ->" + data.toString());
    }
}
