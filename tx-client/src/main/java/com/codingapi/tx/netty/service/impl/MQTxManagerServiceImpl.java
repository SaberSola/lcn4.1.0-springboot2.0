package com.codingapi.tx.netty.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.codingapi.tx.aop.bean.TxTransactionInfo;
import com.codingapi.tx.compensate.model.CompensateInfo;
import com.codingapi.tx.compensate.service.CompensateService;
import com.codingapi.tx.framework.utils.SerializerUtils;
import com.codingapi.tx.framework.utils.SocketManager;
import com.codingapi.tx.listener.service.ModelNameService;
import com.codingapi.tx.model.Request;
import com.codingapi.tx.model.TxGroup;
import com.codingapi.tx.netty.service.MQTxManagerFeginService;
import com.codingapi.tx.netty.service.MQTxManagerService;
import com.lorne.core.framework.utils.encode.Base64Utils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by lorne on 2017/6/30.
 */
@Service
public class MQTxManagerServiceImpl implements MQTxManagerService {


    @Autowired
    private ModelNameService modelNameService;

    @Autowired
    private CompensateService compensateService;

    @Autowired
    private MQTxManagerFeginService mqTxManagerFeginService;

    @Override
    public void createTransactionGroup(String groupId) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("g", groupId);
        Request request = new Request("cg", jsonObject.toString());
        SocketManager.getInstance().sendMsg(request);
    }

    /**
     *
     * @param groupId   事务组id
     * @param taskId    任务Id
     * @param isGroup   是否合并到事务组 true合并 false不合并
     * @param methodStr   方法参数列表
     * atg 指令标识 事务调用方 ---> txManger
     * g:事务组id
     * t:唤醒taskId
     * ms: 切面方法名称
     * s:事务是事务组 0 否 1 是
     * @return
     */
    @Override
    public TxGroup addTransactionGroup(String groupId, String taskId, boolean isGroup, String methodStr) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("g", groupId);
        jsonObject.put("t", taskId);
        jsonObject.put("ms", methodStr);
        jsonObject.put("s", isGroup ? 1 : 0);
        Request request = new Request("atg", jsonObject.toString());
        String json =  SocketManager.getInstance().sendMsg(request);
        return TxGroup.parser(json);
    }

    @Override
    public int closeTransactionGroup(final String groupId, final int state) {
        /**
         * 发送消息
         * 关闭事务组的消息
         */
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("g", groupId);
        jsonObject.put("s", state);
        Request request = new Request("ctg", jsonObject.toString());
        String json =  SocketManager.getInstance().sendMsg(request); // 获取res 0 ， 1 ，2
        try {
            return Integer.parseInt(json);
        }catch (Exception e){
            return 0;
        }
    }


    @Override
    public void uploadModelInfo() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("m", modelNameService.getModelName());
        jsonObject.put("i", modelNameService.getIpAddress());
        jsonObject.put("u", modelNameService.getUniqueKey());
        Request request = new Request("umi", jsonObject.toString());
        String json = SocketManager.getInstance().sendMsg(request);
    }

    @Override
    public int cleanNotifyTransaction(String groupId, String taskId) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("g", groupId);
        jsonObject.put("t", taskId);
        Request request = new Request("ckg", jsonObject.toString());
        String json =  SocketManager.getInstance().sendMsg(request);
        try {
            return Integer.parseInt(json);
        }catch (Exception e){
            return -2;
        }
    }


    @Override
    public int cleanNotifyTransactionHttp(String groupId, String waitTaskId) {
        String clearRes = mqTxManagerFeginService.cleanNotifyTransactionHttp(groupId, waitTaskId);
        if(clearRes==null){
            return -1;
        }
        return  clearRes.contains("true") ? 1 : 0;
    }


    @Override
    public String httpGetServer() {
        return mqTxManagerFeginService.getServer();
    }

    @Override
    public void sendCompensateMsg(String groupId, long time, TxTransactionInfo info,int startError) {

        String modelName = modelNameService.getModelName();
        String uniqueKey = modelNameService.getUniqueKey();
        String address = modelNameService.getIpAddress();


        byte[] serializers =  SerializerUtils.serializeTransactionInvocation(info.getInvocation());
        String data = Base64Utils.encode(serializers);

        String className = info.getInvocation().getTargetClazz().getName();
        String methodStr = info.getInvocation().getMethodStr();
        long currentTime = System.currentTimeMillis();


        CompensateInfo compensateInfo = new CompensateInfo(currentTime, modelName, uniqueKey, data, methodStr, className, groupId, address, time,startError);

        String json = mqTxManagerFeginService.sendCompensateMsg(currentTime, groupId, modelName, address, uniqueKey, className, methodStr, data, time,startError);

        compensateInfo.setResJson(json);

        //记录本地日志
        compensateService.saveLocal(compensateInfo);

    }
}
