package com.codingapi.tx.aop.service.impl;

import com.codingapi.tx.Constants;
import com.codingapi.tx.aop.bean.TxCompensateLocal;
import com.codingapi.tx.aop.bean.TxTransactionInfo;
import com.codingapi.tx.aop.bean.TxTransactionLocal;
import com.codingapi.tx.aop.service.TransactionServer;
import com.codingapi.tx.framework.task.TaskGroupManager;
import com.codingapi.tx.framework.task.TaskState;
import com.codingapi.tx.framework.task.TxTask;
import com.codingapi.tx.netty.service.MQTxManagerService;
import com.lorne.core.framework.utils.KidUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 分布式事务启动开始时的业务处理
 * Created by lorne on 2017/6/8.
 */
@Service(value = "txStartTransactionServer")
public class TxStartTransactionServerImpl implements TransactionServer {


    private Logger logger = LoggerFactory.getLogger(TxStartTransactionServerImpl.class);


    @Autowired
    protected MQTxManagerService txManagerService;

    /**
     *
     * 事务的发起方
     *
     * @param point
     * @param info
     * @return
     * @throws Throwable
     */

    @Override
    public Object execute(ProceedingJoinPoint point,final TxTransactionInfo info) throws Throwable {
        //分布式事务开始执行

        logger.info("事务发起方...");

        logger.debug("--->分布式事务开始执行 begin start transaction");

        final long start = System.currentTimeMillis();

        int state = 0;

        //新建事务组id
        final String groupId = TxCompensateLocal.current()==null?KidUtils.generateShortUuid():TxCompensateLocal.current().getGroupId();

        //创建事务组
        logger.debug("创建事务组并发送消息");
        //创建事务组
        txManagerService.createTransactionGroup(groupId);

        TxTransactionLocal txTransactionLocal = new TxTransactionLocal();
        txTransactionLocal.setGroupId(groupId); //事务组Id
        txTransactionLocal.setHasStart(true);   //代表事务的发起着
        txTransactionLocal.setMaxTimeOut(Constants.txServer.getCompensateMaxWaitTime()); //设置超时时间
        txTransactionLocal.setMode(info.getTxTransaction().mode()); //事务模式
        txTransactionLocal.setReadOnly(info.getTxTransaction().readOnly()); //是否只读
        TxTransactionLocal.setCurrent(txTransactionLocal); //设置ThreadLocal

        try {
            //执行具体方法 调用远程方法的同时 进入下一个切面 txRunningTransactionServer
            Object obj = point.proceed();
            /**
             * 所有的chilrd 结束后 会执行
             */
            state = 1;
            return obj;  //return 后会事务会立即执行操作 并且阻塞
        } catch (Throwable e) {
            state = rollbackException(info,e);
            throw e;
        } finally {
            /**
             * 业务已经全部执行结束
             */
            final String type = txTransactionLocal.getType();
            /**
             * 发送关闭事务组的消息 事务组Id state = 1
             */
            int rs = txManagerService.closeTransactionGroup(groupId, state); // 获取res 0 ，1 ，2

            int lastState = rs==-1?0:state;

            int executeConnectionError = 0;

            //控制本地事务的数据提交
            final TxTask waitTask = TaskGroupManager.getInstance().getTask(groupId, type); // 这里是
            if(waitTask!=null){
                waitTask.setState(lastState);
                waitTask.signalTask();

                while (!waitTask.isRemove()){
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if(waitTask.getState()== TaskState.connectionError.getCode()){
                    //本地执行失败.
                    executeConnectionError = 1;

                    lastState = 0;
                }
            }

            final TxCompensateLocal compensateLocal =  TxCompensateLocal.current();

            if (compensateLocal == null) {
                //本地事务执行失败会会进行补偿机制
                long end = System.currentTimeMillis();
                long time = end - start;
                if ((executeConnectionError == 1&&rs == 1)||(lastState == 1 && rs == 0)) {
                    logger.debug("记录补偿日志");
                    txManagerService.sendCompensateMsg(groupId, time, info,executeConnectionError);
                }
            }else{
                if(rs==1){
                    lastState = 1;
                }else{
                    lastState = 0;
                }
            }

            TxTransactionLocal.setCurrent(null);
            logger.debug("<---分布式事务 end start transaction");
            logger.debug("start transaction over, res -> groupId:" + groupId + ", now state:" + (lastState == 1 ? "commit" : "rollback"));

        }
    }


    private int  rollbackException(TxTransactionInfo info,Throwable throwable){

        //spring 事务机制默认回滚异常.
        if(RuntimeException.class.isAssignableFrom(throwable.getClass())){
            return 0;
        }

        if(Error.class.isAssignableFrom(throwable.getClass())){
            return 0;
        }

        //回滚异常检测.
        for(Class<? extends Throwable> rollbackFor:info.getTxTransaction().rollbackFor()){

            //存在关系
            if(rollbackFor.isAssignableFrom(throwable.getClass())){
                return 0;
            }

        }

        //不回滚异常检测.
        for(Class<? extends Throwable> rollbackFor:info.getTxTransaction().noRollbackFor()){

            //存在关系
            if(rollbackFor.isAssignableFrom(throwable.getClass())){
                return 1;
            }
        }
        return 1;
    }
}
