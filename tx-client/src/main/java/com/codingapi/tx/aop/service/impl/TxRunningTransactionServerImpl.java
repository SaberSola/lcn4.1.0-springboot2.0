package com.codingapi.tx.aop.service.impl;

import com.codingapi.tx.Constants;
import com.codingapi.tx.aop.bean.TxTransactionInfo;
import com.codingapi.tx.model.TxGroup;
import com.lorne.core.framework.exception.ServiceException;
import com.lorne.core.framework.utils.KidUtils;
import com.codingapi.tx.aop.bean.TxTransactionLocal;
import com.codingapi.tx.datasource.ILCNTransactionControl;
import com.codingapi.tx.framework.task.TaskGroupManager;
import com.codingapi.tx.framework.task.TxTask;
import com.codingapi.tx.netty.service.MQTxManagerService;
import com.codingapi.tx.aop.service.TransactionServer;
import org.aspectj.lang.ProceedingJoinPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * 分布式事务启动参与事务中的业务处理
 * Created by lorne on 2017/6/8.
 */
@Service(value = "txRunningTransactionServer")
public class TxRunningTransactionServerImpl implements TransactionServer {

    @Autowired
    private MQTxManagerService txManagerService;

    @Autowired
    private ILCNTransactionControl transactionControl;

    private Logger logger = LoggerFactory.getLogger(TxRunningTransactionServerImpl.class);

    /**
     *
     * 事务的参与方 ServiceB
     * @param point
     * @param info
     * @return
     * @throws Throwable
     */
    @Override
    public Object execute(final ProceedingJoinPoint point, final TxTransactionInfo info) throws Throwable {

        logger.info("事务参与方...");

        //生成子事务Id
        String kid = KidUtils.generateShortUuid();

        /**
         * 获取事务组Id
         */
        String txGroupId = info.getTxGroupId();  //获取事务组Id
        logger.debug("--->begin running transaction,groupId:" + txGroupId);
        long t1 = System.currentTimeMillis();

        /**
         * 判断是否是同一事务下
         */
        boolean isHasIsGroup =  transactionControl.hasGroup(txGroupId);

        /**
         * 通过Rpc调用后
         */
        TxTransactionLocal txTransactionLocal = new TxTransactionLocal();
        txTransactionLocal.setGroupId(txGroupId); //groupId
        txTransactionLocal.setHasStart(false);     //是否是发起者
        txTransactionLocal.setKid(kid);            //子事务 事务单元Id
        txTransactionLocal.setHasIsGroup(isHasIsGroup); //是否同一个模块被多次请求
        txTransactionLocal.setMaxTimeOut(Constants.txServer.getCompensateMaxWaitTime());
        txTransactionLocal.setMode(info.getMode()); //事务mode lcn 模式
        TxTransactionLocal.setCurrent(txTransactionLocal);

        try {
            /**
             * 执行 业务
             */
            Object res = point.proceed();
            /**
             * 业务执行结束
             */
            //写操作 处理
            if(!txTransactionLocal.isReadOnly()) {

                /**
                 * 获取方法字符串
                 */
                String methodStr = info.getInvocation().getMethodStr();
                /**
                 *将子事务添加到 事务组
                 *同时将子任务的task添加进去
                 */
                TxGroup resTxGroup = txManagerService.addTransactionGroup(txGroupId, kid, isHasIsGroup, methodStr);

                //已经进入过该模块的，不再执行此方法
                if(!isHasIsGroup) {
                    String type = txTransactionLocal.getType();

                    TxTask waitTask = TaskGroupManager.getInstance().getTask(kid, type);

                    //lcn 连接已经开始等待时.
                    while (waitTask != null && !waitTask.isAwait()) {
                        TimeUnit.MILLISECONDS.sleep(1);
                    }

                    if (resTxGroup == null) {

                        //通知业务回滚事务
                        if (waitTask != null) {
                            //修改事务组状态异常
                            waitTask.setState(-1);
                            waitTask.signalTask();
                            throw new ServiceException("update TxGroup error, groupId:" + txGroupId);
                        }
                    }
                }
            }

            return res;
        } catch (Throwable e) {
            // 这里处理以下情况：当 point.proceed() 业务代码中 db事务正常提交，开始等待，后续处理发生异常。
            // 由于没有加入事务组，不会收到通知。这里唤醒并回滚
            if(!isHasIsGroup) {
                String type = txTransactionLocal.getType();
                TxTask waitTask = TaskGroupManager.getInstance().getTask(kid, type);
                // 有一定几率不能唤醒: wait的代码是在另一个线程，有可能线程还没执行到wait，先执行到了这里
                // TODO 要不要 sleep 1毫秒
                logger.warn("wake the waitTask: {}", (waitTask != null && waitTask.isAwait()));
                if (waitTask != null && waitTask.isAwait()) {
                    waitTask.setState(-1);
                    waitTask.signalTask();
                }
            }
            throw e;
        } finally {
            TxTransactionLocal.setCurrent(null);
            long t2 = System.currentTimeMillis();
            logger.debug("<---end running transaction,groupId:" + txGroupId+",execute time:"+(t2-t1));

        }
    }

}
