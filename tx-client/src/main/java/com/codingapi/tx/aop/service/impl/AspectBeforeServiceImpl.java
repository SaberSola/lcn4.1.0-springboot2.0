package com.codingapi.tx.aop.service.impl;

import com.codingapi.tx.annotation.TxTransaction;
import com.codingapi.tx.annotation.TxTransactionMode;
import com.codingapi.tx.aop.bean.TxTransactionInfo;
import com.codingapi.tx.aop.bean.TxTransactionLocal;
import com.codingapi.tx.aop.service.AspectBeforeService;
import com.codingapi.tx.aop.service.TransactionServer;
import com.codingapi.tx.aop.service.TransactionServerFactoryService;
import com.codingapi.tx.model.TransactionInvocation;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.Method;

/**
 * Created by lorne on 2017/7/1.
 */
@Service
public class AspectBeforeServiceImpl implements AspectBeforeService {

    @Autowired
    private TransactionServerFactoryService transactionServerFactoryService;


    private Logger logger = LoggerFactory.getLogger(AspectBeforeServiceImpl.class);

    @Override
    public Object around(String groupId, ProceedingJoinPoint point, String mode) throws Throwable {

        MethodSignature signature = (MethodSignature) point.getSignature(); //切点获取方法签名
        Method method = signature.getMethod(); //签名获取方法
        Class<?> clazz = point.getTarget().getClass();// 获取执行器的类
        Object[] args = point.getArgs(); //获取参数
        Method thisMethod = clazz.getMethod(method.getName(), method.getParameterTypes()); //类中获取方法

        TxTransaction transaction = thisMethod.getAnnotation(TxTransaction.class);

        //事务发起者 txTransactionLocal 为null
        TxTransactionLocal txTransactionLocal = TxTransactionLocal.current();

        logger.debug("around--> groupId-> " +groupId+",txTransactionLocal->"+txTransactionLocal);

        TransactionInvocation invocation = new TransactionInvocation(clazz, thisMethod.getName(), thisMethod.toString(), args, method.getParameterTypes());

        TxTransactionInfo info = new TxTransactionInfo(transaction,txTransactionLocal,invocation,groupId);
        try {
            info.setMode(TxTransactionMode.valueOf(mode));
        } catch (Exception e) {
            info.setMode(TxTransactionMode.TX_MODE_LCN);
        }
        /**
         * 根据事务信息选择对应的server start running Server
         */
        TransactionServer server = transactionServerFactoryService.createTransactionServer(info);

        return server.execute(point, info);
    }
}
