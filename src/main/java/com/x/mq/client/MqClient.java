package com.x.mq.client;

import com.x.mq.client.config.MqClientConfig;
import com.x.mq.client.util.SpringContextUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: chengzhang
 * @Date: 2020-4-30 17:03
 */
@Slf4j
@Component
public class MqClient {

    @Autowired
    private MqClientConfig mqClientConfig;

    /*public MqClient(MqClientConfig mqClientConfig){
        this.mqClientConfig = mqClientConfig;
    }*/

    public void send(String queueName,String message,String businessUniqKey){
        if(TransactionSynchronizationManager.isSynchronizationActive()){
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            String insertSql = String.format(MqClientConfig.INSERT_SQL, mqClientConfig.getMqClientTableName(),queueName,message,0,0,simpleDateFormat.format(new Date()),simpleDateFormat.format(new Date()),businessUniqKey);
            try {
                PreparedStatement preparedStatement = DataSourceUtils.getConnection(mqClientConfig.getDataSource()).prepareStatement(insertSql);
                preparedStatement.executeUpdate();
            }catch (Exception e){
                log.error("insert mq client table error",e);
                throw new RuntimeException(e);
            }
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
                @Override
                public void afterCommit() {
                    ThreadPoolTaskExecutor executor = (ThreadPoolTaskExecutor)SpringContextUtil.getBean("mqThreadTaskPoolExecutor");
                    executor.execute(new Runnable() {
                        @Override
                        public void run() {
                            doSend(queueName,message,businessUniqKey);
                        }
                    });
                }
            });
        }else{
            //直接发送
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            String insertSql = String.format(MqClientConfig.INSERT_SQL, mqClientConfig.getMqClientTableName(),queueName,message,0,0,simpleDateFormat.format(new Date()),simpleDateFormat.format(new Date()),businessUniqKey);
            PreparedStatement preparedStatement = null;
            Connection connection = null;
            try {
                connection = DataSourceUtils.getConnection(mqClientConfig.getDataSource());
                preparedStatement = connection.prepareStatement(insertSql);
                preparedStatement.execute();
            }catch (Exception e){
                log.error("insert mq client table error",e);
                throw new RuntimeException(e);
            }finally {
                if(null != preparedStatement){
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        //ignore
                    }
                }
                if(null != connection){
                    DataSourceUtils.releaseConnection(connection,mqClientConfig.getDataSource());
                }
            }
            doSend(queueName,message,businessUniqKey);
        }
    }

    private void doSend(String queueName, String message,String businessKey) {
        Map<String,Object> param = new HashMap<>();
        param.put("tableName",mqClientConfig.getMqClientTableName());
        param.put("uniqKey",businessKey);
        try {
            mqClientConfig.getRabbitTemplate().convertAndSend("",queueName,message,new CorrelationData(businessKey));
        }catch (Exception e){
            log.error(String.format("send mq to %s failed,businessKey:%s",queueName,businessKey),e);
            try {
                mqClientConfig.getSqlSessionTemplate().update("com.x.mq.client.mapper.MqClientMapper.updateFail",param);
            }catch (Exception e1){
                log.error(String.format("update mq client table failed,businessKey:%s",businessKey),e1);
            }
        }
    }

}
