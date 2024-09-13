package com.x.mq.client.job;

import com.x.mq.client.config.MqClientConfig;
import com.x.mq.client.dto.MqClientDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import com.distribute.lock.annotation.JobLock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
@Component
@Slf4j
public class MqClientRetryJob {

    @Autowired
    private MqClientConfig mqClientConfig;

    @Scheduled(cron = "0 0/2 * * * *")
    @JobLock("MqClientRetryJob")//分布式锁实现保证job只能在一个节点执行,可自定义实现，比如结合xxl-job调度
    public void process(){
        List<MqClientDTO> list = mqClientConfig.getSqlSessionTemplate().selectList("com.x.mq.client.mapper.MqClientMapper.selectNeedRetry",mqClientConfig.getMqClientTableName());
        if(CollectionUtils.isEmpty(list)){
            return;
        }
        for(MqClientDTO mqClientDTO : list){
            log.info("retry send message to {},businessKey:{} ",mqClientDTO.getDestination(),mqClientDTO.getBusinessUniqKey());
            Map<String,Object> param = new HashMap<>();
            param.put("tableName",mqClientConfig.getMqClientTableName());
            param.put("uniqKey",mqClientDTO.getBusinessUniqKey());

            try {
                mqClientConfig.getRabbitTemplate().convertAndSend("",mqClientDTO.getDestination(),mqClientDTO.getMessage(),new CorrelationData(mqClientDTO.getBusinessUniqKey()));
            }catch (Exception e){
                log.error(String.format("send message to %s failed,business key:%s",mqClientDTO.getDestination(),mqClientDTO.getBusinessUniqKey()),e);
                try {
                    mqClientConfig.getSqlSessionTemplate().update("com.x.mq.client.mapper.MqClientMapper.updateFail",param);
                }catch (Exception e1){
                    //ignore
                }
            }
        }

    }
}
