package com.x.mq.client.config;

import lombok.Data;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Data
public class MqClientConfig implements InitializingBean {

    public static final String INSERT_SQL="insert into %s(destination,message,status,retry_times,created_time,updated_time,business_uniq_key)values('%s','%s',%d,%d,'%s','%s','%s')";
    public static final String UPDATE_SUCCESS_SQL="update %s set status=1 where business_uniq_key=%s";
    public static final String UPDATE_FAILE_SQL="update %s set status=2,retry_times=retry_times+1 where business_uniq_key=%s";
    public static final String SELECT_BY_UK_SQL="select * from %s where business_uniq_key=%s";

    private String mqClientTableName;

    private RabbitTemplate rabbitTemplate;

    private SqlSessionTemplate sqlSessionTemplate;

    private DataSource dataSource;

    public MqClientConfig(String mqClientTableName, RabbitTemplate rabbitTemplate, DataSource dataSource){
        this.mqClientTableName = mqClientTableName;
        this.rabbitTemplate = rabbitTemplate;
        this.dataSource = dataSource;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
        bean.setMapperLocations(new PathMatchingResourcePatternResolver().getResources("classpath:mqMapper/*.xml"));
        bean.setConfigLocation(new PathMatchingResourcePatternResolver().getResource("classpath:mybatis-config.xml"));
        bean.setDataSource(dataSource);
        this.sqlSessionTemplate = new SqlSessionTemplate(bean.getObject());
        this.rabbitTemplate.setConfirmCallback(new RabbitTemplate.ConfirmCallback() {
            @Override
            public void confirm(CorrelationData correlationData, boolean ack, String cause) {
                String businessUK = correlationData.getId();
                Map<String,Object> param = new HashMap<>();
                param.put("tableName",mqClientTableName);
                param.put("uniqKey",businessUK);
                try {
                    if(ack){
                        sqlSessionTemplate.update("com.x.mq.client.mapper.MqClientMapper.updateSuccess",param);
                    }else {
                        sqlSessionTemplate.update("com.x.mq.client.mapper.MqClientMapper.updateFail",param);
                    }
                }catch (Exception e){
                    //ignore database error wait retry
                }
            }
        });
    }
}
