package com.x.mq.client.mapper;

import com.x.mq.client.dto.MqClientDTO;

import java.util.List;

/**
 * @Author: chengzhang
 * @Date: 2021-10-8 11:44
 */
public interface MqClientMapper {

    public int updateSuccess(String uniqKey,String tableName);

    public int updateFail(String uniqKey,String tableName);

    public List<MqClientDTO> selectNeedRetry(String tableName);
}
