package com.x.mq.client.dto;

import lombok.Data;

import java.util.Date;

/**
 * @Author: chengzhang
 * @Date: 2021-10-8 11:45
 */
@Data
public class MqClientDTO {

    private Long id;

    private String destination;

    private String message;

    private Integer status;

    private Integer retryTimes=0;

    private Date createdTime;

    private Date updatedTime;

    private String businessUniqKey;

    private Long version;

    private String tableName;
}
