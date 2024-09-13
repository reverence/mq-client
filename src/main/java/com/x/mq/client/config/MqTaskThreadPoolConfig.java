package com.x.mq.client.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author: chengzhang
 * @Date: 2021-10-2 16:07
 */
@Configuration
public class MqTaskThreadPoolConfig {

    @Value("${mq.thread.pool.core.size:5}")
    private Integer mqTaskThreadPoolCoreSize;
    @Value("${mq.thread.pool.keepalive.seconds:60}")
    private Integer mqTaskThreadKeepAliveSeconds;
    @Value("${mq.thread.pool.queue.size:1000}")
    private Integer mqTaskThreadPoolQueueCapacity;
    @Value("${mq.thread.pool.max.size:10}")
    private Integer mqTaskThreadPoolMaxSize;

    @Bean("mqThreadTaskPoolExecutor")
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor pool = new ThreadPoolTaskExecutor();
        // 线程池维护线程的最少数量
        pool.setCorePoolSize(mqTaskThreadPoolCoreSize);
        pool.setKeepAliveSeconds(mqTaskThreadKeepAliveSeconds);
        pool.setQueueCapacity(mqTaskThreadPoolQueueCapacity);
        pool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // 线程池维护线程的最大数量
        pool.setMaxPoolSize(mqTaskThreadPoolMaxSize);
        // 当调度器shutdown被调用时等待当前被调度的任务完成
        pool.setWaitForTasksToCompleteOnShutdown(true);
        return pool;
    }
}
