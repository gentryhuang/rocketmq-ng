/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.console.config;

import java.io.File;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import static org.apache.rocketmq.client.ClientConfig.SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY;

/**
 * RocketMQ控制台配置类
 * 用于配置RocketMQ的相关属性，如：NameServer地址，是否使用VIP通道等
 */
@Configuration
@ConfigurationProperties(prefix = "rocketmq.config")
public class RMQConfigure {
    private Logger logger = LoggerFactory.getLogger(RMQConfigure.class);

    // 首先尝试使用rocketmq.namesrv.addr 配置的属性的值，如果为空，则使用系统属性或环境变量中的值
    //use rocketmq.namesrv.addr first,if it is empty,than use system proerty or system env
    private volatile String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));

    // 是否使用VIP通道，默认为true
    private volatile String isVIPChannel = System.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "true");

    // 配置存储路径
    private String dataPath;

    // 是否启用仪表盘数据收集
    private boolean enableDashBoardCollect;


    /**
     * 获取NameServer地址
     *
     * @return
     */
    public String getNamesrvAddr() {
        return namesrvAddr;
    }


    /**
     * 设置NameServer地址
     * 如果传入的地址非空，将更新当前实例的NameServer地址，并同时更新系统属性中的相应值。
     *
     * @param namesrvAddr 要设置的NameServer地址
     */
    public void setNamesrvAddr(String namesrvAddr) {
        if (StringUtils.isNotBlank(namesrvAddr)) {
            this.namesrvAddr = namesrvAddr;
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);
            logger.info("setNameSrvAddrByProperty nameSrvAddr={}", namesrvAddr);
        }
    }

    /**
     * 获取存储路径
     *
     * @return
     */
    public String getRocketMqConsoleDataPath() {
        return dataPath;
    }

    /**
     * 获取仪表盘数据收集路径
     *
     * @return 仪表盘数据收集路径字符串
     */
    public String getConsoleCollectData() {
        return dataPath + File.separator + "dashboard";
    }


    /**
     * 设置数据存储路径
     *
     * @param dataPath 要设置的数据路径
     */
    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }


    /**
     * 获取是否使用VIP通道的配置
     *
     * @return 是否使用VIP通道的字符串表示
     */
    public String getIsVIPChannel() {
        return isVIPChannel;
    }


    /**
     * 设置是否使用VIP通道
     * 如果传入的值非空，将更新当前实例是否使用VIP通道的设置，并同时更新系统属性中的相应值。
     *
     * @param isVIPChannel 是否使用VIP通道的字符串表示
     */
    public void setIsVIPChannel(String isVIPChannel) {
        if (StringUtils.isNotBlank(isVIPChannel)) {
            this.isVIPChannel = isVIPChannel;
            System.setProperty(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, isVIPChannel);
            logger.info("setIsVIPChannel isVIPChannel={}", isVIPChannel);
        }
    }

    /**
     * 获取是否启用仪表盘数据收集
     * @return 是否启用的布尔值
     */
    public boolean isEnableDashBoardCollect() {
        return enableDashBoardCollect;
    }

    public void setEnableDashBoardCollect(String enableDashBoardCollect) {
        this.enableDashBoardCollect = Boolean.valueOf(enableDashBoardCollect);
    }
}
