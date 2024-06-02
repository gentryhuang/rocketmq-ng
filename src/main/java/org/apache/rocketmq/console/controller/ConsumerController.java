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
package org.apache.rocketmq.console.controller;

import com.google.common.base.Preconditions;

import javax.annotation.Resource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.console.model.ConnectionInfo;
import org.apache.rocketmq.console.model.request.ConsumerConfigInfo;
import org.apache.rocketmq.console.model.request.DeleteSubGroupRequest;
import org.apache.rocketmq.console.model.request.ResetOffsetRequest;
import org.apache.rocketmq.console.service.ConsumerService;
import org.apache.rocketmq.console.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/consumer")
public class ConsumerController {
    private Logger logger = LoggerFactory.getLogger(ConsumerController.class);

    @Resource
    private ConsumerService consumerService;

    /**
     * 查询消费者组列表及其消费信息
     *
     * @return
     */
    @RequestMapping(value = "/groupList.query")
    @ResponseBody
    public Object list() {
        return consumerService.queryGroupList();
    }

    @RequestMapping(value = "/group.query")
    @ResponseBody
    public Object groupQuery(@RequestParam String consumerGroup) {
        return consumerService.queryGroup(consumerGroup);
    }

    /**
     * 对指定的 group@topic 重置消费位点
     *
     * @param resetOffsetRequest
     * @return
     */
    @RequestMapping(value = "/resetOffset.do", method = {RequestMethod.POST})
    @ResponseBody
    public Object resetOffset(@RequestBody ResetOffsetRequest resetOffsetRequest) {
        logger.info("op=look resetOffsetRequest={}", JsonUtil.obj2String(resetOffsetRequest));
        return consumerService.resetOffset(resetOffsetRequest);
    }

    /**
     * 查询消费组订阅配置信息
     *
     * @param consumerGroup
     * @return
     */
    @RequestMapping(value = "/examineSubscriptionGroupConfig.query")
    @ResponseBody
    public Object examineSubscriptionGroupConfig(@RequestParam String consumerGroup) {
        return consumerService.examineSubscriptionGroupConfig(consumerGroup);
    }

    /**
     * 从指定的Broker删除指定的消费组的订阅配置信息
     *
     * @param deleteSubGroupRequest
     * @return
     */
    @RequestMapping(value = "/deleteSubGroup.do", method = {RequestMethod.POST})
    @ResponseBody
    public Object deleteSubGroup(@RequestBody DeleteSubGroupRequest deleteSubGroupRequest) {
        return consumerService.deleteSubGroup(deleteSubGroupRequest);
    }


    /**
     * 创建或更新消费组配置信息
     *
     * @param consumerConfigInfo
     * @return
     */
    @RequestMapping(value = "/createOrUpdate.do", method = {RequestMethod.POST})
    @ResponseBody
    public Object consumerCreateOrUpdateRequest(@RequestBody ConsumerConfigInfo consumerConfigInfo) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(consumerConfigInfo.getBrokerNameList()) || CollectionUtils.isNotEmpty(consumerConfigInfo.getClusterNameList()),
                "clusterName or brokerName can not be all blank");
        return consumerService.createAndUpdateSubscriptionGroupConfig(consumerConfigInfo);
    }


    @RequestMapping(value = "/fetchBrokerNameList.query", method = {RequestMethod.GET})
    @ResponseBody
    public Object fetchBrokerNameList(@RequestParam String consumerGroup) {
        return consumerService.fetchBrokerNameSetBySubscriptionGroup(consumerGroup);
    }


    /**
     * 查询指定消费组订阅的主题的 消费消费详情
     *
     * @param consumerGroup 消费组
     * @return
     */
    @RequestMapping(value = "/queryTopicByConsumer.query")
    @ResponseBody
    public Object queryConsumerByTopic(@RequestParam String consumerGroup) {
        // 查询指定消费组订阅的主题的 消费消费详情
        return consumerService.queryConsumeStatsListByGroupName(consumerGroup);
    }

    /**
     * 获取消费组下的消费者连接通道列表 及 订阅信息集合
     * 1 从 NameServer 获取 consumerGroup形式 主题的路由信息
     * 2 从路由信息中获取一个 Broker ，然后从该Broker获取指定消费组下的消费者连接通道列表 及 订阅信息集合
     *
     * @param consumerGroup 消费者组ID。
     * @return
     */
    @RequestMapping(value = "/consumerConnection.query")
    @ResponseBody
    public Object consumerConnection(@RequestParam(required = false) String consumerGroup) {
        // 消费者连接通道列表 及 订阅信息集合
        ConsumerConnection consumerConnection = consumerService.getConsumerConnection(consumerGroup);

        // 设置消费者连接到Broker的连接信息
        consumerConnection.setConnectionSet(ConnectionInfo.buildConnectionInfoHashSet(consumerConnection.getConnectionSet()));
        return consumerConnection;
    }

    @RequestMapping(value = "/consumerRunningInfo.query")
    @ResponseBody
    public Object getConsumerRunningInfo(@RequestParam String consumerGroup, @RequestParam String clientId,
                                         @RequestParam boolean jstack) {
        return consumerService.getConsumerRunningInfo(consumerGroup, clientId, jstack);
    }
}
