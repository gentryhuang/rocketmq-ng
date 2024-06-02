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

package org.apache.rocketmq.console.service;

import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.console.model.ConsumerGroupRollBackStat;
import org.apache.rocketmq.console.model.GroupConsumeInfo;
import org.apache.rocketmq.console.model.TopicConsumerInfo;
import org.apache.rocketmq.console.model.request.ConsumerConfigInfo;
import org.apache.rocketmq.console.model.request.DeleteSubGroupRequest;
import org.apache.rocketmq.console.model.request.ResetOffsetRequest;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 消费者服务接口，提供了一系列查询和管理消费者组以及消费状态的方法。
 */
public interface ConsumerService {
    /**
     * 查询消费者组列表。
     *
     * @return 返回消费者组信息列表。
     */
    List<GroupConsumeInfo> queryGroupList();

    /**
     * 根据消费者组查询消费者组信息。
     *
     * @param consumerGroup 消费者组ID。
     * @return 返回指定消费者组的信息。
     */
    GroupConsumeInfo queryGroup(String consumerGroup);

    /**
     * 查询指定消费组订阅的主题的 消费消费详情
     *
     * @param groupName 消费者组名。
     * @return 返回指定消费者组名的消费状态信息列表。
     */
    List<TopicConsumerInfo> queryConsumeStatsListByGroupName(String groupName);

    /**
     * 根据主题和组名查询消费状态信息列表。
     *
     * @param topic     主题名称。
     * @param groupName 消费者组名。
     * @return 返回指定主题和消费者组名的消费状态信息列表。
     */
    List<TopicConsumerInfo> queryConsumeStatsList(String topic, String groupName);

    /**
     * 根据主题名称查询消费状态信息列表。
     *
     * @param topic 主题名称。
     * @return 返回映射，键为消费者组名，值为对应的消费状态信息。
     */
    Map<String, TopicConsumerInfo> queryConsumeStatsListByTopicName(String topic);

    /**
     * 重置消费位点。
     *
     * @param resetOffsetRequest 重置消费位点的请求信息。
     * @return 返回映射，键为消费者组，值为重置状态信息。
     */
    Map<String, ConsumerGroupRollBackStat> resetOffset(ResetOffsetRequest resetOffsetRequest);

    /**
     * 查询订阅组的配置信息。
     *
     * @param group 订阅组ID。
     * @return 返回订阅组的配置信息列表。
     */
    List<ConsumerConfigInfo> examineSubscriptionGroupConfig(String group);

    /**
     * 从指定的Broker删除指定的消费组的订阅配置信息
     *
     * @param deleteSubGroupRequest 删除订阅组的请求信息。
     * @return 返回布尔值，表示删除操作是否成功。
     */
    boolean deleteSubGroup(DeleteSubGroupRequest deleteSubGroupRequest);

    /**
     * 创建或更新订阅组的配置信息。
     *
     * @param consumerConfigInfo 订阅组的配置信息。
     * @return 返回布尔值，表示创建或更新操作是否成功。
     */
    boolean createAndUpdateSubscriptionGroupConfig(ConsumerConfigInfo consumerConfigInfo);

    /**
     * 根据订阅组查询Broker名称集合。
     *
     * @param group 订阅组ID。
     * @return 返回Broker名称的集合。
     */
    Set<String> fetchBrokerNameSetBySubscriptionGroup(String group);

    /**
     * 获取消费组下的消费者连接通道列表 及 订阅信息集合
     *
     * @param consumerGroup 消费者组ID。
     * @return 返回消费者连接信息。
     */
    ConsumerConnection getConsumerConnection(String consumerGroup);

    /**
     * 查询消费者运行时信息。
     *
     * @param consumerGroup 消费者组ID。
     * @param clientId      客户端ID。
     * @param jstack        是否获取堆栈信息。
     * @return 返回消费者运行时信息。
     */
    ConsumerRunningInfo getConsumerRunningInfo(String consumerGroup, String clientId, boolean jstack);
}
