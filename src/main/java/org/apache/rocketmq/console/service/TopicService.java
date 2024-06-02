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

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.console.model.request.SendTopicMessageRequest;
import org.apache.rocketmq.console.model.request.TopicConfigInfo;

import java.util.List;

/**
 * Topic 服务
 */
public interface TopicService {
    /**
     * 从 NameServer 获取 Topic 列表
     *
     * @return
     */
    TopicList fetchAllTopicList();

    /**
     * 获取 Topic 状态信息-> 【消息队列-消息索引信息】
     * 从 NameServer 获取 Topic 的路由信息，再根据路由信息获取 Broker 信息，再从 Broker 获取 Topic 的 【消息队列-消息索引信息】
     *
     * @param topic
     * @return
     */
    TopicStatsTable stats(String topic);

    /**
     * 从 NameServer 获取 Topic 路由信息
     *
     * @param topic
     * @return
     */
    TopicRouteData route(String topic);

    GroupList queryTopicConsumerInfo(String topic);

    void createOrUpdate(TopicConfigInfo topicCreateOrUpdateRequest);

    TopicConfig examineTopicConfig(String topic, String brokerName);

    /**
     * 查看 topic 的配置
     *
     * @param topic
     * @return
     */
    List<TopicConfigInfo> examineTopicConfig(String topic);

    /**
     * 删除 Topic 的时候，需要指定集群名，如果集群名不为空，则只删除指定集群的 Topic，否则删除所有集群的 Topic
     *
     * @param topic       主题
     * @param clusterName 集群名
     * @return
     */
    boolean deleteTopic(String topic, String clusterName);

    boolean deleteTopic(String topic);

    boolean deleteTopicInBroker(String brokerName, String topic);

    /**
     * 手动向指定主题发送消息
     *
     * @param sendTopicMessageRequest
     * @return
     */
    SendResult sendTopicMessageRequest(SendTopicMessageRequest sendTopicMessageRequest);

}
