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

package org.apache.rocketmq.console.service.impl;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.console.config.RMQConfigure;
import org.apache.rocketmq.console.model.request.SendTopicMessageRequest;
import org.apache.rocketmq.console.model.request.TopicConfigInfo;
import org.apache.rocketmq.console.service.AbstractCommonService;
import org.apache.rocketmq.console.service.TopicService;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TopicServiceImpl extends AbstractCommonService implements TopicService {

    /**
     * RocketMQ控制台配置类
     */
    @Autowired
    private RMQConfigure rMQConfigure;

    /**
     * 从 NameServer 获取 Topic 列表
     *
     * @return
     */
    @Override
    public TopicList fetchAllTopicList() {
        try {
            return mqAdminExt.fetchAllTopicList();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * 获取 Topic 状态信息-> 【消息队列-消息索引信息】
     * 从 NameServer 获取 Topic 的路由信息，再根据路由信息获取 Broker 列表信息，再从每个主 Broker 获取 Topic 的 【消息队列-消息索引信息】
     *
     * @param topic
     * @return
     */
    @Override
    public TopicStatsTable stats(String topic) {
        try {
            return mqAdminExt.examineTopicStats(topic);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * 从 NameServer 获取 Topic 路由信息
     *
     * @param topic
     * @return
     */
    @Override
    public TopicRouteData route(String topic) {
        try {
            return mqAdminExt.examineTopicRouteInfo(topic);
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Override
    public GroupList queryTopicConsumerInfo(String topic) {
        try {
            return mqAdminExt.queryTopicConsumeByWho(topic);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void createOrUpdate(TopicConfigInfo topicCreateOrUpdateRequest) {
        TopicConfig topicConfig = new TopicConfig();
        BeanUtils.copyProperties(topicCreateOrUpdateRequest, topicConfig);
        try {
            ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();
            for (String brokerName : changeToBrokerNameSet(clusterInfo.getClusterAddrTable(),
                    topicCreateOrUpdateRequest.getClusterNameList(), topicCreateOrUpdateRequest.getBrokerNameList())) {
                mqAdminExt.createAndUpdateTopicConfig(clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr(), topicConfig);
            }
        } catch (Exception err) {
            throw Throwables.propagate(err);
        }
    }

    @Override
    public TopicConfig examineTopicConfig(String topic, String brokerName) {
        ClusterInfo clusterInfo = null;
        try {
            clusterInfo = mqAdminExt.examineBrokerClusterInfo();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return mqAdminExt.examineTopicConfig(clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr(), topic);
    }

    /**
     * 查看Topic 配置信息
     *
     * @param topic
     * @return
     */
    @Override
    public List<TopicConfigInfo> examineTopicConfig(String topic) {
        List<TopicConfigInfo> topicConfigInfoList = Lists.newArrayList();
        // 从 NameServer 获取 Topic 路由信息
        TopicRouteData topicRouteData = route(topic);
        for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
            TopicConfigInfo topicConfigInfo = new TopicConfigInfo();
            TopicConfig topicConfig = examineTopicConfig(topic, brokerData.getBrokerName());
            BeanUtils.copyProperties(topicConfig, topicConfigInfo);
            topicConfigInfo.setBrokerNameList(Lists.newArrayList(brokerData.getBrokerName()));
            topicConfigInfoList.add(topicConfigInfo);
        }
        return topicConfigInfoList;
    }

    /**
     * 删除 Topic 的时候，需要指定集群名，如果集群名不为空，则只删除指定集群的 Topic，否则删除所有集群的 Topic
     *
     * @param topic       主题
     * @param clusterName 集群名
     * @return
     */
    @Override
    public boolean deleteTopic(String topic, String clusterName) {
        try {

            // 如果集群名为空，则删除所有集群的 Topic
            if (StringUtils.isBlank(clusterName)) {
                return deleteTopic(topic);
            }


            // 集群名不为空，则只删除指定集群的 Topic

            // 删除 Broker 上的 Topic
            // 获取指定集群的 Master Broker
            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(mqAdminExt, clusterName);
            // 删除主Broker上的 Topic【删除主题下的所有队列文件，清除主题下的队列的最大逻辑偏移量缓存记录，尝试删除 BrokerStatsManager 中该主题的统计数据（默认不删除）】
            mqAdminExt.deleteTopicInBroker(masterSet, topic);

            // 删除 NameServer 上的 Topic【主题的队列映射缓存】
            Set<String> nameServerSet = null;
            if (StringUtils.isNotBlank(rMQConfigure.getNamesrvAddr())) {
                String[] ns = rMQConfigure.getNamesrvAddr().split(";");
                nameServerSet = new HashSet<String>(Arrays.asList(ns));
            }
            mqAdminExt.deleteTopicInNameServer(nameServerSet, topic);

        } catch (Exception err) {
            throw Throwables.propagate(err);
        }

        return true;
    }

    @Override
    public boolean deleteTopic(String topic) {
        ClusterInfo clusterInfo = null;
        try {
            clusterInfo = mqAdminExt.examineBrokerClusterInfo();
        } catch (Exception err) {
            throw Throwables.propagate(err);
        }
        for (String clusterName : clusterInfo.getClusterAddrTable().keySet()) {
            deleteTopic(topic, clusterName);
        }
        return true;
    }

    @Override
    public boolean deleteTopicInBroker(String brokerName, String topic) {

        try {
            ClusterInfo clusterInfo = null;
            try {
                clusterInfo = mqAdminExt.examineBrokerClusterInfo();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            mqAdminExt.deleteTopicInBroker(Sets.newHashSet(clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr()), topic);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return true;
    }

    /**
     * 手动向指定主题发送消息
     *
     * @param sendTopicMessageRequest
     * @return
     */
    @Override
    public SendResult sendTopicMessageRequest(SendTopicMessageRequest sendTopicMessageRequest) {
        // 创建消息生产者
        DefaultMQProducer producer = new DefaultMQProducer(MixAll.SELF_TEST_PRODUCER_GROUP);

        // 设置实例名
        producer.setInstanceName(String.valueOf(System.currentTimeMillis()));

        // 设置NameServer地址
        producer.setNamesrvAddr(rMQConfigure.getNamesrvAddr());
        try {

            // 启动生产者
            producer.start();

            // 构建消息
            Message msg = new Message(sendTopicMessageRequest.getTopic(),
                    sendTopicMessageRequest.getTag(),
                    sendTopicMessageRequest.getKey(),
                    sendTopicMessageRequest.getMessageBody().getBytes()
            );

            // 发送消息
            return producer.send(msg);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            // 关闭生产者
            producer.shutdown();
        }
    }

}
