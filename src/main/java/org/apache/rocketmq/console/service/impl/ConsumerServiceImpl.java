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

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.RollbackStats;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.console.aspect.admin.annotation.MultiMQAdminCmdMethod;
import org.apache.rocketmq.console.model.ConsumerGroupRollBackStat;
import org.apache.rocketmq.console.model.GroupConsumeInfo;
import org.apache.rocketmq.console.model.QueueStatInfo;
import org.apache.rocketmq.console.model.TopicConsumerInfo;
import org.apache.rocketmq.console.model.request.ConsumerConfigInfo;
import org.apache.rocketmq.console.model.request.DeleteSubGroupRequest;
import org.apache.rocketmq.console.model.request.ResetOffsetRequest;
import org.apache.rocketmq.console.service.AbstractCommonService;
import org.apache.rocketmq.console.service.ConsumerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import static com.google.common.base.Throwables.propagate;

/**
 * 消费服务
 */
@Service
public class ConsumerServiceImpl extends AbstractCommonService implements ConsumerService {
    private Logger logger = LoggerFactory.getLogger(ConsumerServiceImpl.class);

    /**
     * 查询消费组列表及其消费信息
     *
     * @return
     */
    @Override
    @MultiMQAdminCmdMethod
    public List<GroupConsumeInfo> queryGroupList() {
        Set<String> consumerGroupSet = Sets.newHashSet();
        try {

            // 从 NameServer 获取Broker的集群信息
            ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();

            // 遍历Broker，向每个Broker（优先主Broker）获取订阅配置信息
            for (BrokerData brokerData : clusterInfo.getBrokerAddrTable().values()) {
                SubscriptionGroupWrapper subscriptionGroupWrapper = mqAdminExt.getAllSubscriptionGroup(brokerData.selectBrokerAddr(), 3000L);
                // 收集订阅配置中的消费组
                consumerGroupSet.addAll(subscriptionGroupWrapper.getSubscriptionGroupTable().keySet());
            }
        } catch (Exception err) {
            throw Throwables.propagate(err);
        }

        List<GroupConsumeInfo> groupConsumeInfoList = Lists.newArrayList();
        // 遍历消费组，并查询每个消费组的消费信息
        for (String consumerGroup : consumerGroupSet) {
            groupConsumeInfoList.add(queryGroup(consumerGroup));
        }

        Collections.sort(groupConsumeInfoList);
        return groupConsumeInfoList;
    }

    @Override
    @MultiMQAdminCmdMethod
    public GroupConsumeInfo queryGroup(String consumerGroup) {
        GroupConsumeInfo groupConsumeInfo = new GroupConsumeInfo();
        try {
            ConsumeStats consumeStats = null;
            try {
                consumeStats = mqAdminExt.examineConsumeStats(consumerGroup);
            } catch (Exception e) {
                logger.warn("examineConsumeStats exception, " + consumerGroup, e);
            }

            ConsumerConnection consumerConnection = null;
            try {
                consumerConnection = mqAdminExt.examineConsumerConnectionInfo(consumerGroup);
            } catch (Exception e) {
                logger.warn("examineConsumerConnectionInfo exception, " + consumerGroup, e);
            }

            groupConsumeInfo.setGroup(consumerGroup);

            if (consumeStats != null) {
                groupConsumeInfo.setConsumeTps((int) consumeStats.getConsumeTps());
                groupConsumeInfo.setDiffTotal(consumeStats.computeTotalDiff());
            }

            if (consumerConnection != null) {
                groupConsumeInfo.setCount(consumerConnection.getConnectionSet().size());
                groupConsumeInfo.setMessageModel(consumerConnection.getMessageModel());
                groupConsumeInfo.setConsumeType(consumerConnection.getConsumeType());
                groupConsumeInfo.setVersion(MQVersion.getVersionDesc(consumerConnection.computeMinVersion()));
            }
        } catch (Exception e) {
            logger.warn("examineConsumeStats or examineConsumerConnectionInfo exception, " + consumerGroup, e);
        }
        return groupConsumeInfo;
    }


    /**
     * 查询指定消费组订阅的主题的 消费消费详情
     *
     * @param groupName 消费者组名。
     * @return
     */
    @Override
    public List<TopicConsumerInfo> queryConsumeStatsListByGroupName(String groupName) {
        return queryConsumeStatsList(null, groupName);
    }


    /**
     * 根据主题和组名查询消费状态信息列表。
     * 如果没有指定主题，那么就查询消费组订阅的所有主题
     *
     * @param topic     主题名称。
     * @param groupName 消费者组名。
     * @return
     */
    @Override
    @MultiMQAdminCmdMethod
    public List<TopicConsumerInfo> queryConsumeStatsList(final String topic, String groupName) {
        ConsumeStats consumeStats = null;
        try {
            // 从 NameServer 中获取 groupName形式的 topic 的路由信息，然后向该路由信息中的所有 Broker 获取group的topic（没有指定topic，则是消费者订阅的所有topic)的消费状态信息。
            consumeStats = mqAdminExt.examineConsumeStats(groupName, topic);
        } catch (Exception e) {
            throw propagate(e);
        }


        List<MessageQueue> mqList = Lists.newArrayList(Iterables.filter(consumeStats.getOffsetTable().keySet(), new Predicate<MessageQueue>() {
            @Override
            public boolean apply(MessageQueue o) {
                return StringUtils.isBlank(topic) || o.getTopic().equals(topic);
            }
        }));

        Collections.sort(mqList);
        List<TopicConsumerInfo> topicConsumerInfoList = Lists.newArrayList();
        TopicConsumerInfo nowTopicConsumerInfo = null;

        // 获取消费组下的 消费者 消费的队列信息。
        // key: MessageQueue, value: 消费者标识
        Map<MessageQueue, String> messageQueueClientMap = getClientConnection(groupName);
        for (MessageQueue mq : mqList) {
            if (nowTopicConsumerInfo == null || (!StringUtils.equals(mq.getTopic(), nowTopicConsumerInfo.getTopic()))) {
                nowTopicConsumerInfo = new TopicConsumerInfo(mq.getTopic());
                topicConsumerInfoList.add(nowTopicConsumerInfo);
            }
            QueueStatInfo queueStatInfo = QueueStatInfo.fromOffsetTableEntry(mq, consumeStats.getOffsetTable().get(mq));
            queueStatInfo.setClientInfo(messageQueueClientMap.get(mq));

            // 计算并封装topic 的消费信息
            nowTopicConsumerInfo.appendQueueStatInfo(queueStatInfo);
        }
        return topicConsumerInfoList;
    }

    /**
     * 获取消费组下的 消费者消费的队列信息。
     *
     * @param groupName
     * @return
     */
    private Map<MessageQueue, String> getClientConnection(String groupName) {
        Map<MessageQueue, String> results = Maps.newHashMap();
        try {

            // 根据消费组名获取对应重试主题的路由信息，然后向路由信息中某个Broker获取该消费组下的消费详细信息
            ConsumerConnection consumerConnection = mqAdminExt.examineConsumerConnectionInfo(groupName);

            // 遍历该消费组下的所有消费者，然后获取该消费者
            for (Connection connection : consumerConnection.getConnectionSet()) {
                String clinetId = connection.getClientId();
                // 间接从 Broker 获取消费组的消费者消费信息
                ConsumerRunningInfo consumerRunningInfo = mqAdminExt.getConsumerRunningInfo(groupName, clinetId, false);
                for (MessageQueue messageQueue : consumerRunningInfo.getMqTable().keySet()) {
//                    results.put(messageQueue, clinetId + " " + connection.getClientAddr());
                    results.put(messageQueue, clinetId);
                }
            }
        } catch (Exception err) {
            logger.error("op=getClientConnection_error", err);
        }
        return results;
    }

    /**
     * 根据主题名称查询消费状态信息列表。
     *
     * @param topic 主题名称。
     * @return
     */
    @Override
    @MultiMQAdminCmdMethod
    public Map<String /*groupName*/, TopicConsumerInfo> queryConsumeStatsListByTopicName(String topic) {
        // 消费组到消费状态信息列表。
        Map<String, TopicConsumerInfo> group2ConsumerInfoMap = Maps.newHashMap();
        try {

            /*
              通过NameServer查询指定 topic 的路由信息，然后根据路由信息中的 Broker 信息，选择任意一个主Broker，然后获取订阅该topic的消费组列表。
              1 客户端（生产者/消费者）会周期性从NameServer拉取Topic的路由信息，并处理成发布信息/订阅信息缓存在本地；
              2 客户端（生产者/消费者）会周期性向Broker（topic路由信息中的所有broker)发送心跳包，Broker收到心跳包后，会更新该客户端（生产者/消费者）信息。
              3 所以根据从NameServer上拉下来的路由信息中的 Broker 信息，选择任意一个主Broker是可以获取订阅该topic的所有消费组列表。
             */
            GroupList groupList = mqAdminExt.queryTopicConsumeByWho(topic);

            // 遍历消费组
            for (String group : groupList.getGroupList()) {
                List<TopicConsumerInfo> topicConsumerInfoList = null;
                try {

                    // 根据topic和消费组，查询消费状态信息列表
                    topicConsumerInfoList = queryConsumeStatsList(topic, group);
                } catch (Exception ignore) {
                }

                // 将 消费组和消费状态信息列表 放入结果集
                group2ConsumerInfoMap.put(group, CollectionUtils.isEmpty(topicConsumerInfoList) ? new TopicConsumerInfo(topic) : topicConsumerInfoList.get(0));
            }
            return group2ConsumerInfoMap;
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    /**
     * 重置消费位点
     * 说明：针对的是订阅该 topic 的消费组，将组下的所有消费者持有的队列位点进行重置
     *
     * @param resetOffsetRequest 重置消费位点的请求信息。
     * @return
     */
    @Override
    @MultiMQAdminCmdMethod
    public Map<String, ConsumerGroupRollBackStat> resetOffset(ResetOffsetRequest resetOffsetRequest) {
        Map<String, ConsumerGroupRollBackStat> groupRollbackStats = Maps.newHashMap();

        // 遍历消费组，对订阅该 topic 的消费组下的所有消费者们进行消费位点重置
        for (String consumerGroup : resetOffsetRequest.getConsumerGroupList()) {
            try {


                // 重置消费端的消费位点（broker端不会重置）
                // 返回的结果是成功重置的 消息队列-消费位点
                Map<MessageQueue, Long> rollbackStatsMap = mqAdminExt.resetOffsetByTimestamp(resetOffsetRequest.getTopic(), consumerGroup, resetOffsetRequest.getResetTime(), resetOffsetRequest.isForce());
                ConsumerGroupRollBackStat consumerGroupRollBackStat = new ConsumerGroupRollBackStat(true);

                List<RollbackStats> rollbackStatsList = consumerGroupRollBackStat.getRollbackStatsList();
                for (Map.Entry<MessageQueue, Long> rollbackStatsEntty : rollbackStatsMap.entrySet()) {
                    RollbackStats rollbackStats = new RollbackStats();
                    // 重置后的消费位点 - 可回滚位点
                    // 重置broker端时，该值是消费组的队列的消费进度（虽然Broker重置了，但是消费端还没有，当前消费端上报进度时会覆盖Broker端的进度）
                    rollbackStats.setRollbackOffset(rollbackStatsEntty.getValue());
                    // 队列ID
                    rollbackStats.setQueueId(rollbackStatsEntty.getKey().getQueueId());
                    // 队列所属的Broker
                    rollbackStats.setBrokerName(rollbackStatsEntty.getKey().getBrokerName());

                    // 重置消费端的位点不会设置： brokerOffset（消息的逻辑偏移量），consumerOffset（消费位点），timestampOffset（时间戳对应的偏移量）


                    rollbackStatsList.add(rollbackStats);
                }

                groupRollbackStats.put(consumerGroup, consumerGroupRollBackStat);

                // 如果重置消费进度失败
            } catch (MQClientException e) {
                // 消费组下的消费者不在线，
                if (ResponseCode.CONSUMER_NOT_ONLINE == e.getResponseCode()) {
                    try {
                        ConsumerGroupRollBackStat consumerGroupRollBackStat = new ConsumerGroupRollBackStat(true);
                        // 重置 Broker 端的消费位点（消费位点不会重置）
                        List<RollbackStats> rollbackStatsList = mqAdminExt.resetOffsetByTimestampOld(consumerGroup, resetOffsetRequest.getTopic(), resetOffsetRequest.getResetTime(), true);

                        /*
                          rollbackOffset: 时间戳对应的偏移量
                          brokerOffset: 消息的逻辑偏移量
                          consumerOffset: 消费组队列的消费进度，
                          timestampOffset: 时间戳对应的偏移量
                         */
                        consumerGroupRollBackStat.setRollbackStatsList(rollbackStatsList);
                        groupRollbackStats.put(consumerGroup, consumerGroupRollBackStat);
                        continue;
                    } catch (Exception err) {
                        logger.error("op=resetOffset_which_not_online_error", err);
                    }
                } else {
                    logger.error("op=resetOffset_error", e);
                }
                groupRollbackStats.put(consumerGroup, new ConsumerGroupRollBackStat(false, e.getMessage()));
            } catch (Exception e) {
                logger.error("op=resetOffset_error", e);
                groupRollbackStats.put(consumerGroup, new ConsumerGroupRollBackStat(false, e.getMessage()));
            }
        }

        return groupRollbackStats;
    }

    @Override
    @MultiMQAdminCmdMethod
    public List<ConsumerConfigInfo> examineSubscriptionGroupConfig(String group) {
        List<ConsumerConfigInfo> consumerConfigInfoList = Lists.newArrayList();
        try {
            // 从 NameServer 获取Broker集群信息
            ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();

            // 遍历 Broker 集群，向每个Broker 集群发送请求，获取该消费组的配置信息
            for (String brokerName : clusterInfo.getBrokerAddrTable().keySet()) { //foreach brokerName
                String brokerAddress = clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr();
                SubscriptionGroupConfig subscriptionGroupConfig = mqAdminExt.examineSubscriptionGroupConfig(brokerAddress, group);
                if (subscriptionGroupConfig == null) {
                    continue;
                }


                consumerConfigInfoList.add(new ConsumerConfigInfo(Lists.newArrayList(brokerName), subscriptionGroupConfig));
            }
        } catch (Exception e) {
            throw propagate(e);
        }
        return consumerConfigInfoList;
    }

    /**
     * 从指定的Broker删除指定的消费组的订阅配置信息
     *
     * @param deleteSubGroupRequest 删除订阅组的请求信息。
     * @return
     */
    @Override
    @MultiMQAdminCmdMethod
    public boolean deleteSubGroup(DeleteSubGroupRequest deleteSubGroupRequest) {
        try {
            // 从 NameServer 获取Broker集群信息
            ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();

            // 遍历 Broker 集群，向每个Broker 集群发送请求，删除该消费组的配置信息
            for (String brokerName : deleteSubGroupRequest.getBrokerNameList()) {
                logger.info("addr={} groupName={}", clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr(), deleteSubGroupRequest.getGroupName());
                mqAdminExt.deleteSubscriptionGroup(clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr(), deleteSubGroupRequest.getGroupName());
            }
        } catch (Exception e) {
            throw propagate(e);
        }
        return true;
    }

    /**
     * 创建或更新对应Broker上的消费组的配置信息
     *
     * @param consumerConfigInfo 订阅组的配置信息。
     * @return
     */
    @Override
    public boolean createAndUpdateSubscriptionGroupConfig(ConsumerConfigInfo consumerConfigInfo) {
        try {

            // 从 NameServer 获取Broker集群信息
            ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();

            // 遍历Broker集群，向每个Broker集群发送请求，创建或更新对应Broker上的该消费组的配置信息
            for (String brokerName : changeToBrokerNameSet(clusterInfo.getClusterAddrTable(), consumerConfigInfo.getClusterNameList(), consumerConfigInfo.getBrokerNameList())) {
                mqAdminExt.createAndUpdateSubscriptionGroupConfig(clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr(), consumerConfigInfo.getSubscriptionGroupConfig());
            }
        } catch (Exception err) {
            throw Throwables.propagate(err);
        }
        return true;
    }

    @Override
    @MultiMQAdminCmdMethod
    public Set<String> fetchBrokerNameSetBySubscriptionGroup(String group) {
        Set<String> brokerNameSet = Sets.newHashSet();
        try {
            List<ConsumerConfigInfo> consumerConfigInfoList = examineSubscriptionGroupConfig(group);
            for (ConsumerConfigInfo consumerConfigInfo : consumerConfigInfoList) {
                brokerNameSet.addAll(consumerConfigInfo.getBrokerNameList());
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return brokerNameSet;

    }

    /**
     * 获取消费组下的消费者连接通道列表 及 订阅信息集合
     * 1 从 NameServer 获取 consumerGroup形式 主题的路由信息
     * 2 从路由信息中获取一个 Broker ，然后从该Broker获取指定消费组下的消费者连接通道列表 及 订阅信息集合
     *
     * @param consumerGroup 消费者组ID。
     * @return
     */
    @Override
    public ConsumerConnection getConsumerConnection(String consumerGroup) {
        try {
            return mqAdminExt.examineConsumerConnectionInfo(consumerGroup);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ConsumerRunningInfo getConsumerRunningInfo(String consumerGroup, String clientId, boolean jstack) {
        try {
            return mqAdminExt.getConsumerRunningInfo(consumerGroup, clientId, jstack);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
