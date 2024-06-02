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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Set;
import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.console.model.MessageView;
import org.apache.rocketmq.console.service.MessageService;
import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.apache.rocketmq.tools.admin.api.MessageTrack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class MessageServiceImpl implements MessageService {

    private Logger logger = LoggerFactory.getLogger(MessageServiceImpl.class);
    /**
     * @see org.apache.rocketmq.store.config.MessageStoreConfig maxMsgsNumBatch = 64;
     * @see org.apache.rocketmq.store.index.IndexService maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());
     */
    private final static int QUERY_MESSAGE_MAX_NUM = 64;
    @Resource
    private MQAdminExt mqAdminExt;

    /**
     * 查询消息详情
     * 1 消息
     * 2 消息轨迹
     *
     * @param subject topic
     * @param msgId   消息 offsetMsgId
     * @return
     */
    public Pair<MessageView, List<MessageTrack>> viewMessage(String subject, final String msgId) {
        try {
            // 根据 offsetMsgId 解析出消息所在的 Broker地址，以及消息在 CommitLog 的物理偏移量，从Broker查询消息
            MessageExt messageExt = mqAdminExt.viewMessage(subject, msgId);

            // 获取消息轨迹【订阅该消息的消费组、消息轨迹情况、异常描述-用于记录消费过程中可能出现的异常情况】
            List<MessageTrack> messageTrackList = messageTrackDetail(messageExt);
            return new Pair<>(MessageView.fromMessageExt(messageExt), messageTrackList);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<MessageView> queryMessageByTopicAndKey(String topic, String key) {
        try {
            return Lists.transform(mqAdminExt.queryMessage(topic, key, QUERY_MESSAGE_MAX_NUM, 0, System.currentTimeMillis()).getMessageList(), new Function<MessageExt, MessageView>() {
                @Override
                public MessageView apply(MessageExt messageExt) {
                    return MessageView.fromMessageExt(messageExt);
                }
            });
        } catch (Exception err) {
            throw Throwables.propagate(err);
        }
    }

    /**
     * 通过 Topic 和时间范围查询消息
     *
     * @param topic 主题
     * @param begin 开始时间
     * @param end   结束时间
     * @return
     */
    @Override
    public List<MessageView> queryMessageByTopic(String topic, final long begin, final long end) {
        // 创建消费者
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(MixAll.TOOLS_CONSUMER_GROUP, null);
        List<MessageView> messageViewList = Lists.newArrayList();

        try {
            // 订阅所有消息
            String subExpression = "*";
            consumer.start();

            // 从 NameServer 获取 topic 的路由信息，然后组装成消费队列
            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);

            // 遍历消费队列
            for (MessageQueue mq : mqs) {
                // 从对应的 Broker 间接获取当前队列的最小逻辑偏移量
                long minOffset = consumer.searchOffset(mq, begin);
                // 从对应的 Broker 间接获取当前队列的最大逻辑偏移量
                long maxOffset = consumer.searchOffset(mq, end);
                READQ:
                for (long offset = minOffset; offset <= maxOffset; ) {
                    try {
                        // 一次最多2000条消息
                        if (messageViewList.size() > 2000) {
                            break;
                        }

                        // 手动拉取消息
                        PullResult pullResult = consumer.pull(mq, subExpression, offset, 32);
                        // 下次从什么地方拉取消息
                        offset = pullResult.getNextBeginOffset();

                        switch (pullResult.getPullStatus()) {

                            // 收集符合条件的消息
                            case FOUND:
                                List<MessageView> messageViewListByQuery = Lists.transform(pullResult.getMsgFoundList(), new Function<MessageExt, MessageView>() {
                                    @Override
                                    public MessageView apply(MessageExt messageExt) {
                                        messageExt.setBody(null);
                                        return MessageView.fromMessageExt(messageExt);
                                    }
                                });
                                List<MessageView> filteredList = Lists.newArrayList(Iterables.filter(messageViewListByQuery, new Predicate<MessageView>() {
                                    @Override
                                    public boolean apply(MessageView messageView) {
                                        if (messageView.getStoreTimestamp() < begin || messageView.getStoreTimestamp() > end) {
                                            logger.info("begin={} end={} time not in range {} {}", begin, end, messageView.getStoreTimestamp(), new Date(messageView.getStoreTimestamp()).toString());
                                        }
                                        return messageView.getStoreTimestamp() >= begin && messageView.getStoreTimestamp() <= end;
                                    }
                                }));
                                messageViewList.addAll(filteredList);
                                break;
                            case NO_MATCHED_MSG:
                            case NO_NEW_MSG:
                            case OFFSET_ILLEGAL:
                                break READQ;
                        }
                    } catch (Exception e) {
                        break;
                    }
                }
            }


            // 按照存储时间排序
            Collections.sort(messageViewList, new Comparator<MessageView>() {
                @Override
                public int compare(MessageView o1, MessageView o2) {
                    if (o1.getStoreTimestamp() - o2.getStoreTimestamp() == 0) {
                        return 0;
                    }
                    return (o1.getStoreTimestamp() > o2.getStoreTimestamp()) ? -1 : 1;
                }
            });


            return messageViewList;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            consumer.shutdown();
        }
    }

    /**
     * 消息轨迹
     * 1 根据消息中的 topic ，获取订阅该 topic 的消费组列表；
     * 2 针对每个消费组，获取消费组的消费者连接通道列表及订阅信息集合，判断消息轨迹是否属于不在线的情况、拉方式
     * 3 如果消费组消息类型是 PUSH 模式，则判断消息是否被消费组消费，判断方式：group@topic 对应的消费进度 > 消息的逻辑偏移量；
     * 4 为了使消息轨迹更精准，还需对已消费的情况做进一步判断，因为消费消息时消费进度提交可能是因为消息被过滤掉了，针对这种情况，
     * 需要判断消费组订阅的数据是否匹配消息，如果匹配，则认为消息被消费；否则，则认为已消费但被过滤的跟踪类型；
     *
     * @param msg 消息
     * @return
     */
    @Override
    public List<MessageTrack> messageTrackDetail(MessageExt msg) {
        try {
            return mqAdminExt.messageTrackDetail(msg);
        } catch (Exception e) {
            logger.error("op=messageTrackDetailError", e);
            return Collections.emptyList();
        }
    }


    /**
     * 直接将消息发送给消费组下的消费者消费
     * 1 根据 msgId 从 Broker 查询对应的消息；
     * 2 根据消息中的Broker地址请求Broker
     * 3 根据 消费组和消费者ID 从 Broker 中找到消费者连接通道
     * 4 通过该通道向消费者发送消息
     *
     * @param topic         主题
     * @param msgId         消息offsetMsgId
     * @param consumerGroup 消费组
     * @param clientId      客户端ID
     * @return
     */
    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(String topic, String msgId, String consumerGroup,
                                                               String clientId) {
        // 消费者ID不为空
        if (StringUtils.isNotBlank(clientId)) {
            try {
                return mqAdminExt.consumeMessageDirectly(consumerGroup, clientId, topic, msgId);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }


        // 没有指定消费者ID，那么从消费组中选择一个消费者
        try {
            ConsumerConnection consumerConnection = mqAdminExt.examineConsumerConnectionInfo(consumerGroup);
            for (Connection connection : consumerConnection.getConnectionSet()) {
                if (StringUtils.isBlank(connection.getClientId())) {
                    continue;
                }
                logger.info("clientId={}", connection.getClientId());
                return mqAdminExt.consumeMessageDirectly(consumerGroup, connection.getClientId(), topic, msgId);
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        throw new IllegalStateException("NO CONSUMER");

    }

}
