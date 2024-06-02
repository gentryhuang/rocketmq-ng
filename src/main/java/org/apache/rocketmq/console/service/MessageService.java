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

import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.tools.admin.api.MessageTrack;
import org.apache.rocketmq.console.model.MessageView;

import java.util.List;

public interface MessageService {

    /**
     * 查询消息详情
     * 1 消息
     * 2 消息轨迹
     *
     * @param subject topic
     * @param msgId   消息 offsetMsgId
     */
    Pair<MessageView, List<MessageTrack>> viewMessage(String subject, final String msgId);

    List<MessageView> queryMessageByTopicAndKey(final String topic, final String key);

    /**
     * 通过 Topic 和时间范围查询消息
     *
     * @param topic 主题
     * @param begin 开始时间
     * @param end   结束时间
     */
    List<MessageView> queryMessageByTopic(final String topic, final long begin,
                                          final long end);

    /**
     * * 消息轨迹
     * * 1 根据消息中的 topic ，获取订阅该 topic 的消费组列表；
     * * 2 针对每个消费组，获取消费组的消费者连接通道列表及订阅信息集合，判断消息轨迹是否属于不在线的情况、拉方式
     * * 3 如果消费组消息类型是 PUSH 模式，则判断消息是否被消费组消费，判断方式：group@topic 对应的消费进度 > 消息的逻辑偏移量；
     * * 4 为了使消息轨迹更精准，还需对已消费的情况做进一步判断，因为消费消息时消费进度提交可能是因为消息被过滤掉了，针对这种情况，
     * * 需要判断消费组订阅的数据是否匹配消息，如果匹配，则认为消息被消费；否则，则认为已消费但被过滤的跟踪类型；
     *
     * @param msg
     * @return
     */
    List<MessageTrack> messageTrackDetail(MessageExt msg);

    /**
     * 将消费直接发送给消费者消费
     *
     * @param topic         主题
     * @param msgId         消息offsetMsgId
     * @param consumerGroup 消费组
     * @param clientId      客户端ID
     * @return
     */
    ConsumeMessageDirectlyResult consumeMessageDirectly(String topic, String msgId, String consumerGroup,
                                                        String clientId);

}
