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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.console.controller;

import javax.annotation.Resource;

import com.google.common.base.Strings;
import org.apache.rocketmq.console.service.DashboardService;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * RocketMQ 大盘相关的 HTTP 请求
 */
@Controller
@RequestMapping("/dashboard")
public class DashboardController {
    /**
     * 大盘服务
     */
    @Resource
    DashboardService dashboardService;

    /**
     * 查询 Broker 数据
     *
     * @param date 查询指定日期的Broker数据
     * @return Broker数据
     */
    @RequestMapping(value = "/broker.query", method = RequestMethod.GET)
    @ResponseBody
    public Object broker(@RequestParam String date) {
        return dashboardService.queryBrokerData(date);
    }

    /**
     * 查询 Topic 数据
     *
     * @param date      查询指定日期的Topic数据
     * @param topicName 可选参数，指定查询的Topic名称。如果为空，返回所有Topic的汇总数据
     * @return 返回查询到的 Topic数据
     */
    @RequestMapping(value = "/topic.query", method = RequestMethod.GET)
    @ResponseBody
    public Object topic(@RequestParam String date, String topicName) {
        // 如果没有指定 Topic 名称，则返回所有 Topic 的汇总数据
        if (Strings.isNullOrEmpty(topicName)) {
            return dashboardService.queryTopicData(date);
        }

        // 返回指定 Topic 的数据
        return dashboardService.queryTopicData(date, topicName);
    }

    /**
     * 查询当前Topic数据
     *
     * @return 返回当前时间的 Topic 数据
     */
    @RequestMapping(value = "/topicCurrent", method = RequestMethod.GET)
    @ResponseBody
    public Object topicCurrent() {
        return dashboardService.queryTopicCurrentData();
    }

}
