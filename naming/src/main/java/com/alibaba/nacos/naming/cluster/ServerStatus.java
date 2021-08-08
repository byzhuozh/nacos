/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.cluster;

/**
 * A flag to indicate the exact status of a server.
 *
 * 服务状态
 *
 * @author nkorange
 * @since 1.0.0
 */
public enum ServerStatus {
    /**
     * server is up and ready for request
     * 节点已经上线，已经具备接收请求能力
     */
    UP,

    /**
     * server is out of service, something abnormal happened
     * 节点已经下线
     */
    DOWN,

    /**
     * server is preparing itself for request, usually 'UP' is the next status
     * 节点正在启动中，加载并初始化节点资源，还不具备接收请求能力
     */
    STARTING,

    /**
     * server is manually paused
     * 节点服务处于手动暂停状态
     */
    PAUSED,

    /**
     * only write operation is permitted.
     * 节点只能写
     */
    WRITE_ONLY,

    /**
     * only read operation is permitted.
     * 节点只能读
     */
    READ_ONLY
}
