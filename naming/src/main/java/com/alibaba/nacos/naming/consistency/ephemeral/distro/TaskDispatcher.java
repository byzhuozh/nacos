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
package com.alibaba.nacos.naming.consistency.ephemeral.distro;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.misc.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Data sync task dispatcher
 *
 * @author nkorange
 * @since 1.0.0
 */
@Component
public class TaskDispatcher {

    @Autowired
    private GlobalConfig partitionConfig;

    @Autowired
    private DataSyncer dataSyncer;

    private List<TaskScheduler> taskSchedulerList = new ArrayList<>();

    private final int cpuCoreCount = Runtime.getRuntime().availableProcessors();

    @PostConstruct
    public void init() {
        // 根据cpu个数，新建任务，并且执行每个任务
        for (int i = 0; i < cpuCoreCount; i++) {
            TaskScheduler taskScheduler = new TaskScheduler(i);
            taskSchedulerList.add(taskScheduler);
            GlobalExecutor.submitTaskDispatch(taskScheduler);
        }
    }

    public void addTask(String key) {
        // 添加实例到变更列表 (taskSchedulerList 在 TaskDispatcher 初始化的时候就开始构建)
        taskSchedulerList.get(UtilsAndCommons.shakeUp(key, cpuCoreCount)).addTask(key);
    }

    public class TaskScheduler implements Runnable {

        private int index;

        private int dataSize = 0;

        private long lastDispatchTime = 0L;

        private BlockingQueue<String> queue = new LinkedBlockingQueue<>(128 * 1024);

        public TaskScheduler(int index) {
            this.index = index;
        }

        public void addTask(String key) {
            queue.offer(key);
        }

        public int getIndex() {
            return index;
        }

        @Override
        public void run() {
            List<String> keys = new ArrayList<>();

            while (true) {
                try {
                    // 2s 超时阻塞
                    String key = queue.poll(partitionConfig.getTaskDispatchPeriod(), TimeUnit.MILLISECONDS);

                    if (Loggers.DISTRO.isDebugEnabled() && StringUtils.isNotBlank(key)) {
                        Loggers.DISTRO.debug("got key: {}", key);
                    }

                    if (dataSyncer.getServers() == null || dataSyncer.getServers().isEmpty()) {
                        continue;
                    }

                    if (StringUtils.isBlank(key)) {
                        continue;
                    }

                    if (dataSize == 0) {
                        keys = new ArrayList<>();
                    }

                    keys.add(key);
                    dataSize++;

                    // 每次批量同步 1000 条 或是 两次同步的时间间隔至少超过2s
                    if (dataSize == partitionConfig.getBatchSyncKeyCount() ||
                        (System.currentTimeMillis() - lastDispatchTime) > partitionConfig.getTaskDispatchPeriod()) {

                        for (Server member : dataSyncer.getServers()) {
                            // 剔除本机服务
                            if (NetUtils.localServer().equals(member.getKey())) {
                                continue;
                            }

                            // 构建同步任务
                            SyncTask syncTask = new SyncTask();
                            syncTask.setKeys(keys);
                            syncTask.setTargetServer(member.getKey());

                            if (Loggers.DISTRO.isDebugEnabled() && StringUtils.isNotBlank(key)) {
                                Loggers.DISTRO.debug("add sync task: {}", JSON.toJSONString(syncTask));
                            }

                            // 提交数据同步任务
                            dataSyncer.submit(syncTask, 0);
                        }

                        //更新同步时间
                        lastDispatchTime = System.currentTimeMillis();
                        dataSize = 0;
                    }

                } catch (Exception e) {
                    Loggers.DISTRO.error("dispatch sync task failed.", e);
                }
            }
        }
    }
}
