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

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.ServerStatus;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.consistency.ApplyAction;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.ephemeral.EphemeralConsistencyService;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.pojo.Record;
import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * A consistency protocol algorithm called <b>Distro</b>
 * <p>
 * Use a distro algorithm to divide data into many blocks. Each Nacos server node takes
 * responsibility for exactly one block of data. Each block of data is generated, removed
 * and synchronized by its responsible server. So every Nacos server only handles writings
 * for a subset of the total service data.
 * <p>
 * At mean time every Nacos server receives data sync of other Nacos server, so every Nacos
 * server will eventually have a complete set of data.
 *
 * @author nkorange
 * @since 1.0.0
 *
 * AP 模式的一致性服务
 */
@org.springframework.stereotype.Service("distroConsistencyService")
public class DistroConsistencyServiceImpl implements EphemeralConsistencyService {

    private ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);

            t.setDaemon(true);
            t.setName("com.alibaba.nacos.naming.distro.notifier");

            return t;
        }
    });

    @Autowired
    private DistroMapper distroMapper;

    @Autowired
    private DataStore dataStore;

    @Autowired
    private TaskDispatcher taskDispatcher;

    @Autowired
    private DataSyncer dataSyncer;

    @Autowired
    private Serializer serializer;

    @Autowired
    private ServerListManager serverListManager;

    @Autowired
    private SwitchDomain switchDomain;

    @Autowired
    private GlobalConfig globalConfig;

    private boolean initialized = false;

    public volatile Notifier notifier = new Notifier();

    //key：nameSpace + serviceName, 保存每个服务的监听器
    private Map<String, CopyOnWriteArrayList<RecordListener>> listeners = new ConcurrentHashMap<>();

    private Map<String, String> syncChecksumTasks = new ConcurrentHashMap<>(16);

    @PostConstruct
    public void init() {
        GlobalExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    // 注册中心节点同步数据任务
                    load();
                } catch (Exception e) {
                    Loggers.DISTRO.error("load data failed.", e);
                }
            }
        });

        // 提交通知任务
        executor.submit(notifier);
    }

    public void load() throws Exception {
        // 单机模式
        if (SystemUtils.STANDALONE_MODE) {
            initialized = true;
            return;
        }
        // size = 1 means only myself in the list, we need at least one another server alive:
        while (serverListManager.getHealthyServers().size() <= 1) {
            Thread.sleep(1000L);
            Loggers.DISTRO.info("waiting server list init...");
        }

        for (Server server : serverListManager.getHealthyServers()) {
            // 剔除本机服务
            if (NetUtils.localServer().equals(server.getKey())) {
                continue;
            }

            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.debug("sync from " + server);
            }

            // try sync data from remote server:
            // 发起HTTP请求获取数据，从其他服务节点拿数据
            // 只要有一个节点同步成功则返回
            if (syncAllDataFromRemote(server)) {
                initialized = true;
                return;
            }
        }
    }

    @Override
    public void put(String key, Record value) throws NacosException {
        //将实例信息更新到注册表
        onPut(key, value);

        //同步实例数据到集群其他节点
        taskDispatcher.addTask(key);
    }

    @Override
    public void remove(String key) throws NacosException {
        onRemove(key);
        listeners.remove(key);
    }

    @Override
    public Datum get(String key) throws NacosException {
        return dataStore.get(key);
    }

    public void onPut(String key, Record value) {
        // 如果是临时注册的服务
        if (KeyBuilder.matchEphemeralInstanceListKey(key)) {

            //构建 datum
            Datum<Instances> datum = new Datum<>();
            datum.value = (Instances) value;
            datum.key = key;
            datum.timestamp.incrementAndGet();
            // 将实例信息更新到注册表
            dataStore.put(key, datum);
        }

        // 如果 listener 里边有没有key, 则直接返回，主要用作发布订阅，进行通知
        // 在 ServiceManager.putServiceAndInit 方法中注入了两个监听器
        if (!listeners.containsKey(key)) {
            return;
        }

        // 添加通知任务，表示该 key 代表的服务发生变更
        notifier.addTask(key, ApplyAction.CHANGE);
    }

    public void onRemove(String key) {

        dataStore.remove(key);

        if (!listeners.containsKey(key)) {
            return;
        }

        notifier.addTask(key, ApplyAction.DELETE);
    }

    public void onReceiveChecksums(Map<String, String> checksumMap, String server) {

        if (syncChecksumTasks.containsKey(server)) {
            // Already in process of this server:
            Loggers.DISTRO.warn("sync checksum task already in process with {}", server);
            return;
        }

        //标记同步的服务
        syncChecksumTasks.put(server, "1");

        try {

            //待更新的服务
            List<String> toUpdateKeys = new ArrayList<>();
            List<String> toRemoveKeys = new ArrayList<>();

            for (Map.Entry<String, String> entry : checksumMap.entrySet()) {
                if (distroMapper.responsible(KeyBuilder.getServiceName(entry.getKey()))) {
                    // this key should not be sent from remote server:
                    Loggers.DISTRO.error("receive responsible key timestamp of " + entry.getKey() + " from " + server);
                    // abort the procedure:
                    return;
                }

                //服务不存在或是服务校验和不一致
                if (!dataStore.contains(entry.getKey()) ||
                    dataStore.get(entry.getKey()).value == null ||
                    !dataStore.get(entry.getKey()).value.getChecksum().equals(entry.getValue())) {
                    toUpdateKeys.add(entry.getKey());
                }
            }

            for (String key : dataStore.keys()) {

                // 只处理来源于 server 地址的请求数据
                if (!server.equals(distroMapper.mapSrv(KeyBuilder.getServiceName(key)))) {
                    continue;
                }

                //同步过来的请求数据不包含本地缓存的key，则本地缓存的key，需要删除
                if (!checksumMap.containsKey(key)) {
                    toRemoveKeys.add(key);
                }
            }

            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.info("to remove keys: {}, to update keys: {}, source: {}", toRemoveKeys, toUpdateKeys, server);
            }

            for (String key : toRemoveKeys) {
                // 移除本地缓存的服务数据
                onRemove(key);
            }

            if (toUpdateKeys.isEmpty()) {
                return;
            }

            try {
                byte[] result = NamingProxy.getData(toUpdateKeys, server);
                // 处理待更新的服务列表
                processData(result);
            } catch (Exception e) {
                Loggers.DISTRO.error("get data from " + server + " failed!", e);
            }
        } finally {
            // Remove this 'in process' flag:
            syncChecksumTasks.remove(server);
        }

    }

    public boolean syncAllDataFromRemote(Server server) {

        try {
            // 获取全部注册的服务信息
            byte[] data = NamingProxy.getAllData(server.getKey());
            // 同步的实现，处理远程获取到数据
            processData(data);
            return true;
        } catch (Exception e) {
            Loggers.DISTRO.error("sync full data from " + server + " failed!", e);
            return false;
        }
    }

    public void processData(byte[] data) throws Exception {
        if (data.length > 0) {
            // 发序列化数据，获取服务的实例信息
            Map<String, Datum<Instances>> datumMap = serializer.deserializeMap(data, Instances.class);

            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {
                // 放入当前服务实例的dataStore中
                dataStore.put(entry.getKey(), entry.getValue());

                //获取到远程数据之后，填充dataStore，但是service数据还未被填充
                //这里会先判断service是否已经存在，listeners添加的位置是在创建完service实例后，会添加到listeners里
                //而创建service有两处，一处是服务注册时，另外一处是心跳时发现服务不在了，也会创建一个新的service
                if (!listeners.containsKey(entry.getKey())) {

                    // pretty sure the service not exist:
                    if (switchDomain.isDefaultInstanceEphemeral()) {
                        // create empty service
                        Loggers.DISTRO.info("creating service {}", entry.getKey());
                        Service service = new Service();
                        String serviceName = KeyBuilder.getServiceName(entry.getKey());
                        String namespaceId = KeyBuilder.getNamespace(entry.getKey());
                        service.setName(serviceName);
                        service.setNamespaceId(namespaceId);
                        service.setGroupName(Constants.DEFAULT_GROUP);
                        // now validate the service. if failed, exception will be thrown
                        service.setLastModifiedMillis(System.currentTimeMillis());
                        service.recalculateChecksum();

                        //执行监听的onChange方法，这里默认拿到的就是 ServiceManager 监听
                        // ServiceManager.init 会初始化 SERVICE_META_KEY_PREFIX 的监听器
                        //然后通过ServiceManager的onChange方法更新服务列表
                        listeners.get(KeyBuilder.SERVICE_META_KEY_PREFIX)
                            .get(0)
                            .onChange(KeyBuilder.buildServiceMetaKey(namespaceId, serviceName), service);
                    }
                }
            }

            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {

                if (!listeners.containsKey(entry.getKey())) {
                    // Should not happen:
                    Loggers.DISTRO.warn("listener of {} not found.", entry.getKey());
                    continue;
                }

                try {
                    for (RecordListener listener : listeners.get(entry.getKey())) {
                        // 触发执行service监听，目的是更新服务列表
                        listener.onChange(entry.getKey(), entry.getValue().value);
                    }
                } catch (Exception e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] error while execute listener of key: {}", entry.getKey(), e);
                    continue;
                }

                // Update data store if listener executed successfully:
                //触发执行监听时，可能会对数据发生改变，所以需要重新put
                dataStore.put(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public void listen(String key, RecordListener listener) throws NacosException {
        //判断 key 对应的服务是否存在监听器
        if (!listeners.containsKey(key)) {
            listeners.put(key, new CopyOnWriteArrayList<>());
        }

        if (listeners.get(key).contains(listener)) {
            return;
        }

        // 添加监听器
        listeners.get(key).add(listener);
    }

    @Override
    public void unlisten(String key, RecordListener listener) throws NacosException {
        if (!listeners.containsKey(key)) {
            return;
        }
        for (RecordListener recordListener : listeners.get(key)) {
            if (recordListener.equals(listener)) {
                listeners.get(key).remove(listener);
                break;
            }
        }
    }

    @Override
    public boolean isAvailable() {
        return isInitialized() || ServerStatus.UP.name().equals(switchDomain.getOverriddenServerStatus());
    }

    public boolean isInitialized() {
        return initialized || !globalConfig.isDataWarmup();
    }

    public class Notifier implements Runnable {

        private ConcurrentHashMap<String, String> services = new ConcurrentHashMap<>(10 * 1024);

        private BlockingQueue<Pair> tasks = new LinkedBlockingQueue<Pair>(1024 * 1024);

        public void addTask(String datumKey, ApplyAction action) {
            // 如果已经存在，并且是 change 时间
            if (services.containsKey(datumKey) && action == ApplyAction.CHANGE) {
                return;
            }

           // change 时间
            if (action == ApplyAction.CHANGE) {
                // 往 缓存中 存一份
                services.put(datumKey, StringUtils.EMPTY);
            }

            // 添加到任务队列中
            tasks.add(Pair.with(datumKey, action));
        }

        public int getTaskSize() {
            return tasks.size();
        }

        @Override
        public void run() {
            Loggers.DISTRO.info("distro notifier started");

            while (true) {
                try {
                    // 取出任务
                    Pair pair = tasks.take();

                    if (pair == null) {
                        continue;
                    }

                    String datumKey = (String) pair.getValue0();
                    ApplyAction action = (ApplyAction) pair.getValue1();

                    services.remove(datumKey);

                    int count = 0;

                    if (!listeners.containsKey(datumKey)) {
                        continue;
                    }

                    for (RecordListener listener : listeners.get(datumKey)) {

                        count++;

                        try {
                            // 通知数据已经改变
                            if (action == ApplyAction.CHANGE) {
                                // 将 key 对应的实例集合传过去
                                listener.onChange(datumKey, dataStore.get(datumKey).value);
                                continue;
                            }

                            // 通知数据删除
                            if (action == ApplyAction.DELETE) {
                                listener.onDelete(datumKey);
                                continue;
                            }
                        } catch (Throwable e) {
                            Loggers.DISTRO.error("[NACOS-DISTRO] error while notifying listener of key: {}", datumKey, e);
                        }
                    }

                    if (Loggers.DISTRO.isDebugEnabled()) {
                        Loggers.DISTRO.debug("[NACOS-DISTRO] datum change notified, key: {}, listener count: {}, action: {}",
                            datumKey, count, action.name());
                    }
                } catch (Throwable e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] Error while handling notifying task", e);
                }
            }
        }
    }
}
