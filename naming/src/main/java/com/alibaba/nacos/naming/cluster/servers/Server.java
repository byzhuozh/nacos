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
package com.alibaba.nacos.naming.cluster.servers;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;

/**
 * Member node of Nacos cluster
 *
 * 集群的节点信息
 *
 * @author nkorange
 * @since 1.0.0
 */
public class Server implements Comparable<Server> {

    /**
     * IP of member
     * 节点IP地址
     */
    private String ip;

    /**
     * serving port of member.
     * 端口号
     */
    private int servePort;

    /**
     * 节点位置，暂时没有使用
     */
    private String site = UtilsAndCommons.UNKNOWN_SITE;

    // weight：节点权重
    private int weight = 1;

    /**
     * additional weight, used to adjust manually
     * 添加权重
     */
    private int adWeight;

    /*
     * 是否存活，默认为false
     */
    private boolean alive = false;

    /*
     * 最后服务时间 long类型
     */
    private long lastRefTime = 0L;

    /**
     * 最后服务时间 String类型
     */
    private String lastRefTimeStr;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getServePort() {
        return servePort;
    }

    public void setServePort(int servePort) {
        this.servePort = servePort;
    }

    public String getSite() {
        return site;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public int getAdWeight() {
        return adWeight;
    }

    public void setAdWeight(int adWeight) {
        this.adWeight = adWeight;
    }

    public boolean isAlive() {
        return alive;
    }

    public void setAlive(boolean alive) {
        this.alive = alive;
    }

    public long getLastRefTime() {
        return lastRefTime;
    }

    public void setLastRefTime(long lastRefTime) {
        this.lastRefTime = lastRefTime;
    }

    public String getLastRefTimeStr() {
        return lastRefTimeStr;
    }

    public void setLastRefTimeStr(String lastRefTimeStr) {
        this.lastRefTimeStr = lastRefTimeStr;
    }

    public String getKey() {
        return ip + UtilsAndCommons.IP_PORT_SPLITER + servePort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Server server = (Server) o;
        return servePort == server.servePort && ip.equals(server.ip);
    }

    @Override
    public int hashCode() {
        int result = ip.hashCode();
        result = 31 * result + servePort;
        return result;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    @Override
    public int compareTo(Server server) {
        if (server == null) {
            return 1;
        }
        return this.getKey().compareTo(server.getKey());
    }
}
