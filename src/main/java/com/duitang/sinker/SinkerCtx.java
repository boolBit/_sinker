package com.duitang.sinker;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.Lists;

/**
 * 
 * @author kevx
 * @since 8:16:52 PM Mar 18, 2015
 */
public class SinkerCtx {

    private final String group = "sinker";
    private String zkCommEndpoint;
    private String clusterZkConnStr;
    private String cluster;
    private String biz;
    private int consolePort;
    private List<String> topics = Lists.newArrayList();
    
    private ObjectMapper mapper = new ObjectMapper();
    
    private static final String zkBase = "/config/kafka_clusters";
    
    public SinkerCtx(Properties prop) {
        biz = prop.getProperty("biz");
        zkCommEndpoint = prop.getProperty("zkCommEndpoint");
        consolePort = Integer.parseInt(prop.getProperty("consolePort"));
        Validate.isTrue(StringUtils.isNotEmpty(biz));
        
        try {
            ZooKeeper zk = new ZooKeeper(zkCommEndpoint, 3000, null);
            List<String> clusters = zk.getChildren(zkBase, false);
            for (String cluster : clusters) {
                List<String> bizs = zk.getChildren(zkBase + '/' + cluster, false);
                if (bizs.contains(biz)) {
                    byte[] bs = zk.getData(zkBase + '/' + cluster, false, new Stat());
                    Validate.isTrue(bs != null);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> obj = mapper.readValue(new String(bs), Map.class);
                    clusterZkConnStr = (String) obj.get("zk_endpoint");
                    
                    List<String> lst = zk.getChildren(zkBase + '/' + cluster + '/' + biz, false);
                    if (lst != null && !lst.isEmpty()) {
                        topics.addAll(lst);
                    } else {
                        topics.add(biz);
                    }
                }
            }
            zk.close();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    
    public String getClusterZkConnStr() {
        return clusterZkConnStr;
    }

    @Override
    public String toString() {
        return String.format("cluster:%s;biz:%s;group:%s", cluster, biz, group);
    }

    public List<String> getTopics() {
        return topics;
    }

    public String getCluster() {
        return cluster;
    }

    public String getGroup() {
        return group;
    }

    public String getBiz() {
        return biz;
    }

    public int getConsolePort() {
        return consolePort;
    }
    
}
