package com.duitang.sinker;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 
 * @author kevx
 * @since 8:16:52 PM Mar 18, 2015
 */
public class SinkerCtx {

    private String clusterZkConnStr;
    private String cluster;
    private String group;
    private String biz;
    private String zkCommEndpoint;
    private int consolePort;
    
    private ObjectMapper mapper = new ObjectMapper();
    
    public SinkerCtx(Properties prop) {
        cluster = prop.getProperty("cluster");
        group = prop.getProperty("group");
        biz = prop.getProperty("biz");
        zkCommEndpoint = prop.getProperty("zkCommEndpoint");
        consolePort = Integer.parseInt(prop.getProperty("consolePort"));
        Validate.isTrue(StringUtils.isNotEmpty(cluster));
        Validate.isTrue(StringUtils.isNotEmpty(group));
        Validate.isTrue(StringUtils.isNotEmpty(biz));
        
        try {
            ZooKeeper zk = new ZooKeeper(zkCommEndpoint, 3000, null);
            byte[] bs = zk.getData("/config/kafka_clusters/" + cluster, false, new Stat());
            zk.close();
            Validate.isTrue(bs != null);
            @SuppressWarnings("unchecked")
            Map<String, Object> obj = mapper.readValue(new String(bs), Map.class);
            String connStr = (String) obj.get("zk_endpoint");
            clusterZkConnStr = connStr;
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
