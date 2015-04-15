package com.duitang.sinker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * 
 * @author kevx
 * @since 8:16:52 PM Mar 18, 2015
 */
public class SinkerCtx {

    private final String group = "sinker";
    private ZooKeeper zk;
    private String hdfsEndpoint;
    private String zkCommEndpoint;
    private String clusterZkConnStr;
    private String cluster;
    private String biz;
    private int consolePort;
    private int parallel;
    private boolean daily = false;
    
    public final AtomicLong msgCount = new AtomicLong();
    
    private ObjectMapper mapper = new ObjectMapper();
    
    private static final String zkBase = "/config/kafka_clusters";
    private static final List<ACL> acls = Lists.newArrayList(new ACL(Perms.ALL, Ids.ANYONE_ID_UNSAFE));
    
    public String getLogBasePath() {
        return "/duitang/logs/usr/sinker/" + biz;
    }
    
    public SinkerCtx(CommandLine cmd) {
        biz = cmd.getOptionValue("biz");
        hdfsEndpoint = cmd.getOptionValue("hdfs");
        zkCommEndpoint = cmd.getOptionValue("zkcomm");
        consolePort = Integer.parseInt(cmd.getOptionValue("port"));
        parallel = Integer.parseInt(cmd.getOptionValue("parallel"));
        daily = cmd.hasOption("daily");
        Validate.isTrue(StringUtils.isNotEmpty(biz));
        
        try {
            zk = new ZooKeeper(zkCommEndpoint, 3000, null);
            List<String> clusters = zk.getChildren(zkBase, false);
            for (String cluster : clusters) {
                List<String> bizs = zk.getChildren(zkBase + '/' + cluster, false);
                if (bizs.contains(biz)) {
                    byte[] bs = zk.getData(zkBase + '/' + cluster, false, new Stat());
                    Validate.isTrue(bs != null);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> obj = mapper.readValue(new String(bs), Map.class);
                    this.clusterZkConnStr = (String) obj.get("zk_endpoint");
                    this.cluster = cluster;
                    break;
                }
            }
            zkDaemon.start();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public String endpoint() throws UnknownHostException {
        InetAddress ia = InetAddress.getLocalHost();
        String host = ia.getHostAddress();
        String endpoint = host + ':' + consolePort;
        return endpoint;
    }
    
    public Thread zkDaemon = new Thread() {
        @Override
        public void run() {
            while (true) {
                try {
                    if (zk == null)  zk = new ZooKeeper(zkCommEndpoint, 3000, null);
                    Thread.sleep(2000);
                    if (!zk.getState().isAlive()) {
                        zk.close();
                        zk = null;
                        continue;
                    }
                    String path = "/trivial/sinkers/" + biz + '/' + endpoint();
                    if (zk.exists(path, false) == null) {
                        zk.create(path, null, acls, CreateMode.EPHEMERAL);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    };
    
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
    
    public int getParallel() {
        return parallel;
    }

    public String getHdfsEndpoint() {
        return hdfsEndpoint;
    }
    public boolean isDaily() {
        return daily;
    }

    public List<String> topics() {
        return Splitter.on(',').splitToList(biz);
    }
}
