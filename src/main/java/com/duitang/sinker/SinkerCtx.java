package com.duitang.sinker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * 
 * @author kevx
 * @since 8:16:52 PM Mar 18, 2015
 */
public class SinkerCtx {

    private ZooKeeper zk;
    private final String group = "sinker";
    private final String hdfsEndpoint;
    private final String zkCommEndpoint;
    private final String zkKafkaEndpoint;
    private final String cluster;
    private final String biz;
    private final List<String> topics = Lists.newArrayList();
    private final int consolePort;
    private final int parallel;
    private final boolean daily;
    
    public final AtomicLong msgCount = new AtomicLong();
    
    private static final List<ACL> acls = Lists.newArrayList(new ACL(Perms.ALL, Ids.ANYONE_ID_UNSAFE));
    
    public String getLogBasePath() {
        return "/duitang/logs/usr/sinker/" + biz;
    }
    
    public SinkerCtx(CommandLine cmd) {
        biz = cmd.getOptionValue("biz");
        cluster = cmd.getOptionValue("cluster");
        hdfsEndpoint = cmd.getOptionValue("hdfs");
        zkCommEndpoint = cmd.getOptionValue("zkcomm");
        zkKafkaEndpoint = cmd.getOptionValue("zkkafka");
        consolePort = Integer.parseInt(cmd.getOptionValue("port"));
        parallel = Integer.parseInt(cmd.getOptionValue("parallel"));
        daily = cmd.hasOption("daily");
        String topicsStr = null;
        if (cmd.hasOption("topics")) {
            topicsStr = cmd.getOptionValue("topics");
        }
        Set<String> topicSet = Sets.newHashSet();
        if (topicsStr != null) {
            topicSet.addAll(Splitter.on(',').splitToList(topicsStr));
        }
        topicSet.add(biz);
        topics.addAll(topicSet);
        
        Validate.isTrue(StringUtils.isNotEmpty(biz));
        Validate.isTrue(topics.size() > 0);
        zkDaemon.start();
        
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
    
    public String getZkKafkaEndpoint() {
        return zkKafkaEndpoint;
    }

    @Override
    public String toString() {
        return String.format("cluster:%s;biz:%s;group:%s", cluster, biz, group);
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

    public long sinkerTimeout() {
        if (daily) return 24 * 60 * 60 * 1000L;
        else return 60 * 60 * 1000L;
    }
    
    public List<String> topics() {
        return topics;
    }
}
