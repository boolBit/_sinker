package com.duitang.sinker;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

/**
 * 
 * @author kevx
 * @since 8:14:52 PM Mar 18, 2015
 */
public class MsgDispatcher {
    
    private final SinkerCtx ctx;
    private final String hdfsTable;
    
    private final Logger msgBuff = Logger.getLogger("msg");
    private final Logger log = Logger.getLogger("main");
    
    private final AtomicBoolean halt = new AtomicBoolean();
    private final Map<String, Integer> topicCountMap = Maps.newHashMap();
    private Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap;
    private ConsumerConnector consumer;
    private HdfsSinker hdfsSinker = new HdfsSinker();
    
    public MsgDispatcher(final SinkerCtx conf) {
        this.ctx = conf;
        hdfsTable = "t_" + conf.getBiz();
        new Thread("Hdfs_flusher") {
            @Override
            public void run() {
                while (true) {
                    //枚举目录所有*.data文件，并传送到HDFS，操作完成后将文件改名为*.data.done
                    String[] exts = new String[]{"data"};
                    try {
                        ssleep(60 * 1000);
                        Collection<File> files = FileUtils.listFiles(
                            new File(ctx.getLogBasePath()), 
                            exts, false
                        );
                        if (files == null || files.size() == 0) continue;
                        for (File f : files) {
                            if(!f.canRead() || f.length() == 0) continue;
                            String path = f.getAbsolutePath();
                            String tail = UUID.randomUUID().toString();
                            String pt = f.getName().substring(8, 16);
                            log.warn(String.format("sinking:%s@%s with:%s", path, pt, tail));
                            hdfsSinker.copyToDW(path, hdfsTable, pt, tail);//复制到hdfs，成功后改名
                        }
                    } catch (HaltException e) {
                        break;
                    } catch (Exception e) {
                        log.error("HdfsSinker_flush_failed:", e);
                    }
                }
            }
        }.start();
    }
    
    private void ssleep(int n) {
        try {
            Thread.sleep(n);
        } catch (InterruptedException e) {
            throw new HaltException();
        }
    }
    
    private ConsumerConfig createConsumerConfig() {
        String mxName = ManagementFactory.getRuntimeMXBean().getName();//pid@hostname
        String consumerId = String.format("%s-%s", ctx.getBiz(), mxName.replace('@', '-'));
        Properties props = new Properties();
        props.put("zookeeper.connect", ctx.getClusterZkConnStr());
        props.put("group.id", ctx.getGroup());
        props.put("zookeeper.session.timeout.ms", "8000");
        props.put("zookeeper.sync.time.ms", "500");
        props.put("auto.commit.interval.ms", "1000");
        props.put("refresh.leader.backoff.ms", "10000");
        props.put("consumer.id", consumerId);
        return new ConsumerConfig(props);
    }
    
    public void doHalt() {
        halt.set(true);
    }
    
    public void startup() {
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
        for (final String topic : this.ctx.topics()) {
            topicCountMap.put(topic, new Integer(1));
        }
        consumerMap = consumer.createMessageStreams(topicCountMap);
        
        for(final String topic : this.ctx.topics()) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    System.out.println("receiver_started");
                    KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
                    ConsumerIterator<byte[], byte[]> it = stream.iterator();
                    String msg;
                    while (!halt.get() && it.hasNext()) {
                        msg = new String(it.next().message());
                        msgBuff.warn(msg);
                        ctx.msgCount.incrementAndGet();
                    }
                    consumer.shutdown();
                    System.out.println("receiver_ended");
                }
            }, "sinker_consumer_" + topic).start();
        }
    }
}
