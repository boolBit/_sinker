package com.duitang.sinker;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.Maps;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * sinker
 * @author kevx
 * @since 03/18/2015
 */
public class App {
    
    private SinkerCtx ctx;
    
    private final ObjectMapper mapper = new ObjectMapper();
    
    public void initLog4j() {
        DailyRollingFileAppender appenderMain = new DailyRollingFileAppender();
        appenderMain.setName("msgAppender");
        appenderMain.setFile(ctx.getLogBasePath() + "/main.log");
        appenderMain.setDatePattern("'.'yyyy-MM-dd'.log'");
        appenderMain.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
        appenderMain.setThreshold(Level.DEBUG);
        appenderMain.setAppend(true);
        appenderMain.activateOptions();
        Logger.getRootLogger().addAppender(appenderMain);
        
        DailyRollingFileAppender appenderMsg = new DailyRollingFileAppender();
        appenderMsg.setName("msgAppender");
        appenderMsg.setFile(ctx.getLogBasePath() + "/msg.log");
        if (ctx.isDaily()) {
            appenderMsg.setDatePattern("'.'yyyyMMdd'.data'");
        } else {
            appenderMsg.setDatePattern("'.'yyyyMMddHH'.data'");
        }
        appenderMsg.setLayout(new PatternLayout("%m%n"));
        appenderMsg.setThreshold(Level.DEBUG);
        appenderMsg.setAppend(true);
        appenderMsg.activateOptions();
        Logger.getLogger("msg").addAppender(appenderMsg);
    }
    
    public void initConf(String[] args) {
        try {
            Options options = new Options();
            options.addOption("zkcomm", true, "zk comm endpoint");
            options.addOption("zkkafka", true, "zk kafka endpoint");
            options.addOption("biz", true, "biz name");
            options.addOption("port", true, "console port");
            options.addOption("hdfs", true, "hdfs endpoint");
            options.addOption("parallel", true, "parallel thread count");
            options.addOption("cluster", true, "kafka cluster name");
            options.addOption("topics", true, "topics");
            options.addOption("daily", false, "daily flush data to hdfs (hourly by default)");
            CommandLineParser parser = new PosixParser();
            CommandLine cmd = parser.parse( options, args);
            ctx = new SinkerCtx(cmd);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private String json(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }
    
    public void initConsole() {
        InetSocketAddress addr = new InetSocketAddress(ctx.getConsolePort());
        try {
            HttpServer s = HttpServer.create(addr, 0);
            Executor exe = Executors.newFixedThreadPool(1);
            s.setExecutor(exe);
            s.createContext("/", new HttpHandler() {
                @Override
                public void handle(HttpExchange ex) throws IOException {
                    Map<String, Object> m = Maps.newHashMap();
                    m.put("success", true);
                    m.put("msgcount", ctx.msgCount.get());
                    String data = json(m);
                    ex.sendResponseHeaders(200, data.getBytes().length + 1);
                    PrintWriter pw = new PrintWriter(ex.getResponseBody());
                    pw.println(data);
                    pw.close();
                }
            });
            
            s.createContext("/halt", new HttpHandler() {
                @Override
                public void handle(HttpExchange ex) throws IOException {
                    Map<String, Object> m = Maps.newHashMap();
                    m.put("success", true);
                    String data = json(m);
                    ex.sendResponseHeaders(200, data.getBytes().length + 1);
                    PrintWriter pw = new PrintWriter(ex.getResponseBody());
                    pw.println(data);
                    pw.close();
                    new Timer().schedule(new TimerTask() {
                        @Override
                        public void run() {
                            Runtime.getRuntime().exit(0);
                        }
                    }, 1000);
                }
            });
            s.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        App app = new App();
        app.initConf(args);
        app.initLog4j();
        
        final MsgDispatcher md = new MsgDispatcher(app.ctx);
        md.startup();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                md.doHalt();
            }
        });
        app.initConsole();
        System.out.println("init sinker:" + app.ctx + " done");
    }
}
