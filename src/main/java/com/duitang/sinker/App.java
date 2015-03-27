package com.duitang.sinker;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

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
    
    private SinkerCtx conf;
    
    private final ObjectMapper mapper = new ObjectMapper();
    
    public String getLogBasePath() {
        return "/duitang/logs/usr/sinker/" + conf.getBiz();
    }
    
    public void initLog4j() {
        DailyRollingFileAppender appenderMain = new DailyRollingFileAppender();
        appenderMain.setName("msgAppender");
        appenderMain.setFile(getLogBasePath() + "/main.log");
        appenderMain.setDatePattern("'.'yyyy-MM-dd'.log'");
        appenderMain.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
        appenderMain.setThreshold(Level.DEBUG);
        appenderMain.setAppend(true);
        appenderMain.activateOptions();
        Logger.getRootLogger().addAppender(appenderMain);
        
        DailyRollingFileAppender appenderMsg = new DailyRollingFileAppender();
        appenderMsg.setName("msgAppender");
        appenderMsg.setFile(getLogBasePath() + "/msg.log");
        appenderMsg.setDatePattern("'.'yyyyMMddHH'.data'");
        appenderMsg.setLayout(new PatternLayout("%m%n"));
        appenderMsg.setThreshold(Level.DEBUG);
        appenderMsg.setAppend(true);
        appenderMsg.activateOptions();
        Logger.getLogger("msg").addAppender(appenderMsg);
    }
    
    public void initConf(String[] args) {
        Properties prop = new Properties();
        try {
            prop.put("zkCommEndpoint", args[0]);
            prop.put("biz", args[1]);
            prop.put("consolePort", args[2]);
            conf = new SinkerCtx(prop);
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
        InetSocketAddress addr = new InetSocketAddress(conf.getConsolePort());
        try {
            HttpServer s = HttpServer.create(addr, 0);
            Executor exe = Executors.newFixedThreadPool(1);
            s.setExecutor(exe);
            s.createContext("/metadata", new HttpHandler() {
                @Override
                public void handle(HttpExchange ex) throws IOException {
                    Map<String, Object> m = Maps.newHashMap();
                    m.put("success", true);
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
        
        final MsgDispatcher md = new MsgDispatcher(app.conf);
        //md.startup();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                md.doHalt();
            }
        });
        app.initConsole();
        System.out.println("init sinker:" + app.conf + " done");
    }
}
