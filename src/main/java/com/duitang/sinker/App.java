package com.duitang.sinker;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

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
            prop.put("cluster", args[1]);
            prop.put("biz", args[2]);
            prop.put("group", args[3]);
            prop.put("consolePort", args[4]);
            conf = new SinkerCtx(prop);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void initConsole(SinkerCtx ctx) {
        InetSocketAddress addr = new InetSocketAddress(ctx.getConsolePort());
        try {
            HttpServer s = HttpServer.create(addr, 0);
            Executor exe = Executors.newFixedThreadPool(1);
            s.setExecutor(exe);
            s.createContext("/metadata", new HttpHandler() {
                @Override
                public void handle(HttpExchange ex) throws IOException {
                    PrintWriter pw = new PrintWriter(ex.getResponseBody());
                    pw.println("done");
                    pw.close();
                }
            });
            
            s.createContext("/halt", new HttpHandler() {
                @Override
                public void handle(HttpExchange ex) throws IOException {
                    Runtime.getRuntime().exit(0);
                    PrintWriter pw = new PrintWriter(ex.getResponseBody());
                    pw.println("done");
                    pw.close();
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
        md.startup();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                md.doHalt();
            }
        });
        System.out.println("init sinker:" + app.conf + " done");
    }
}
