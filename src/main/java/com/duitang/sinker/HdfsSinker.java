package com.duitang.sinker;

import java.io.File;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * 本地文件同步至HDFS
 * 
 * HDFS对append支持的很差，如果有这种需求请使用小文件化写入。
 * 
 * @author kevx
 * @since 12/16/2014
 */
public class HdfsSinker {
	
    private final Logger log = Logger.getLogger("main");
    
	private Configuration conf = new Configuration();
	
	private String hdfsPfx = "hdfs://s4:9999";
	
	public HdfsSinker() {
	    
	}
	
	/**
	 * 写入数据仓库
	 * 
	 * 支持小文件化的写入
	 * [pt].json文件实际上是一个目录，
	 * 但是hive和spark均会将其当作一个整体文件
	 * 目录里面存储了真正包含数据的小文件，小文件的名字对应tail
	 */
	public void copyToDW(String local, String tableName, String pt, String tail) {
		StringBuilder sb = new StringBuilder();
		sb.append("/dw/");
		sb.append(tableName);
		sb.append('/');
		sb.append(pt);
		sb.append(".json");
		if (tail == null) {
		    tail = UUID.randomUUID().toString();
			sb.append("/");
			sb.append(tail);
		}
		copyToHdfs(local, sb.toString());
	}
	
	public void copyToHdfs(String local, String remote) {
		try {
			if (!remote.startsWith("/")) {
				remote = "/" + remote;
			}
			//防止传输过程中断，先写到hdfs临时目录，成功后执行原子操作rename
			Path src = new Path(local);
			Path hdfstmp = new Path(hdfsPfx + "/tmp" + remote);
			Path path = new Path(hdfsPfx + remote);
			FileSystem fs = path.getFileSystem(conf);
			fs.copyFromLocalFile(false, src, hdfstmp);
			fs.rename(hdfstmp, path);
			fs.delete(hdfstmp, true);
			fs.close();
			new File(local).renameTo(new File(local + ".done"));
		} catch (Exception e) {
		    log.error("copyToHdfs_failed:", e);
		}
	}

	public void setHdfsPfx(String hdfsPfx) {
		this.hdfsPfx = hdfsPfx;
	}
	
	
}
