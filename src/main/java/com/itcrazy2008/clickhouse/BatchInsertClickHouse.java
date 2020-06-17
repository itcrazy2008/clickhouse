package com.itcrazy2008.clickhouse;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

public class BatchInsertClickHouse {
	
	private static Logger logger = Logger.getLogger(BatchInsertClickHouse.class);
	
	public static void main(String[] args) {		
		String ip = null;
		int port = 8123;//默认数据库端口 8123
		String db = "metrics";//默认数据库metrics
		String table = "samples";//默认数据表samples
		
		int threads = 10;//默认启动10个线程
		int batchsize = 10;//一次批量提交的数据量,默认10W，单位为万
		String date = null;//默认当天
		
		int count=0;
		while (count < args.length) {
			if (args[count].equalsIgnoreCase("-ip")) {
				if (count + 1 >= args.length)
					break;
				ip = args[count + 1];
				count = count + 2; 
			}else if (args[count].equalsIgnoreCase("-port")) {
				if (count + 1 >= args.length)
					break;
				port = new Integer(args[count + 1]).intValue();
				count = count + 2; 
			}else if (args[count].equalsIgnoreCase("-db")) {
				if (count + 1 >= args.length)
					break;
				db = args[count + 1];
				count = count + 2; 
			}else if (args[count].equalsIgnoreCase("-table")) {
				if (count + 1 >= args.length)
					break;
				table = args[count + 1];
				count = count + 2; 
			}else if (args[count].equalsIgnoreCase("-batchsize")) {
				if (count + 1 >= args.length)
					break;
				batchsize = new Integer(args[count + 1]).intValue();
				count = count + 2; 
			}else if (args[count].equalsIgnoreCase("-date")) {
				if (count + 1 >= args.length)
					break;
				date = args[count + 1];
				count = count + 2; 
			}else if (args[count].equalsIgnoreCase("-threads")) {
				if (count + 1 >= args.length)
					break;
				threads = new Integer(args[count + 1]).intValue();
				count = count + 2; 
			}else if (args[count].equalsIgnoreCase("-help")) {
				logger.info("usage: ./batchInsertClickHouse.sh -ip ip [-port port] [-db db] [-table table] [-batchsize batchsize] [-date date] [-threads threads] ");
				logger.info("  ip : database ip address ");
				logger.info("  port : database port.default 8123 ");
				logger.info("  db : database name.default metrics ");
				logger.info("  table : database table.default samples ");
				
				logger.info("  batchsize : default 10,unit W.eg 1=10000 ");
				logger.info("  date : default date(yyyy-MM-dd),current date ");
				logger.info("  threads : default 10 ");
				return ; 
			}
			else
				count++;
		}	

		if(StringUtils.isEmpty(ip)) {
			logger.info("ip is null.");
			return ;
		}
		
		batchsize = batchsize * 10000;//单位为万
		
		for (int i = 0; i < threads; i++) {
			try {
				DBThread dbThread = new DBThread(ip,port,db,table,batchsize,date,threads);
				Thread thread = new Thread(dbThread);
				thread.setName("DBThread-thread-" + i);
				thread.start();
			} catch (Exception e) {
				logger.error("Exception",e);
			}
		}

	}

	public static class DBThread implements Runnable {
			
		String ip;
		int port;
		String db;
		String table;
		
		int batchsize;
		String date;
		int threads;
		
		public DBThread(String ip,int port,String db,String table,int batchsize,String date,int threads) {
			this.ip = ip;
			this.port = port;
			this.db = db;			
			this.table = table;
			
			this.batchsize = batchsize;			
			this.date = date;
			this.threads = threads;
		}
		
		@Override
		public void run() {
			String url = "jdbc:clickhouse://"+ip+":"+port+"/"+db;
			ClickHouseDataSource ds = new ClickHouseDataSource(url);
			Connection conn = null;
			PreparedStatement pstmt = null;
			
			try {
				conn = ds.getConnection();
				conn.setAutoCommit(false);					
				String sql = "INSERT INTO "+table+" (date, name, tags, val, ts, updated) VALUES (?, ?,?, ?,?, ?)";
				pstmt = conn.prepareStatement(sql);
			} catch (Exception e) {
				logger.error("Exception",e);
			}

			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			SimpleDateFormat format_ts = new SimpleDateFormat("yyyy-MM-dd HH");

			//如果指定了日期，则只插入指定日期的数据，如果没有指定日期，则插入当天的数据
			Date date = null;
			if(!StringUtils.isEmpty(this.date)) {
				try {
					date = format.parse(this.date);
				} catch (Exception e) {
					logger.error("Exception",e);
				}
			}
			
			while (true) {
				long start = System.currentTimeMillis();
				try {					
					Date tsHour = date;//yyyy-MM-dd 00|HH
					if(StringUtils.isEmpty(this.date)) {
						Date curTime = new Date();
						date = format.parse(format.format(curTime));
						tsHour = format_ts.parse(format_ts.format(curTime));
					}
					String dateStr = format.format(date);//yyyy-MM-dd
					
					for (int i = 0; i < batchsize; i++) {
						int index = 1;
						//Date date = new Date(new Date().getTime() - (new Integer(gen(3))%730)*24*60*60*1000L);	//2年,不随机产生(因随机产生一旦数据量大，则合并量大，和实际的场景的时间序列不符合)
						
						Integer cpuid = new Integer(gen(1)) % 10;//10个cpu;
						String vnodeid = gen(1);//10个vnode
						String name = "node_cpu_frequency_min_hertz_" + gen(2) + cpuid + vnodeid;//1W指标
						
						//2W IP
						int ip3 = new Integer(gen(3)) % 255;//0~255
						int ip4 = new Integer(gen(3)) % 80;//0~25
						String ip = "192.16" + new Integer(gen(1)) % 3 + "." + ip3 + "." + ip4;//192.[160|161|162]
						String job = "node_" + ip;
						String cpu = "cup_" + cpuid;
						String vnode = "vnode_" + vnodeid;
						String [] array = {"__name__="+name,"instance="+ip+":29100","job="+job,"cpu="+cpu,"vnode="+vnode};
						Array tags = new ClickHouseArray(ClickHouseDataType.String, array);
						
						Double val = new Double(gen(6))/5000.0;
						
						Date ts = new Date(tsHour.getTime() + new Integer(gen(2))%11*5*60*1000L);//5分钟粒度
						
						pstmt.setString(index++,dateStr);
						pstmt.setString(index++,name);
						pstmt.setArray(index++,tags);
						pstmt.setDouble(index++,val);
						pstmt.setTimestamp(index++,new Timestamp(ts.getTime()));
						pstmt.setTimestamp(index++,new Timestamp(ts.getTime()));
						pstmt.addBatch();
					}
					pstmt.executeBatch();
					conn.commit();
				} catch (Exception e) {
					logger.error("Exception",e);
				}
				long end = System.currentTimeMillis();
				long interval = end - start;
				logger.info("interval-->>::" + interval + "ms (" + interval/1000 + "s)  batchsize-->>::" + batchsize);
			}
		}

		/**
		 * 生成bits位的数字
		 * 
		 * @param bits
		 * @return
		 */
		public String gen(int bits) {
			return new String("" + Math.random()).substring(2, 2 + bits);// bits位随机数
		}
	}
}
