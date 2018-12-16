package com.jinhongliu.hbase.balancer;

import com.jinhongliu.hbase.util.ParamParser;

import java.io.*;
import java.util.*;

public abstract class TableBalancer {
	String zk = null;
	String zkPath = null;
	String tableName = null;
	Map<String, Integer> destHostNames = null;
	Map<String,String> destServerInfo = null;
	Map<String, String> plan = null;
	HBaseClient client = null;

	TableBalancer() {}

	public void init(String tableName, String serverFile, String zk, String zkPath) throws Exception {
		this.zk = zk;
		this.zkPath = zkPath;
		this.tableName = tableName;
		loadServers(serverFile);
		if (client != null) {
			client.close();
		}
		this.client = new HBaseClient(zk, zkPath);
		this.destServerInfo = client.getServersInfo(this.destHostNames.keySet());
	}

	public void close() throws Exception {
		client.close();
	}

	public void loadServers(String serverFile) throws Exception {
		this.destHostNames = new HashMap<>();
		File file = new File(serverFile);
		FileReader input = new FileReader(file);
		BufferedReader reader = new BufferedReader(input);
		String line  = "";
		while((line = reader.readLine()) != null) {
			String ss[] = line.trim().split(" ");
			if (ss.length != 2) {
				System.out.println("invalid server: " + line);
				continue;
			}
			int capacity = Integer.valueOf(ss[1]);
			if (capacity > 0) {
				this.destHostNames.put(ss[0], capacity);
			}
		}
		reader.close();
	}

	public void savePlan(String fileName) throws Exception {
		File file = new File(fileName);
		FileWriter output = new FileWriter(file);
		BufferedWriter writer = new BufferedWriter(output);

		for (String s : plan.keySet()) {
			String line = String.format("move \'%s\', \'%s\'", s, plan.get(s));
			writer.write(line);
			writer.newLine();
		}
		writer.close();
	}

	abstract void makeMovePlan() throws Exception;

	void doBalance(boolean realRun, int interval) throws Exception {
		if (realRun) {
			plan.forEach((k,v) -> {
				try {
					client.moveRegion(k, v);
				} catch (Exception e) {
					System.out.println(String.format("fail to move region: %s to server: %s", k, v));
				}
			});
		} else {
			plan.forEach((k,v) -> {
				System.out.println(k + " -> " + v);
			});
		}
	}

	public static void main(String[] args) throws Exception {
		ParamParser parser = new ParamParser(args);

		String table = parser.getStringOption("table");
		String server_file = parser.getStringOption("server_file");
		String zk = parser.getStringOption("zk");
		String zk_path = parser.getStringOption("zk_path");
		String plan_file = parser.getStringOption("plan_file");
		int policy = parser.getIntegerOptionOrDefault("policy", 0);
		int interval = parser.getIntegerOptionOrDefault("interval", 10);
		boolean real_run = parser.getBooleanOptionOrDefault("real_run", false);

		if (table == null) {
			throw new Exception("you have to specify the table!");
		}
		if (server_file == null) {
			throw new Exception("you have to specify the server_file!");
		}
		if (zk == null) {
			throw new Exception("you have to specify the zk!");
		}
		if (zk_path == null) {
			throw new Exception("you have to specify the zk_path!");
		}
		if (plan_file == null) {
			throw new Exception("you have to specify the plan_file!");
		}

		TableBalancer balancer = BalancerFactory.createBalancer(policy);
		if (balancer instanceof DataLocalityRandomBalancer) {
			if (parser.getStringOption("threshold") != null) {
				float threshold = Float.valueOf(parser.getStringOption("threshold"));
				((DataLocalityRandomBalancer) balancer).setThreshold(threshold);
			}
		}

		balancer.init(table, server_file, zk, zk_path);
		balancer.makeMovePlan();
		balancer.savePlan(plan_file);
		balancer.doBalance(real_run, interval);
		balancer.close();
	}
}
