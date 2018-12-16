package com.jinhongliu.hbase.keeper;

import com.jinhongliu.hbase.balancer.HBaseClient;
import com.jinhongliu.hbase.util.ParamParser;

import java.io.*;
import java.util.*;

public class HbaseLocationKeeper {

	public static void saveLocation(String tableName, String fileName, String zk, String zkPath) throws Exception {
		HBaseClient client = new HBaseClient(zk, zkPath);
		List<String> location = new ArrayList<>();
		Map<String, Map<String, String>> tableRegionInfo = client.getTableRegionsInfo(tableName);

		for (String region : tableRegionInfo.keySet()) {
			location.add(region + " " + tableRegionInfo.get(region).get("hostName"));
		}
		client.close();
		saveToFile(location, fileName);
	}

	public static void saveToFile(List<String> location, String fileName) throws Exception {
		File file = new File(fileName);
		FileWriter output = new FileWriter(file);
		BufferedWriter writer = new BufferedWriter(output);

		for (String l : location) {
			writer.write(l);
			writer.newLine();
		}
		writer.close();
	}

	public static Map<String, String> loadFromFile(String fileName) throws Exception {
		Map<String, String> location = new HashMap<>();
		File file = new File(fileName);
		FileReader input = new FileReader(file);
		BufferedReader reader = new BufferedReader(input);
		String line  = "";
		while((line = reader.readLine()) != null) {
			if(line.length() > 0) {
				String[] ss = line.split(" ");
				if (ss.length != 2) {
					System.out.println("invalid line: " + line);
				}
				location.put(ss[0], ss[1]);
			}
		}
		reader.close();
		return location;
	}

	public static void recoverLocation(String fileName, String zk, String zkPath) throws Exception {
		Map<String, String> location = loadFromFile(fileName);
		HBaseClient client = new HBaseClient(zk, zkPath);

		Map<String, String> serversInfo = client.getServersInfo(location.keySet());
		for (String region : location.keySet()) {
			String realName = serversInfo.get(location.get(region));
			client.moveRegion(region, realName);
		}
		client.close();
	}

	public static void main(String[] args) throws Exception{
		ParamParser parser = new ParamParser(args);

		String zk = parser.getStringOption("zk");
		String zk_path = parser.getStringOption("zk_path");
		String action = parser.getStringOption("action");
		String table = parser.getStringOption("table");
		String location_file = parser.getStringOption("location_file");

		if (action.equals("save")) {
			saveLocation(table, location_file, zk, zk_path);
		} else if (action.equals("recover")) {
			recoverLocation(location_file, zk, zk_path);
		}
	}
}
