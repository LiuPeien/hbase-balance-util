package com.jinhongliu.hbase.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

import java.util.*;

public class HBaseClient {
	Connection connection = null;
	Admin admin = null;

	public HBaseClient(String zk, String zkPath) {
		Configuration conf;
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", zk);
		conf.set("hbase.zookeeper.property.clientPort","2181");
		conf.set("zookeeper.znode.parent", zkPath);
		try {
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
		} catch (Exception e) {
			System.out.println("failed to get connection");
			System.out.println(e.toString());
		}
	}

	public void close() throws Exception{
		if (admin != null) {
			admin.close();
		}
		if (connection != null) {
			connection.close();
		}
	}

	public void moveRegion(String region, String server) throws Exception {
		admin.move(Bytes.toBytes(region), Bytes.toBytes(server));

	}

	public Map<String, Map<String, String>> getTableRegionsInfo(String tableName) throws Exception {
		RegionLocator locations = connection.getRegionLocator(TableName.valueOf(tableName));
		List<HRegionLocation> regionLocations = locations.getAllRegionLocations();
		Map<String, Map<String, String>> tableRegionsInfo = new HashMap<>();

		for (HRegionLocation location : regionLocations) {
			HRegionInfo info = location.getRegionInfo();
			ServerName server = location.getServerName();
			Map<String, String> regionInfo = new HashMap<>();
			regionInfo.put("regionName", Bytes.toString(info.getRegionName()));
			regionInfo.put("hostName", server.getHostname());
			regionInfo.put("serverName", server.getServerName());
			tableRegionsInfo.put(info.getEncodedName(), regionInfo);
		}
		return tableRegionsInfo;
	}

	public Map<String, List<String>> getTableServerRegionInfo(String tableName) throws Exception {
		RegionLocator locations = connection.getRegionLocator(TableName.valueOf(tableName));
		List<HRegionLocation> regionLocations = locations.getAllRegionLocations();
		Map<String, List<String>> tableServerRegionInfo  = new HashMap<>();

		for (HRegionLocation location : regionLocations) {
			HRegionInfo info = location.getRegionInfo();
			ServerName server = location.getServerName();
			if (!tableServerRegionInfo.containsKey(server.getHostname())) {
				tableServerRegionInfo.put(server.getHostname(), new ArrayList<>());
			}
			tableServerRegionInfo.get(server.getHostname()).add(info.getEncodedName());
		}
		return tableServerRegionInfo;
	}

	public int getTableRegionCount(String tableName) throws Exception {
		RegionLocator locations = connection.getRegionLocator(TableName.valueOf(tableName));
		return locations.getAllRegionLocations().size();
	}

	public List<String> getTableRegions(String tableName) throws Exception {
		RegionLocator locations = connection.getRegionLocator(TableName.valueOf(tableName));
		List<HRegionLocation> regionLocations = locations.getAllRegionLocations();
		List<String> tableRegions = new ArrayList<>();
		for (HRegionLocation location : regionLocations) {
			tableRegions.add(location.getRegionInfo().getEncodedName());
		}
		return tableRegions;
	}

	public List<String> getTableServers(String tableName) throws Exception {
		RegionLocator locations = connection.getRegionLocator(TableName.valueOf(tableName));
		List<HRegionLocation> regionLocations = locations.getAllRegionLocations();
		List<String> tableServers = new ArrayList<>();
		for (HRegionLocation location : regionLocations) {
			tableServers.add(location.getServerName().getServerName());
		}
		return tableServers;
	}

	public Map<String, Map<String, Float>> getRegionDegreeLocalityMappingFromFS(String tableName) throws Exception {
		Configuration conf = connection.getConfiguration();
		return FSUtils.getRegionDegreeLocalityMappingFromFS(conf, tableName, 5);
	}

	public Map<String, String> getServersInfo(Set<String> hostNames) throws Exception {
		ClusterStatus cs = admin.getClusterStatus();
		Map<String, String> serversInfo = new HashMap<>();
		for(ServerName server : cs.getServers()) {
			String name = server.getServerName();
			String ss[] = name.split(",");
			if (hostNames.contains(ss[0])) {
				serversInfo.put(ss[0], name);
			}
		}
		return serversInfo;
	}
}
