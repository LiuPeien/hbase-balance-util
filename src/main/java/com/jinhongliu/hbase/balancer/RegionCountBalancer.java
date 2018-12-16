package com.jinhongliu.hbase.balancer;

import java.util.*;

public class RegionCountBalancer extends TableBalancer {

	public RegionCountBalancer() {
		super();
	}

	@Override
	public void makeMovePlan() throws Exception {
		System.out.println("make move plan according region count");
		Map<String, List<String>> srcTableServerRegions = this.client.getTableServerRegionInfo(this.tableName);
		int serverCapacityTotal = 0;
		for (String hostName : this.destHostNames.keySet()) {
			serverCapacityTotal += this.destHostNames.get(hostName);
		}
		int regionTotal = this.client.getTableRegionCount(this.tableName);
		int leastRegionPerServer = regionTotal / serverCapacityTotal;
		int mostRegionPerServer = regionTotal % serverCapacityTotal == 0 ? leastRegionPerServer : leastRegionPerServer + 1;
		int serverWithMostRegion = regionTotal % serverCapacityTotal;

		List<String> needToMoveOutRegions = new ArrayList<>();
		Map<String, Integer> needToMoveInServers = new HashMap<>();
		Map<String, Integer> destServerRegionCount = new HashMap<>();

		this.destHostNames.forEach((hostName, capacity) -> {
			if (srcTableServerRegions.containsKey(hostName)) {
				destServerRegionCount.put(hostName, srcTableServerRegions.get(hostName).size());
			} else {
				destServerRegionCount.put(hostName, 0);
			}
		});

		List<Map.Entry<String,Integer>> list = new ArrayList<>(destServerRegionCount.entrySet());
		list.sort(Comparator.comparing(Map.Entry<String,Integer>::getValue));

		for (int i = list.size() - 1; i >= 0 ; i--) {
			int capacity = this.destHostNames.get(list.get(i).getKey());
			int capacities;
			if (serverWithMostRegion >= capacity) {
				capacities = mostRegionPerServer * capacity;
				serverWithMostRegion -= capacity;
			} else {
				if (serverWithMostRegion > 0) {
					capacities = mostRegionPerServer * serverWithMostRegion + leastRegionPerServer * (capacity - serverWithMostRegion);
					serverWithMostRegion = 0;
				} else {
					capacities = leastRegionPerServer * capacity;
				}
			}
			int count = list.get(i).getValue();
			if (count > capacities) {
				List<String> regions = srcTableServerRegions.get(list.get(i).getKey());
				for (int j = 0; j < count - capacities; j++) {
					needToMoveOutRegions.add(regions.get(j));
				}
			} else if (count < capacities) {
				needToMoveInServers.put(list.get(i).getKey(), capacities - count);
			}
		}

		srcTableServerRegions.forEach((hostName, regions) -> {
			if (!this.destHostNames.containsKey(hostName)) {
				needToMoveOutRegions.addAll(regions);
			}
		});

		buildPlan(needToMoveOutRegions, needToMoveInServers);
	}

	public void buildPlan(List<String> needToMoveOutRegions, Map<String, Integer> needToMoveInServers) throws Exception {
		this.plan = new HashMap<>();
		int index = 0;
		for (String server : needToMoveInServers.keySet()) {
			int count = needToMoveInServers.get(server);
			for (int i = 0; i < count; i++) {
				String serverName = this.destServerInfo.get(server);
				this.plan.put(needToMoveOutRegions.get(index), serverName);
				index++;
			}
		}
	}
}
