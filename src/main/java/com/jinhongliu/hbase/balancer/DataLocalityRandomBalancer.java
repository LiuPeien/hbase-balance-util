package com.jinhongliu.hbase.balancer;

import java.util.*;


public class DataLocalityRandomBalancer extends DataLocalityRegionCountBalancer {
	float threshold = 0;

	public DataLocalityRandomBalancer() {
		super();
	}

	public void setThreshold(float threshold) {
		this.threshold = threshold;
	}

	@Override
	public void makeMovePlan() throws Exception {
		Map<String, Map<String, String>> tableRegionsInfo = this.client.getTableRegionsInfo(this.tableName);
		int regionTotal = tableRegionsInfo.size();
		List<String> needToMoveOutRegions = new ArrayList<>();
		Map<String, Integer> needToMoveInServers = new HashMap<>();
		tableRegionsInfo.forEach((region, info) -> {
			needToMoveOutRegions.add(region);
		});
		for (String hostName : this.destHostNames.keySet()) {
			needToMoveInServers.put(hostName, regionTotal);
		}
		buildPlan(needToMoveOutRegions, needToMoveInServers);
	}
}