package com.jinhongliu.hbase.balancer;

import java.util.*;

public class DataLocalityRegionCountBalancer extends RegionCountBalancer {
	public DataLocalityRegionCountBalancer() {
		super();
	}

	@Override
	public void buildPlan(List<String> needToMoveOutRegions, Map<String, Integer> needToMoveInServers) throws Exception {
		Map<String, Map<String, Float>> localityMap = this.client.getRegionDegreeLocalityMappingFromFS(this.tableName);
		this.plan = new HashMap<>();
		needToMoveOutRegions.forEach(encodedName -> {
			Map<String, Float> serverLocalityMap = localityMap.get(encodedName);
			Set<String> serverSetWithMaxLocality = getServerSetWithMaxLocality(serverLocalityMap, needToMoveInServers.keySet());
			int index = new Random().nextInt(serverSetWithMaxLocality.size());
			String serverWithMaxLocality = (String) serverSetWithMaxLocality.toArray()[index];
			this.plan.put(encodedName, this.destServerInfo.get(serverWithMaxLocality));
			needToMoveInServers.put(serverWithMaxLocality, needToMoveInServers.get(serverWithMaxLocality) - 1);
			if (needToMoveInServers.get(serverWithMaxLocality) <= 0 ) {
				needToMoveInServers.remove(serverWithMaxLocality);
			}
		});
	}

	public Set<String> getServerSetWithMaxLocality(Map<String, Float> serverLocalityMap, Set<String> candidateServers) {
		Set<String> serverHasLocality = new HashSet<>(serverLocalityMap.keySet());
		serverHasLocality.retainAll(candidateServers);

		if (serverHasLocality.size() == 0) {
			return candidateServers;
		}
		float maxLocality = -1;
		Set<String> serverWithMaxLocality = new HashSet<>();
		for (String server : serverHasLocality) {
			Float locality = serverLocalityMap.getOrDefault(server, (float) 0);
			if (locality > maxLocality) {
				maxLocality = locality;
			}
		}
		for (String server : serverHasLocality) {
			if (serverLocalityMap.getOrDefault(server, (float) 0) >= maxLocality) {
				serverWithMaxLocality.add(server);
			}
		}
		return serverWithMaxLocality;
	}
}
