package com.jinhongliu.hbase.balancer;

public class BalancerFactory {
	public static TableBalancer createBalancer(int policy) {
		if (policy == 0) {
			return new RegionCountBalancer();
		} else if (policy == 1) {
			return new DataLocalityRandomBalancer();
		} else if (policy == 2) {
			return new DataLocalityRegionCountBalancer();
		} else {
			return null;
		}
	}
}
