package com.jinhongliu.hbase.util;

import java.util.HashMap;
import java.util.Map;

public class ParamParser {
	Map<String, String> options = null;

	public ParamParser(String[] args) {
		options = new HashMap<>();
		for (String arg : args) {
			String[] option = arg.split("=");
			if (option.length != 2) {
				System.out.println("invalid arg " + arg);
				return;
			}
			options.put(option[0], option[1]);
		}
	}

	public String getStringOption(String key) {
		return options.get(key);
	}

	public String getStringOptionOrDefault(String key, String defaultValue) {
		return options.getOrDefault(key, defaultValue);
	}

	public Boolean getBooleanOption(String key) {
		String value = getStringOption(key);
		if (value == null) {
			return null;
		}
		return Boolean.valueOf(value);
	}

	public Boolean getBooleanOptionOrDefault(String key, boolean defaultValue) {
		String value = getStringOption(key);
		if (value == null) {
			return defaultValue;
		}
		return Boolean.valueOf(value);
	}

	public Integer getIntegerOption(String key) {
		String value = getStringOption(key);
		if (value == null) {
			return null;
		}
		return Integer.valueOf(value);
	}

	public Integer getIntegerOptionOrDefault(String key, int defaultValue) {
		String value = getStringOption(key);
		if (value == null) {
			return defaultValue;
		}
		return Integer.valueOf(value);
	}
}
