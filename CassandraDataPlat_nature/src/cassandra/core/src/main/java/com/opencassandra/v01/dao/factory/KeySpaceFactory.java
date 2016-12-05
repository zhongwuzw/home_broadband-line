package com.opencassandra.v01.dao.factory;

import java.util.HashMap;
import java.util.List;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

public class KeySpaceFactory {
	private static final String DYN_KEYSPACE = "logdb";
	
	public static Cluster cluster = HFactory.getOrCreateCluster(
			"Test Cluster", "192.168.85.225:9160");
	public static StringSerializer stringSerializer = StringSerializer.get();
	public static LongSerializer longSerializer = LongSerializer.get();
	
	private static HashMap<String, Keyspace> keyspaceList = new HashMap<String, Keyspace>();
	
	public static Keyspace createKeyspace(String keyspaceName) {
		try {
			if (cluster.describeKeyspace(keyspaceName) == null) {
				KeyspaceDefinition keyspaceDefinition = HFactory
						.createKeyspaceDefinition(keyspaceName);
				cluster.addKeyspace(keyspaceDefinition);
			}
			Keyspace keyspaceOperator = HFactory.createKeyspace(keyspaceName, cluster);
			keyspaceList.put(keyspaceName, keyspaceOperator);
			return keyspaceOperator;
		} catch (HectorException he) {
			he.printStackTrace();
			return null;
		}
	}
	
	public static String createCF(ColumnFamilyDefinition cfName) {
		try {
			return cluster.addColumnFamily(cfName);
		} catch (HectorException he) {
			he.printStackTrace();
			return null;
		}
	}
	
	public static String createorupdateCF(ColumnFamilyDefinition cfName) {
		try {
			if(exist(cfName)){
				System.out.println("here");
				return cluster.updateColumnFamily(cfName);
			}
			System.out.println("there");
			return cluster.addColumnFamily(cfName);
		} catch (HectorException he) {
			he.printStackTrace();
			return null;
		}
	}
	
	public static boolean exist(ColumnFamilyDefinition cfName) {
		boolean isExist = false;
		try {
			List<KeyspaceDefinition> keyspaces = cluster
					.describeKeyspaces();
			for (KeyspaceDefinition kd : keyspaces) {
				if (kd.getName().equals(cfName.getKeyspaceName())) {
					List<ColumnFamilyDefinition> cfDefs = kd.getCfDefs();
					for (ColumnFamilyDefinition def : cfDefs) {
						if(def.getName().equals(cfName.getName())){
							isExist = true;
						}
					}

				}
			}
		} catch (HectorException he) {
			he.printStackTrace();
		}
		return isExist;
	}

	static public Keyspace getKeyspace(String keyspaceName){
		if(keyspaceList.get(keyspaceName)==null){
			return createKeyspace(keyspaceName);
		}
		return keyspaceList.get(keyspaceName);
	}
	
	static public Cluster getInstance(){
		if(cluster==null){
			return HFactory.getOrCreateCluster(
					"Test Cluster", "192.168.118.128:9160");
		}
		return cluster;
	}
	
}
