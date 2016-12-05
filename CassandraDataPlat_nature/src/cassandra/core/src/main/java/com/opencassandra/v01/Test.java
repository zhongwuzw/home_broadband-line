package com.opencassandra.v01;

import java.util.Arrays;
import java.util.List;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

public class Test {
	private static Cluster cluster1 = HFactory.getOrCreateCluster(
			"Test Cluster", "192.168.18.221:9160");
	private static StringSerializer stringSerializer = StringSerializer.get();
	private static LongSerializer longSerializer = LongSerializer.get();
	
	public static void main(String []args){
		createKeyspace("test", "testData");
		Keyspace keyspaceOperator = HFactory.createKeyspace("testData", cluster1);
		try {
			Mutator<String> userMutator = HFactory.createMutator(
					keyspaceOperator, stringSerializer);

			long start = System.currentTimeMillis();
			for (long i = 0; i>=0; i++) {
				String key = i+"ixxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
				String value = i+"izzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
				System.out.println("key:" + key + ",value:" + value);
				userMutator.addInsertion(key, "test",
						HFactory.createStringColumn(key, value));
				if (i % 500 == 0) {
					System.out.println(i);
					try {
						userMutator.execute();
					} catch (Exception e) {
						try {
							userMutator.execute();
						} catch (Exception e1) {
							try {
								userMutator.execute();
							} catch (Exception e2) {
								e2.printStackTrace();
							}
						}
					}
					
				}
			}
			userMutator.execute();
			System.out.println("消耗毫秒：" + (System.currentTimeMillis() - start));
		} catch (HectorException e) {
			e.printStackTrace();
		}
	}
	public static void createKeyspace(String columnName, String keyspace) {
		KeyspaceDefinition kd = cluster1.describeKeyspace(keyspace);
		if (kd != null) {
			List<ColumnFamilyDefinition> cfList = kd.getCfDefs();
			for (int i = 0; i < cfList.size(); i++) {
				ColumnFamilyDefinition cfD = cfList.get(i);
				if (cfD.getName().equals(columnName)) {
					return;
				}
			}
			// 字段操作
			BasicColumnDefinition dataTimeColumnDefinition = new BasicColumnDefinition();
			dataTimeColumnDefinition.setName(stringSerializer
					.toByteBuffer("data_time"));
			dataTimeColumnDefinition.setIndexName(columnName + "_"
					+ "data_time_idx");
			dataTimeColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			dataTimeColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());

			ColumnFamilyDefinition cfDef = HFactory
					.createColumnFamilyDefinition(keyspace, columnName,
							ComparatorType.UTF8TYPE);
			// 下面两行我加的，为了让key和value以UTF8格式保存
			cfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
			cfDef.setDefaultValidationClass(ComparatorType.UTF8TYPE
					.getClassName());
			cfDef.addColumnDefinition(dataTimeColumnDefinition);
			
			try {
				cluster1.addColumnFamily(cfDef);
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			return;
		} else {

			// 字段操作
			BasicColumnDefinition dataTimeColumnDefinition = new BasicColumnDefinition();
			dataTimeColumnDefinition.setName(stringSerializer
					.toByteBuffer("data_time"));
			dataTimeColumnDefinition.setIndexName(columnName + "_"
					+ "data_time_idx");
			dataTimeColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			dataTimeColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());

			BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
			columnFamilyDefinition.setKeyspaceName(keyspace);
			columnFamilyDefinition.setName(columnName);
			columnFamilyDefinition
					.addColumnDefinition(dataTimeColumnDefinition);
			ColumnFamilyDefinition cfReport = new ThriftCfDef(
					columnFamilyDefinition);
			KeyspaceDefinition keyspaceDefinition = HFactory
					.createKeyspaceDefinition(keyspace,
							"org.apache.cassandra.locator.SimpleStrategy", 1,
							Arrays.asList(cfReport));
			cluster1.addKeyspace(keyspaceDefinition);
		}
	}
}
