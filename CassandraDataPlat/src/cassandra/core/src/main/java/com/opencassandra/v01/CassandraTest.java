package com.opencassandra.v01;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.cassandra.thrift.IndexOperator;
import org.apache.commons.lang.math.RandomUtils;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.IndexedSlicesPredicate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

public class CassandraTest {
	private static final String DYN_KEYSPACE = "logdb";
	private static final String DYN_CF = "user";

	private static StringSerializer stringSerializer = StringSerializer.get();
	private static LongSerializer longSerializer = LongSerializer.get();
	private static Cluster cluster = HFactory.getOrCreateCluster(
			"Test Cluster", "192.168.85.225:9160");

	public static void createSchema() {

		try {
			if (cluster.describeKeyspace(DYN_KEYSPACE) != null) {
				cluster.dropKeyspace(DYN_KEYSPACE);
			}

			// 字段操作
			BasicColumnDefinition ageColumnDefinition = new BasicColumnDefinition();
			ageColumnDefinition.setName(stringSerializer.toByteBuffer("age"));
			ageColumnDefinition.setIndexName("age_idx");
			ageColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			ageColumnDefinition.setValidationClass(ComparatorType.LONGTYPE
					.getClassName());

			// 字段操作
			BasicColumnDefinition usernameColumnDefinition1 = new BasicColumnDefinition();
			usernameColumnDefinition1.setName(stringSerializer
					.toByteBuffer("username"));
			usernameColumnDefinition1.setIndexName("username_idx");
			usernameColumnDefinition1.setIndexType(ColumnIndexType.KEYS);
			usernameColumnDefinition1
					.setValidationClass(ComparatorType.UTF8TYPE.getClassName());

			BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
			columnFamilyDefinition.setKeyspaceName(DYN_KEYSPACE);
			columnFamilyDefinition.setName(DYN_CF);
			columnFamilyDefinition.addColumnDefinition(ageColumnDefinition);
			columnFamilyDefinition
					.addColumnDefinition(usernameColumnDefinition1);

			ColumnFamilyDefinition cfUser = new ThriftCfDef(
					columnFamilyDefinition);
			
			KeyspaceDefinition keyspaceDefinition = HFactory
					.createKeyspaceDefinition(DYN_KEYSPACE,
							"org.apache.cassandra.locator.SimpleStrategy", 1,
							Arrays.asList(cfUser));

			cluster.addKeyspace(keyspaceDefinition);

			// insert some data

			for (int i = 0; i < 1; i++) {
				List<KeyspaceDefinition> keyspaces = cluster
						.describeKeyspaces();
				for (KeyspaceDefinition kd : keyspaces) {
					if (kd.getName().equals(DYN_KEYSPACE)) {
						System.out.println("Name: " + kd.getName());
						System.out.println("RF: " + kd.getReplicationFactor());
						System.out.println("strategy class: "
								+ kd.getStrategyClass());
						List<ColumnFamilyDefinition> cfDefs = kd.getCfDefs();
						for (ColumnFamilyDefinition def : cfDefs) {
							System.out.println("  CF Type: "
									+ def.getColumnType());
							System.out.println("  CF Name: " + def.getName());
							System.out.println("  CF Metadata: "
									+ def.getColumnMetadata());

						}

					}
				}
			}

		} catch (HectorException he) {
			he.printStackTrace();
		}
		cluster.getConnectionManager().shutdown();
	}

	public static void operatioinDB() {
		String value = "";
		for (int i = 0; i < 10; i++) {
			value = value + "x";
		}

		Keyspace keyspaceOperator = HFactory.createKeyspace("logdb", cluster);

		try {
			Mutator<String> userMutator = HFactory.createMutator(
					keyspaceOperator, stringSerializer);

			Long age = 1000L;
			long start = System.currentTimeMillis();
			for (long i = 0; i < 100 * 100; i++) {
				userMutator.addInsertion("" + i, "user",
						HFactory.createStringColumn("username", "小贩" + i))
						.addInsertion(
								"" + i,
								"user",
								HFactory.createColumn("age", i,
										stringSerializer, longSerializer));
				if (i % 500 == 0) {
					System.out.println(i);
					userMutator.execute();
				}

			}
			userMutator.execute();
			System.out.println("消耗毫秒：" + (System.currentTimeMillis() - start));

		} catch (HectorException e) {
			e.printStackTrace();
		}
		cluster.getConnectionManager().shutdown();
	}

	public static void query() {
		
		int count = 0;
		//EQ代表“=”，GTE代表“>=”，GT代表“>”，LTE代表“<=”，LT代表“<”
		
		Keyspace keyspaceOperator = HFactory.createKeyspace("CMCC_ZB_SHICHANG_PRJ_E2E_HUAINAN",
				cluster);
		//long value = RandomUtils.nextInt(10000 * 1);
		ColumnFamilyTemplate<String, String> columnFamilyTemplate = new ThriftColumnFamilyTemplate<String, String>(
				keyspaceOperator, "03001", stringSerializer, stringSerializer);
		IndexedSlicesPredicate<String, String, String> usernamePredicate = new IndexedSlicesPredicate<String, String, String>(
				stringSerializer, stringSerializer, stringSerializer);
//		usernamePredicate.addExpression("year_month_day", IndexOperator.EQ,
//				"20141025").addExpression("百度地理位置", IndexOperator.EQ,
//						"北京市朝阳区s11").startKey("");
		usernamePredicate.addExpression("year_month_day", IndexOperator.EQ,
				"20140804").count(1).startKey("863798020138793|00000000_03001.1872_0-2014_08_04_15_50_02_661.abstract.csv_8");
		//agePredicate.addExpression("age", IndexOperator.EQ, value).startKey("");
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss.SSS");
		System.out.println("Start Reading---->"+(dateFormat.format(new Date())));
		
		ColumnFamilyResult<String, String> list = columnFamilyTemplate
				.queryColumns(usernamePredicate);
		//System.out.println(list.getString("username"));
		System.out.println("Stop Reading---->"+(dateFormat.format(new Date())));
		
		
		while (list.hasResults()) {
			Collection<String> element = list.getColumnNames();
			Object[] columns = element.toArray();
			System.out.println("KEY : "+list.getKey());
			count++;
			for(int i=0; i<columns.length; i++){
				System.out.println(""+columns[i]+" : "+list.getString(""+columns[i]));
			}
			if(list.hasNext()){
				list.next();
			}else{
				break;
			}
		}
		System.out.println("总处理数据量"+count);
		//list = columnFamilyTemplate.queryColumns(agePredicate);
		//System.out.println(list.getLong("age"));
		// ======================================================================================

	}

	public static void query2() {
		/*
		 * Keyspace keyspaceOperator = HFactory.createKeyspace("logdb",
		 * cluster); long value = RandomUtils.nextInt(10000 * 1);
		 * IndexedSlicesQuery<String, String, String> indexedSlicesQuery =
		 * HFactory .createIndexedSlicesQuery(keyspaceOperator,
		 * stringSerializer, stringSerializer, stringSerializer);
		 * indexedSlicesQuery.addEqualsExpression("username", "小贩" + value);
		 * indexedSlicesQuery.setColumnFamily("user");
		 * indexedSlicesQuery.setStartKey("");
		 * 
		 * indexedSlicesQuery.setColumnNames("username", "age");
		 * QueryResult<OrderedRows<String, String, String>> result =
		 * indexedSlicesQuery .execute(); System.out.println(result.get());
		 */
		/*
		Keyspace keyspace = HFactory.createKeyspace("logdb",cluster);
		RangeSlicesQuery<String, String, String> rangeQuery = HFactory
				.createRangeSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);

		rangeQuery.setColumnFamily("user");
		long value = RandomUtils.nextInt(10000 * 1);
		rangeQuery.addEqualsExpression("username", "小贩" + value);
		rangeQuery.setColumnNames("username", "age");

		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		
		System.out.println(oRows);
		
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			System.out.println("key: " + row.getKey());
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			for (HColumn<String, String> hColumn : columns) {
				System.out.println("  column name: " + hColumn.getName()
						+ " value: " + hColumn.getValue());
			}
		}
		*/
		Keyspace keyspace = HFactory.createKeyspace("EN_CMCC_CMPAK_DEFAULT",cluster);
//		Keyspace keyspace = HFactory.createKeyspace("GEO_DATA",cluster);
		RangeSlicesQuery<String, String, String> rangeQuery = HFactory
				.createRangeSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);

//		rangeQuery.setColumnFamily("geo_code");
		rangeQuery.setColumnFamily("01001");
		//long value = RandomUtils.nextInt(10000 * 1);
		//rangeQuery.addEqualsExpression("网址", "http://m.taobao.com");
		//rangeQuery.addEqualsExpression("age", value);
		//rangeQuery.setKeys("865488021073839|00000000_02001.1535_0-2014_10_17_16_02_35_143.abstract.csv_0", "865488021073839|00000000_02001.1535_0-2014_10_17_16_02_35_143.abstract.csv_0");
//		rangeQuery.setKeys("865488020373909|00000000_04002.000_00000000-2014_10_15_18_18_25_333.abstract.csv_0", "865488020373909|00000000_04002.000_00000000-2014_10_15_18_18_25_333.abstract.csv_0");
		rangeQuery.setKeys("867742006109440|00000000_01001.000_00000000-2014_11_05_17_59_22_256-TCP.summary.csv_0","");
		rangeQuery.setRowCount(1);
//		rangeQuery.setColumnNames("cassandra_province","cassandra_city","cassandra_district","street","street_number","year_month_day","02001");
		//rangeQuery.setReturnKeysOnly();
	//rangeQuery.setKeys("C:|Users|issuser|Desktop|test|err|00000000_02001.1528_0-2014_01_20_11_21_10_939.abstract.csv_3", "C:|Users|issuser|Desktop|test|err|00000000_02001.1528_0-2014_01_20_11_21_10_939.abstract.csv_3");
		//rangeQuery.setKeys("39.877415_116.401169",	"39.877415_116.401169");
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss.SSS");
		System.out.println("Start Reading---->"+(dateFormat.format(new Date())));
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		System.out.println("Stop Reading---->"+(dateFormat.format(new Date())));
		OrderedRows<String, String, String> oRows = result.get();
		
//		System.out.println(oRows);
		int count = 0;
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			count++;
			
			System.out.println("key: " + row.getKey());
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			for (HColumn<String, String> hColumn : columns) {
				if("age".equals(hColumn.getName())){
					System.out.println("  column name: " + hColumn.getName()
							+ " value: " + hColumn.getValue());
				}else{
					System.out.println("  column name: " + hColumn.getName()
							+ " value: " + stringSerializer.fromByteBuffer(hColumn.getValueBytes()));
				}
			}
			
		}
		System.out.println(count);
	}

	static public void query3(){
		Keyspace keyspace = HFactory.createKeyspace("PUBLIC",cluster);
		SliceQuery<String, String, Long> query = HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
		query.setColumnFamily("signal_strength_LTE");
		query.setKey("864819023533589|00000000_02001.1874_0-2015_01_07_11_10_51_696.monitor.csv_11_1");
		
		ColumnSliceIterator<String, String, Long> iterator = new ColumnSliceIterator<String, String, Long>(query, null, "\uFFFF", false);
		while (iterator.hasNext()) {
			HColumn<String, Long> element = iterator.next();
			System.out.println(element.getName() + ":" + element.getValue());
		}
	}
	
	static public void query4(){
		
		Keyspace keyspace = HFactory.createKeyspace("GEO_DATA",cluster);
		
		SliceQuery<String, String, String> query = HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		query.setColumnFamily("geo_code");
		query.setKey("39.991138_116.464500");

		//ColumnSliceIterator指column的名称值
		ColumnSliceIterator<String, String, String> iterator = new ColumnSliceIterator<String, String, String>(query, null, "\uFFFF", false);
		//ColumnSliceIterator<String, String, String> iterator = new ColumnSliceIterator<String, String, String>(query, "C", "g", false);
		while (iterator.hasNext()) {
			HColumn<String, String> element = iterator.next();
			System.out.println(element.getName() + ":" + element.getValue());
		}
	}
	
	public static void main(String[] args) throws Exception {
		KeyspaceDefinition ksd = cluster.describeKeyspace("EN_CMCC_CMPAK_CEM");
		System.out.println(ksd.getName());
		cluster.dropKeyspace("EN_CMCC_CMPAK_CEM");
//		query2();
		/*
		List<KeyspaceDefinition> ks = cluster.describeKeyspaces();
		for (int i = 0; i < ks.size(); i++) {
			KeyspaceDefinition ksd = ks.get(i);
			if("system".equals(ksd.getName()) || "system_traces".equals(ksd.getName()) || "OpsCenter".equals(ksd.getName())){
				continue;
			}
			System.out.println(ksd.getName());
			
			KeyspaceDefinition kd = cluster.describeKeyspace(ksd.getName());
			List<ColumnFamilyDefinition> cfList = kd.getCfDefs();
			for (int j = 0; j < cfList.size(); j++) {
				ColumnFamilyDefinition cfD = cfList.get(j);
//				if (cfD.getName().equals(numType)) {
//					return;
//				}
				System.out.println(ksd.getName());
				if(cfD.getName().startsWith("total")){
					System.out.println("drop CF--->"+cfD.getName());
					cluster.dropColumnFamily(ksd.getName(), cfD.getName());
				}
			}
			
			//cluster.dropKeyspace(cfD.getName());
		}
		*/
		
		/*
		List<ColumnFamilyDefinition> cfList = ksd.getCfDefs();
		for (int j = 0; j < cfList.size(); j++) {
			ColumnFamilyDefinition cfD = cfList.get(j);
//			if (cfD.getName().equals(numType)) {
//				return;
//			}
			System.out.println(ksd.getName());
			if(cfD.getName().startsWith("total")){
				System.out.println("drop CF--->"+cfD.getName());
				cluster.dropColumnFamily(ksd.getName(), cfD.getName());
			}
		}
		*/
		
//		
//		List<KeyspaceDefinition> ks = cluster.describeKeyspaces();
//		for (int i = 0; i < ks.size(); i++) {
//			KeyspaceDefinition ksd = ks.get(i);
//			if("system".equals(ksd.getName()) || "system_traces".equals(ksd.getName()) || "OpsCenter".equals(ksd.getName())){
//				continue;
//			}
//			System.out.println(ksd.getName());
//			
//			KeyspaceDefinition kd = cluster.describeKeyspace(ksd.getName());
//			List<ColumnFamilyDefinition> cfList = kd.getCfDefs();
//			for (int j = 0; j < cfList.size(); j++) {
//				ColumnFamilyDefinition cfD = cfList.get(j);
////				if (cfD.getName().equals(numType)) {
////					return;
////				}
//				//System.out.println(cfD.getName());
//			}
//			
//			//cluster.dropKeyspace(cfD.getName());
//		}
		
		
		
		//createSchema();
		//operatioinDB();
		//query4();
		//query();
//		query();
//39.8930283,116.3439178
//		DecimalFormat decimalFormat=new DecimalFormat(".000000");
//		String key1 = decimalFormat.format(Float.valueOf(""+39.8930283));
//		String key2 = decimalFormat.format(Float.valueOf(""+116.3439178));
//		System.out.println(key1+" "+key2);
//		
	}
}
