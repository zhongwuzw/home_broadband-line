package com.opencassandra.v01.dao.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.sf.json.JSONObject;

import org.apache.cassandra.thrift.IndexOperator;
import com.opencassandra.v01.bean.ColumnQueryDefination;
import com.opencassandra.v01.bean.TerminalInfo;
import com.opencassandra.v01.dao.factory.KeySpaceFactory;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

public class TerminalManageDao {
	private static final String DYN_KEYSPACE = "DeviceInfo";
	private static final String DYN_CF = "Terminal";

	private static StringSerializer stringSerializer = StringSerializer.get();
	private static LongSerializer longSerializer = LongSerializer.get();

	public static TerminalInfo queryByKey(String key) {
		Keyspace keyspaceOperator = KeySpaceFactory.getKeyspace(DYN_KEYSPACE);

		RangeSlicesQuery<String, String, ByteBuffer> rangeQuery = HFactory
				.createRangeSlicesQuery(keyspaceOperator,
						StringSerializer.get(), StringSerializer.get(),
						ByteBufferSerializer.get());
		rangeQuery.setColumnFamily(DYN_CF);

		rangeQuery.setRange("", "", false, 3);
		rangeQuery.setKeys(key, key);

		QueryResult<OrderedRows<String, String, ByteBuffer>> result = rangeQuery
				.execute();
		OrderedRows<String, String, ByteBuffer> oRows = result.get();

		List<Row<String, String, ByteBuffer>> rowList = oRows.getList();
		for (Row<String, String, ByteBuffer> row : rowList) {
			return TerminalInfo.toJavaBean(row);
		}
		return null;
	}

	public static List<TerminalInfo> query(
			ColumnQueryDefination... columnQueryDefinations) {
		Keyspace keyspaceOperator = KeySpaceFactory.getKeyspace(DYN_KEYSPACE);

		RangeSlicesQuery<String, String, ByteBuffer> rangeQuery = HFactory
				.createRangeSlicesQuery(keyspaceOperator,
						StringSerializer.get(), StringSerializer.get(),
						ByteBufferSerializer.get());
		rangeQuery.setColumnFamily(DYN_CF);
		rangeQuery.setColumnNames(TerminalInfo.toColumns());
		rangeQuery.setRange("", "", false, 300);

		for (ColumnQueryDefination cqd : columnQueryDefinations) {
			switch (cqd.getIndexOperator()) {
			case EQ:
				rangeQuery.addEqualsExpression(cqd.getName(), cqd.getValue());
				break;
			case GT:
				rangeQuery.addGtExpression(cqd.getName(), cqd.getValue());
				break;
			case GTE:
				rangeQuery.addGteExpression(cqd.getName(), cqd.getValue());
				break;
			case LT:
				rangeQuery.addLtExpression(cqd.getName(), cqd.getValue());
				break;
			case LTE:
				rangeQuery.addLteExpression(cqd.getName(), cqd.getValue());
				break;
			default:
				break;
			}
		}

		QueryResult<OrderedRows<String, String, ByteBuffer>> result = rangeQuery
				.execute();
		OrderedRows<String, String, ByteBuffer> oRows = result.get();

		List<Row<String, String, ByteBuffer>> rowList = oRows.getList();
		return TerminalInfo.toJavaBean(rowList);
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
		 * Keyspace keyspace = HFactory.createKeyspace("logdb",cluster);
		 * RangeSlicesQuery<String, String, String> rangeQuery = HFactory
		 * .createRangeSlicesQuery(keyspace, stringSerializer, stringSerializer,
		 * stringSerializer);
		 * 
		 * rangeQuery.setColumnFamily("user"); long value =
		 * RandomUtils.nextInt(10000 * 1);
		 * rangeQuery.addEqualsExpression("username", "小贩" + value);
		 * rangeQuery.setColumnNames("username", "age");
		 * 
		 * QueryResult<OrderedRows<String, String, String>> result = rangeQuery
		 * .execute(); OrderedRows<String, String, String> oRows = result.get();
		 * 
		 * System.out.println(oRows);
		 * 
		 * List<Row<String, String, String>> rowList = oRows.getList(); for
		 * (Row<String, String, String> row : rowList) {
		 * System.out.println("key: " + row.getKey()); List<HColumn<String,
		 * String>> columns = row.getColumnSlice() .getColumns(); for
		 * (HColumn<String, String> hColumn : columns) {
		 * System.out.println("  column name: " + hColumn.getName() + " value: "
		 * + hColumn.getValue()); } }
		 */
		Keyspace keyspace = KeySpaceFactory.getKeyspace(DYN_KEYSPACE);
		RangeSlicesQuery<String, String, Long> rangeQuery = HFactory
				.createRangeSlicesQuery(keyspace, stringSerializer,
						stringSerializer, longSerializer);

		rangeQuery.setColumnFamily("user");
		// rangeQuery.addEqualsExpression("username", "小贩" + value);
		// rangeQuery.addEqualsExpression("age", value);
		rangeQuery.setKeys("64256", "152057");
		rangeQuery.setRowCount(12);
		rangeQuery.setColumnNames("username", "age");
		// rangeQuery.setReturnKeysOnly();

		QueryResult<OrderedRows<String, String, Long>> result = rangeQuery
				.execute();
		OrderedRows<String, String, Long> oRows = result.get();

		System.out.println(oRows);

		List<Row<String, String, Long>> rowList = oRows.getList();
		for (Row<String, String, Long> row : rowList) {
			System.out.println("key: " + row.getKey());
			List<HColumn<String, Long>> columns = row.getColumnSlice()
					.getColumns();
			for (HColumn<String, Long> hColumn : columns) {
				if ("age".equals(hColumn.getName())) {
					System.out.println("  column name: " + hColumn.getName()
							+ " value: " + hColumn.getValue());
				} else {
					System.out.println("  column name: "
							+ hColumn.getName()
							+ " value: "
							+ stringSerializer.fromByteBuffer(hColumn
									.getValueBytes()));
				}
			}

		}
	}

	public static void createSchema() {

		try {

			Keyspace keyspaceOperator = KeySpaceFactory
					.getKeyspace(DYN_KEYSPACE);

			// device_id
			BasicColumnDefinition deviceIdColumnDefinition = new BasicColumnDefinition();
			deviceIdColumnDefinition.setName(stringSerializer
					.toByteBuffer("device_id"));
			deviceIdColumnDefinition.setIndexName("device_id_idx");
			deviceIdColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			deviceIdColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());

			// device_type_name
			BasicColumnDefinition deviceTypeNameColumnDefinition = new BasicColumnDefinition();
			deviceTypeNameColumnDefinition.setName(stringSerializer
					.toByteBuffer("device_type_name"));
			deviceTypeNameColumnDefinition.setIndexName("device_type_name_idx");
			deviceTypeNameColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			deviceTypeNameColumnDefinition
					.setValidationClass(ComparatorType.UTF8TYPE.getClassName());

			// device_type_id
			BasicColumnDefinition deviceTypeIdColumnDefinition = new BasicColumnDefinition();
			deviceTypeIdColumnDefinition.setName(stringSerializer
					.toByteBuffer("device_type_id"));
			deviceTypeIdColumnDefinition.setIndexName("device_type_id_idx");
			deviceTypeIdColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			deviceTypeIdColumnDefinition
					.setValidationClass(ComparatorType.UTF8TYPE.getClassName());
			
			// device_os_name
			BasicColumnDefinition deviceOsNameColumnDefinition = new BasicColumnDefinition();
			deviceOsNameColumnDefinition.setName(stringSerializer
					.toByteBuffer("device_os_name"));
			deviceOsNameColumnDefinition.setIndexName("device_os_name_idx");
			deviceOsNameColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			deviceOsNameColumnDefinition
					.setValidationClass(ComparatorType.UTF8TYPE.getClassName());

			// device_os_id
			BasicColumnDefinition deviceOsIdColumnDefinition = new BasicColumnDefinition();
			deviceOsIdColumnDefinition.setName(stringSerializer
					.toByteBuffer("device_os_id"));
			deviceOsIdColumnDefinition.setIndexName("device_os_id_idx");
			deviceOsIdColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			deviceOsIdColumnDefinition
					.setValidationClass(ComparatorType.UTF8TYPE.getClassName());
			
			// device_group_name
			BasicColumnDefinition deviceGroupNameColumnDefinition = new BasicColumnDefinition();
			deviceGroupNameColumnDefinition.setName(stringSerializer
					.toByteBuffer("device_group_name"));
			deviceGroupNameColumnDefinition.setIndexName("device_group_name_idx");
			deviceGroupNameColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			deviceGroupNameColumnDefinition
					.setValidationClass(ComparatorType.UTF8TYPE.getClassName());

			// device_group_id
			BasicColumnDefinition deviceGroupIdColumnDefinition = new BasicColumnDefinition();
			deviceGroupIdColumnDefinition.setName(stringSerializer
					.toByteBuffer("device_group_id"));
			deviceGroupIdColumnDefinition.setIndexName("device_group_id_idx");
			deviceGroupIdColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			deviceGroupIdColumnDefinition
					.setValidationClass(ComparatorType.UTF8TYPE.getClassName());
			
			// device_imei
			BasicColumnDefinition deviceImeiColumnDefinition = new BasicColumnDefinition();
			deviceImeiColumnDefinition.setName(stringSerializer
					.toByteBuffer("device_imei"));
			deviceImeiColumnDefinition.setIndexName("device_imei_idx");
			deviceImeiColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			deviceImeiColumnDefinition
					.setValidationClass(ComparatorType.UTF8TYPE.getClassName());
			
			// device_mac
			BasicColumnDefinition deviceMacColumnDefinition = new BasicColumnDefinition();
			deviceMacColumnDefinition.setName(stringSerializer
					.toByteBuffer("device_mac"));
			deviceMacColumnDefinition.setIndexName("device_mac_idx");
			deviceMacColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			deviceMacColumnDefinition
					.setValidationClass(ComparatorType.UTF8TYPE.getClassName());
			
			// device_no
			BasicColumnDefinition deviceNoColumnDefinition = new BasicColumnDefinition();
			deviceNoColumnDefinition.setName(stringSerializer
					.toByteBuffer("device_no"));
			deviceNoColumnDefinition.setIndexName("device_no_idx");
			deviceNoColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			deviceNoColumnDefinition
					.setValidationClass(ComparatorType.UTF8TYPE.getClassName());
			
			// last_update_time
			BasicColumnDefinition lastUpdateTimeColumnDefinition = new BasicColumnDefinition();
			lastUpdateTimeColumnDefinition.setName(stringSerializer
					.toByteBuffer("last_update_time"));
			lastUpdateTimeColumnDefinition.setIndexName("last_update_time_idx");
			lastUpdateTimeColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			lastUpdateTimeColumnDefinition
					.setValidationClass(ComparatorType.LONGTYPE.getClassName());

			// apply_time
			BasicColumnDefinition applyTimeColumnDefinition = new BasicColumnDefinition();
			applyTimeColumnDefinition.setName(stringSerializer
					.toByteBuffer("apply_time"));
			applyTimeColumnDefinition.setIndexName("apply_time_idx");
			applyTimeColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			applyTimeColumnDefinition
					.setValidationClass(ComparatorType.LONGTYPE.getClassName());
			
			// apply_location_name
			BasicColumnDefinition applyLocationNameColumnDefinition = new BasicColumnDefinition();
			applyLocationNameColumnDefinition.setName(stringSerializer
					.toByteBuffer("apply_location_name"));
			applyLocationNameColumnDefinition.setIndexName("apply_location_name_idx");
			applyLocationNameColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			applyLocationNameColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());

			// apply_district_name
			BasicColumnDefinition applyDistrictNameColumnDefinition = new BasicColumnDefinition();
			applyDistrictNameColumnDefinition.setName(stringSerializer
					.toByteBuffer("apply_district_name"));
			applyDistrictNameColumnDefinition.setIndexName("apply_district_name_idx");
			applyDistrictNameColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			applyDistrictNameColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());
			
			// description
			BasicColumnDefinition descriptionColumnDefinition = new BasicColumnDefinition();
			descriptionColumnDefinition.setName(stringSerializer
					.toByteBuffer("description"));
			descriptionColumnDefinition.setIndexName("description_idx");
			descriptionColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			descriptionColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());
			
			// remark
			BasicColumnDefinition remarkColumnDefinition = new BasicColumnDefinition();
			remarkColumnDefinition.setName(stringSerializer
					.toByteBuffer("remark"));
			remarkColumnDefinition.setIndexName("remark_idx");
			remarkColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			remarkColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());
			
			// device_tools_version_name
			BasicColumnDefinition deviceToolsVersionNameColumnDefinition = new BasicColumnDefinition();
			deviceToolsVersionNameColumnDefinition.setName(stringSerializer
					.toByteBuffer("device_tools_version_name"));
			deviceToolsVersionNameColumnDefinition.setIndexName("device_tools_version_name_idx");
			deviceToolsVersionNameColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			deviceToolsVersionNameColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());
			
			// device_tools_version_id
			BasicColumnDefinition deviceToolsVersionIdColumnDefinition = new BasicColumnDefinition();
			deviceToolsVersionIdColumnDefinition.setName(stringSerializer
					.toByteBuffer("device_tools_version_id"));
			deviceToolsVersionIdColumnDefinition.setIndexName("device_tools_version_id_idx");
			deviceToolsVersionIdColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			deviceToolsVersionIdColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());
			
			// login_counts
			BasicColumnDefinition loginCountsColumnDefinition = new BasicColumnDefinition();
			loginCountsColumnDefinition.setName(stringSerializer
					.toByteBuffer("login_counts"));
			loginCountsColumnDefinition.setIndexName("login_counts_idx");
			loginCountsColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			loginCountsColumnDefinition.setValidationClass(ComparatorType.LONGTYPE.getClassName());
			
			ArrayList<ColumnDefinition> columns = new ArrayList<ColumnDefinition>();
			columns.add(deviceIdColumnDefinition);
			columns.add(deviceTypeNameColumnDefinition);
			columns.add(deviceTypeIdColumnDefinition);
			columns.add(deviceOsNameColumnDefinition);
			columns.add(deviceOsIdColumnDefinition);
			columns.add(deviceGroupNameColumnDefinition);
			columns.add(deviceGroupIdColumnDefinition);
			columns.add(deviceImeiColumnDefinition);
			columns.add(deviceMacColumnDefinition);
			columns.add(deviceNoColumnDefinition);
			columns.add(lastUpdateTimeColumnDefinition);
			columns.add(applyTimeColumnDefinition);
			columns.add(applyLocationNameColumnDefinition);
			columns.add(applyDistrictNameColumnDefinition);
			columns.add(descriptionColumnDefinition);
			columns.add(remarkColumnDefinition);
			columns.add(deviceToolsVersionNameColumnDefinition);
			columns.add(deviceToolsVersionIdColumnDefinition);
			columns.add(loginCountsColumnDefinition);
			
			ColumnFamilyDefinition columnFamilyDefinition = HFactory
					.createColumnFamilyDefinition(
							keyspaceOperator.getKeyspaceName(), DYN_CF,
							ComparatorType.BYTESTYPE, columns);
			columnFamilyDefinition.setColumnType(ColumnType.STANDARD);
			columnFamilyDefinition.setGcGraceSeconds(100);
			// columnFamilyDefinition.setCompactionStrategy("org.apache.cassandra.locator.SimpleStrategy");
			// columnFamilyDefinition.setKeyValidationClass("org.apache.cassandra.db.marshal.UTF8Type");
			// columnFamilyDefinition.setDefaultValidationClass("org.apache.cassandra.db.marshal.UTF8Type");

			KeySpaceFactory.createorupdateCF(columnFamilyDefinition);

			
			// insert some data
			Mutator<String> userMutator = HFactory.createMutator(
					keyspaceOperator, stringSerializer);

			long start = System.currentTimeMillis();
			for (long i = 1424; i <= 1424; i++) {
				userMutator
						.addInsertion(
								"" + i,
								DYN_CF,
								HFactory.createStringColumn("device_type_name", "HTC 80588"))
						.addInsertion(
								"" + i,
								DYN_CF,
								HFactory.createColumn("last_update_time", (new Date()).getTime(),
										stringSerializer, longSerializer))
						.addInsertion(
								"" + i,
								DYN_CF,
								HFactory.createColumn("device_no", "13911937747"))
						.addInsertion(
								"" + i,
								DYN_CF,
								HFactory.createColumn("login_counts", (long)123445,
										stringSerializer, longSerializer))
						.addInsertion(
								"" + i,
								DYN_CF,
								HFactory.createColumn("remark", "liugang"))
						.addDeletion(
								"" + i,
								DYN_CF, "age", stringSerializer);
				if (i % 500 == 0) {
					System.out.println(i);
					userMutator.execute();
				}

			}
			userMutator.execute();
			System.out.println("消耗毫秒：" + (System.currentTimeMillis() - start));
			
		} catch (HectorException he) {
			he.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		// createSchema();
		// operatioinDB();
		// query2();
		// query();
		// queryByKey("1683");
		ColumnQueryDefination myCQD = new ColumnQueryDefination("device_no",stringSerializer.toByteBuffer("13911937747"),IndexOperator.EQ);
		//ColumnQueryDefination myCQD1 = new ColumnQueryDefination("device_id",stringSerializer.toByteBuffer("12534"),IndexOperator.EQ);
		List<TerminalInfo> myResult = query(myCQD);
		//List<TerminalInfo> myResult = query();1	
		for(int i=0; i<myResult.size(); i++){
			JSONObject mj = JSONObject.fromObject(myResult.get(i));
			System.out.println(mj.toString());
		}
		//createSchema();

	}
}
