package cs523.hbase;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import cs523.model.AirQuality;
import lombok.extern.log4j.Log4j;
import scala.Tuple2;

@Log4j
public class AirConditionRepository implements Serializable {

	private static final long serialVersionUID = 1L;

	private static AirConditionRepository instance;

	private static final String TABLE_NAME = "air_quality";
	private static final String CF = "cf";
	private final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	private AirConditionRepository() {}

	public static AirConditionRepository getInstance() {
		if (instance == null) {
			instance = new AirConditionRepository();
		}
		return instance;
	}

	public void createTable() {
		Connection connection = HbaseConnection.getInstance();
		try (Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF).setCompressionType(Algorithm.NONE));
			if (!admin.tableExists(table.getTableName())) {
				log.info("Creating table ");
				admin.createTable(table);
				log.info("Table created");
			}
		} catch (IOException ex) {
			log.error(ex.getMessage());
		}
	}

	public void putAll(Map<String, AirQuality> data) throws IOException {
		Connection connection = HbaseConnection.getInstance();
		try (Table tb = connection.getTable(TableName.valueOf("obj"))) {
			ArrayList<Put> ls = new ArrayList<Put>();
			for (String k : data.keySet()) {
				ls.add(putObject(k, data.get(k)));
			}
			tb.put(ls);
		}
	}

	public void put(String key, AirQuality obj) throws IOException {
		Connection connection = HbaseConnection.getInstance();
		try (Table tb = connection.getTable(TableName.valueOf(TABLE_NAME))) {
			tb.put(putObject(key, obj));
		}
	}

	private Put putObject(String key, AirQuality obj) {
		Put p = new Put(Bytes.toBytes(key));
		p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("country"), Bytes.toBytes(obj.getCountry()));
		p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("city"), Bytes.toBytes(obj.getCity()));
		p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("location"), Bytes.toBytes(obj.getLocation()));
		p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("latitude"), Bytes.toBytes(obj.getLatitude()));
		p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("longitude"), Bytes.toBytes(obj.getLongitude()));
		p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("value"), Bytes.toBytes(obj.getValue()));
		p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("timestamp"), Bytes.toBytes(obj.getTimestamp().format(formatter)));
		return p;
	}

	public AirQuality get(Connection connection, String key) throws IOException {
		try (Table tb = connection.getTable(TableName.valueOf(TABLE_NAME))) {
			Get g = new Get(Bytes.toBytes(key));
			Result result = tb.get(g);
			if (result.isEmpty()) {
				return null;
			}
			byte [] value1 = result.getValue(Bytes.toBytes(CF),Bytes.toBytes("country"));
			byte [] value2 = result.getValue(Bytes.toBytes(CF),Bytes.toBytes("city"));
			byte [] value3 = result.getValue(Bytes.toBytes(CF),Bytes.toBytes("location"));
			byte [] value4 = result.getValue(Bytes.toBytes(CF),Bytes.toBytes("latitude"));
			byte [] value5 = result.getValue(Bytes.toBytes(CF),Bytes.toBytes("longitude"));
			byte [] value6 = result.getValue(Bytes.toBytes(CF),Bytes.toBytes("value"));
			byte [] value7 = result.getValue(Bytes.toBytes(CF),Bytes.toBytes("timestamp"));

			return AirQuality.of(
				Bytes.toString(value1), Bytes.toString(value2),
				Bytes.toString(value3), Bytes.toString(value4),
				Bytes.toString(value5), Bytes.toString(value6),
				LocalDateTime.parse(Bytes.toString(value7).substring(0, 19), formatter)
			);
		}
	}

	public void save(Configuration config, JavaRDD<AirQuality> record)
				throws MasterNotRunningException, Exception {

		Job job = Job.getInstance(config);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
		job.setOutputFormatClass(TableOutputFormat.class);

		JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = record
			.mapToPair(new MyPairFunction());
		hbasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration());
	}

	class MyPairFunction implements PairFunction<AirQuality, ImmutableBytesWritable, Put> {

		private static final long serialVersionUID = 1L;
		@Override
		public Tuple2<ImmutableBytesWritable, Put> call(AirQuality record) throws Exception {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS");
			String key = record.getTimestamp().format(formatter);
			Put put = putObject(key, record);
			return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
		}

	}

}
