package cs523.sparksql;

import java.util.List;
import java.util.ArrayList;

import java.io.IOException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import cs523.hbase.AirConditionRepository;
import cs523.hbase.HbaseConnection;
import cs523.model.*;

public class AirQualityReview {
	private static final String TABLE_NAME = "air_quality";
	private static SparkSession spark;
	
	public static void ReadRecords(JavaSparkContext jsc, String[] keys) throws IOException{
		AirConditionRepository repo = AirConditionRepository.getInstance();
		List<HbaseRecord> list = new ArrayList<HbaseRecord>();
		for (String key: keys) {
			AirQuality record = repo.get(HbaseConnection.getInstance(), key);
			list.add(HbaseRecord.of(record));
		}
		spark = SparkSession.builder().appName("Spark SQL").master("local").getOrCreate();
		spark.createDataFrame(list, HbaseRecord.class).createOrReplaceTempView(TABLE_NAME);
	}

	public static void TopAirPolution() {
		String query =  " SELECT * FROM " + TABLE_NAME 
					  + " ORDER BY value DESC "
					  + " LIMIT 15 ";
		System.out.println(query);
		Dataset<Row> sqlDF = spark.sql(query);
		sqlDF.show();
	}
}
