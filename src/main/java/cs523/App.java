package cs523;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import cs523.hbase.AirConditionRepository;
import cs523.hbase.HbaseConnection;
import cs523.model.AirQuality;
import cs523.model.Parser;
import cs523.sparksql.AirQualityReview;

public class App {

	public static void main(String[] args) throws IOException, InterruptedException {
		try (Connection connection = HbaseConnection.getInstance()) {	
			SparkConf sparkConf = new SparkConf().setAppName("App").setMaster("local[*]");
			JavaSparkContext sc = new JavaSparkContext(sparkConf);
			String mode = "sql";
			if (args.length > 0) mode = args[0];
			if (mode.equals("s3")) streamingFromS3(sc);
			if (mode.equals("sql")) sparkSql(sc, args[1]);
			if (mode.equals("kafka")) streamingFromKafka(sc);
		}
	}
	public static void streamingFromS3(JavaSparkContext sc) throws IOException, InterruptedException {
		Configuration hadoopConf = sc.hadoopConfiguration();
		hadoopConf.set("fs.s3a.access.key", "AKIAIEKMELN37QBVV5KA");
		hadoopConf.set("fs.s3a.secret.key", "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");
		
		AirConditionRepository repo = AirConditionRepository.getInstance();
		repo.createTable();
		
		try (JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000))) {
			JavaDStream<String> streamOfRecords = ssc.textFileStream("s3a://amazon-reviews-pds-local/tsv");
			streamOfRecords.print();

			JavaDStream<AirQuality> reviews = streamOfRecords.map(Parser::parse);

			reviews.foreachRDD(rdd -> {
				if (!rdd.isEmpty()) {
					repo.save(hadoopConf, rdd);
				}
			});
			ssc.start();
			ssc.awaitTermination();
		}
	}
	public static void streamingFromKafka(JavaSparkContext sc) throws IOException, InterruptedException {

	}
	public static void sparkSql(JavaSparkContext jsc, String input) throws IOException, InterruptedException{
		AirQualityReview.ReadRecords(jsc, getKeyFromFile(input));
		AirQualityReview.TopAirPolution();
	}
	public static String[] getKeyFromFile(String input){
		List<String> keys = new ArrayList<String>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(input));
			String line = reader.readLine();
			while (line != null) {
				keys.add(line);
				line = reader.readLine();
			}
			reader.close();
		}catch(IOException ignore) {
			ignore.printStackTrace();
		}
		return keys.stream().toArray(String[]::new);
	}
}
