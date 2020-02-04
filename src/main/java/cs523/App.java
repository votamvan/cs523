package cs523;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.hadoop.hbase.client.*;

import cs523.hbase.AirConditionRepository;
import cs523.hbase.HbaseConnection;
import cs523.model.AirQuality;
import cs523.model.Parser;
import cs523.sparksql.AirQualityReview;

import cs523.config.IKafkaConstants;
import kafka.serializer.StringDecoder;

public class App {

	public static void main(String[] args) throws IOException, InterruptedException {
		try (Connection connection = HbaseConnection.getInstance()) {
			SparkConf sparkConf = new SparkConf().setAppName("App").setMaster("local[*]");
			JavaSparkContext sc = new JavaSparkContext(sparkConf);
			String mode = "sql";
			if (args.length > 0) mode = args[0];
			if (mode.equals("s3")) streamingFromS3(sc);
			if (mode.equals("sql")) sparkSql(sc);
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

			JavaDStream<AirQuality> recoredRDDs = streamOfRecords.map(Parser::parse);

			recoredRDDs.foreachRDD(rdd -> {
				if (!rdd.isEmpty()) {
					repo.save(hadoopConf, rdd);
				}
			});
			ssc.start();
			ssc.awaitTermination();
		}
	}

	public static void streamingFromKafka(JavaSparkContext sc) throws IOException, InterruptedException {
		AirConditionRepository repo = AirConditionRepository.getInstance();
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("bootstrap.servers", IKafkaConstants.KAFKA_BROKERS);
		kafkaParams.put("fetch.message.max.bytes", String.valueOf(IKafkaConstants.MESSAGE_SIZE));
		Set<String> topicName = Collections.singleton(IKafkaConstants.TOPIC_NAME);
		Configuration hadoopConf = sc.hadoopConfiguration();

		try (JavaStreamingContext streamingContext = new JavaStreamingContext(sc, new Duration(5000))) {
			JavaPairInputDStream<String, String> kafkaSparkPairInputDStream = KafkaUtils.createDirectStream(
				streamingContext, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicName);
			JavaDStream<AirQuality> recoredRDDs = kafkaSparkPairInputDStream.map(Parser::parse);
			recoredRDDs.foreachRDD(rdd -> {
				if (!rdd.isEmpty()) {
					repo.save(hadoopConf, rdd);
				}
			});

			streamingContext.start();
			streamingContext.awaitTermination();
		}
	}

	public static void sparkSql(JavaSparkContext jsc) throws IOException, InterruptedException{
		AirQualityReview.ReadRecords(jsc, getKeyFromFile("keys.txt"));
		// top five air polution
		AirQualityReview.TopAirPolution();
		// best five air quality by country
		AirQualityReview.BestFiveAirQualityByCountry("US");
		// average air quality by date
		AirQualityReview.AverageAirQualityFromDate("2015-01-01 00:00:01");
		// air quality index by city
		AirQualityReview.AirQualityIndexFilterByCity("Detroit", "2015-01-01 00:00:01");
		// air quality index by location
		AirQualityReview.AirQualityIndexFilterByLocation("DET POLICE 4TH", "2015-01-01 00:00:01");
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
