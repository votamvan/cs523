package cs523;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.hadoop.hbase.client.Connection;

import cs523.hbase.AirConditionRepository;
import cs523.hbase.HbaseConnection;
import cs523.model.AirQuality;
import cs523.model.Parser;

public class App {

	public static void main(String[] args) throws IOException, InterruptedException {
		try (Connection connection = HbaseConnection.getInstance()) {
			AirConditionRepository repo = AirConditionRepository.getInstance();
			repo.createTable();
			SparkConf sparkConf = new SparkConf().setAppName("s3SparkStream").setMaster("local[*]");
			JavaSparkContext sc = new JavaSparkContext(sparkConf);

			Configuration hadoopConf = sc.hadoopConfiguration();
			hadoopConf.set("fs.s3a.access.key", "AKIAIEKMELN37QBVV5KA");
			hadoopConf.set("fs.s3a.secret.key", "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");
			// hadoopConf.set("fs.s3a.awsAccessKeyId","AKIAIEKMELN37QBVV5KA");
			// hadoopConf.set("fs.s3a.awsSecretAccessKey","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");

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

		// var data = Parser.parse("MK	Ilinden Municipality	2020-01-30T18:00:00.000Z	25.5	MK0045A	41.987439999466	21.6525");
		// repo.put(UUID.randomUUID().toString(), data);

	}

}
