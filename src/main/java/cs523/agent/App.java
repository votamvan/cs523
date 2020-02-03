package cs523.agent;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import cs523.config.IKafkaConstants;

public class App {

	public static Producer<Long, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, IKafkaConstants.MESSAGE_SIZE);
		return new KafkaProducer<>(props);
	}

	static void runProducer() {
		Producer<Long, String> producer = createProducer();
		for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
			ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME,
					"This is record " + index);
			try {
				RecordMetadata metadata = producer.send(record).get();
				System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
						+ " with offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}
		}

	}

	private static List<String> parse(String[] args) throws IOException {
		StringJoiner url = new StringJoiner("&");
		url.add("https://api.openaq.org/v1/measurements?has_geo=false&format=csv");
		if (args.length > 0 && !args[0].equals("")) {
			url.add("country="+args[0]);
		}
		if (args.length > 1 && !args[1].equals("")) {
			url.add("parameter="+args[1]);
		}
		if (args.length > 2 && !args[2].equals("")) {
			url.add("date_from="+args[2]);
		}
		if (args.length > 3 && !args[3].equals("")) {
			url.add("date_to="+args[3]);
		}
		HttpClient client = HttpClientBuilder.create().build();
		HttpGet request = new HttpGet(url.toString());
		request.addHeader("accept", "application/json");
		HttpResponse response = client.execute(request);
		String csv = IOUtils.toString(response.getEntity().getContent());
		CSVParser parser = new CSVParserBuilder()
				.withSeparator(',')
				.withQuoteChar('"')
				.build();
		List<String> data = new ArrayList<>();
		try (CSVReader csvReader = new CSVReaderBuilder(new StringReader(csv))
				.withSkipLines(1)
				.withCSVParser(parser)
				.build()) {
			List<String[]> allData = csvReader.readAll();
			for (int i = 0; i < allData.size(); i++) {
				String[] line = allData.get(i);
				StringJoiner sj = new StringJoiner("\t");
				sj.add(line[2]);
				sj.add(line[1]);
				sj.add(line[3]);
				sj.add(line[6]);
				sj.add(line[0]);
				sj.add(line[8]);
				sj.add(line[9]);
				sj.add(line[9]);
				data.add(sj.toString());
			}
		}
		return data;
	}

	public static void publish(List<String> data) throws IOException {
		try (Producer<Long, String> producer = createProducer()) {
			for (String d : data) {
				ProducerRecord<Long, String> record = new ProducerRecord<>(
						IKafkaConstants.TOPIC_NAME,
						d
				);
				try {
					producer.send(record).get();
				} catch (ExecutionException e) {
					System.out.println("Error in sending record");
					System.out.println(e);
				} catch (InterruptedException e) {
					System.out.println("Error in sending record");
					System.out.println(e);
				}
			}
		}
	}

	public static void main(String[] args) {
		try {
			List<String> data = parse(args);
			publish(data);
		} catch (IOException ex) {
			System.out.println(ex.getMessage());
		}
		System.out.println("Done!!!");
	}

	void check() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", IKafkaConstants.KAFKA_BROKERS);
		properties.put("connections.max.idle.ms", 10000);
		properties.put("request.timeout.ms", 5000);
		try (AdminClient client = KafkaAdminClient.create(properties)) {
			ListTopicsResult topics = client.listTopics();
			Set<String> names = topics.names().get();
			if (names.isEmpty()) {
				return;
			}
			names.forEach(System.out::println);
		} catch (InterruptedException | ExecutionException e) {
		}
	}

}
