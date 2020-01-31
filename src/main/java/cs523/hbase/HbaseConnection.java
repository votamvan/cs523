package cs523.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;

import lombok.extern.log4j.Log4j;

@Log4j
public class HbaseConnection {

	private static Connection connection;

	public static Connection getInstance() {
		if (connection == null) {
			try {
				Configuration config = HBaseConfiguration.create();
				connection = ConnectionFactory.createConnection(config);
			} catch (IOException ex) {
				log.error(ex.getMessage());
				System.exit(0);
			}
		}
		return connection;
	}
}
