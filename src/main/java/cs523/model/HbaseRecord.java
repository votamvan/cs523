package cs523.model;

import java.io.Serializable;
import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class HbaseRecord implements Serializable{
	private static final long serialVersionUID = 1L;

	private String country;
	private String city;
	private String location;
	private String latitude;
	private String longitude;
	private Double value;
	private String timestamp;
	public static HbaseRecord of(AirQuality aq) {
		String country = aq.getCountry();
		String city = aq.getCity();
		String location = aq.getLocation();
		String latitude = aq.getLatitude();
		String longitude = aq.getLatitude();
		Double value = Double.valueOf(aq.getValue());
		String timestamp = aq.getTimestamp().toString();
		return new HbaseRecord(country, city, location, latitude, longitude, value, timestamp);
	}
}
