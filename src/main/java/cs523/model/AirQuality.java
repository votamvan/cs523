package cs523.model;

import java.io.Serializable;
import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AirQuality implements Serializable {

	private static final long serialVersionUID = 1L;

	private String country;
	private String city;
	private String location;
	private String latitude;
	private String longitude;
	private String value;
	private LocalDateTime timestamp;

	public static AirQuality of(String country, String city, String location, String latitude, String longitude, String value, LocalDateTime timestamp) {
		return new AirQuality(country, city, location, latitude, longitude, value, timestamp);
	}

}
