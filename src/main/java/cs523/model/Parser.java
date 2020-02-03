package cs523.model;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

import lombok.extern.log4j.Log4j;
import scala.Tuple2;

@Log4j
public class Parser {

	public static AirQuality parse(String line) {
		String[] part = line.split("\t");
		if (part.length < 4) {
			return null;
		}
		try {
			String country = part[0];
			String city = part[1];
			LocalDateTime timestamp = ZonedDateTime.parse(part[2])
												   .truncatedTo(ChronoUnit.SECONDS)
												   .toLocalDateTime();
			String value = part[3];
			String location = part.length > 4 ? part[4] : "";
			String latitude = part.length > 5 ?  part[5] : "";
			String longitude = part.length > 6 ?  part[6] : "";
			return AirQuality.of(country, city, location, latitude, longitude, value, timestamp);
		} catch (DateTimeParseException ex) {
			log.warn("DateTime:" + ex.getMessage());
		} catch (NumberFormatException ex) {
			log.warn("Number:" + ex.getMessage());
		}
		return null;
	}

	public static AirQuality parse(Tuple2<String, String> tuple2) {
		return parse(tuple2._2());
	}

}
