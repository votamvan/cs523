package cs523.model;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

import lombok.extern.log4j.Log4j;

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
			double value = Double.parseDouble(part[3]);

			String location = part.length > 4 ? part[4] : "";
			double latitude = part.length > 5 ?  Double.parseDouble(part[5]) : 0.0;
			double longitude = part.length > 6 ?  Double.parseDouble(part[6]) : 0.0;
			return AirQuality.of(country, city, location, latitude, longitude, value, timestamp);
		} catch (DateTimeParseException ex) {
			log.warn("DateTime:" + ex.getMessage());
		} catch (NumberFormatException ex) {
			log.warn("Number:" + ex.getMessage());
		}
		return null;

	}

}
