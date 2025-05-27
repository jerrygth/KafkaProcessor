package org.kafka.producer.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * Model class for Open-Meteo API response.
 */
public class WeatherResponse {
    private double latitude;
    private double longitude;
    @JsonProperty("generationtime_ms")
    private double generationTimeMs;
    @JsonProperty("utc_offset_seconds")
    private int utcOffsetSeconds;
    private String timezone;
    @JsonProperty("timezone_abbreviation")
    private String timezoneAbbreviation;
    private double elevation;
    @JsonProperty("current_weather_units")
    private Map<String, String> currentWeatherUnits;
    @JsonProperty("current_weather")
    private Map<String, Object> currentWeather;

    // Getters and setters
    public double getLatitude() { return latitude; }
    public void setLatitude(double latitude) { this.latitude = latitude; }
    public double getLongitude() { return longitude; }
    public void setLongitude(double longitude) { this.longitude = longitude; }
    public double getGenerationTimeMs() { return generationTimeMs; }
    public void setGenerationTimeMs(double generationTimeMs) { this.generationTimeMs = generationTimeMs; }
    public int getUtcOffsetSeconds() { return utcOffsetSeconds; }
    public void setUtcOffsetSeconds(int utcOffsetSeconds) { this.utcOffsetSeconds = utcOffsetSeconds; }
    public String getTimezone() { return timezone; }
    public void setTimezone(String timezone) { this.timezone = timezone; }
    public String getTimezoneAbbreviation() { return timezoneAbbreviation; }
    public void setTimezoneAbbreviation(String timezoneAbbreviation) { this.timezoneAbbreviation = timezoneAbbreviation; }
    public double getElevation() { return elevation; }
    public void setElevation(double elevation) { this.elevation = elevation; }
    public Map<String, String> getCurrentWeatherUnits() { return currentWeatherUnits; }
    public void setCurrentWeatherUnits(Map<String, String> currentWeatherUnits) { this.currentWeatherUnits = currentWeatherUnits; }
    public Map<String, Object> getCurrentWeather() { return currentWeather; }
    public void setCurrentWeather(Map<String, Object> currentWeather) { this.currentWeather = currentWeather; }
}
