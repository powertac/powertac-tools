package org.powertac.windpark;

import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
//import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

@XStreamAlias("data")
public class WsData 
{
	public static final String dateFormat = "yyyy-MM-dd'T'HH:mm:ssZ";
	@XStreamAlias("weatherReport")
	public static class WeatherReport implements Comparable<WeatherReport>
	{	
		@XStreamAlias("date")
		@XStreamAsAttribute
		private String dateString;
		
		@XStreamAlias("windspeed")
		@XStreamAsAttribute
		private float  wspeed; 
		
		@XStreamOmitField
		private DateTime date;
				
		public WeatherReport(String dt, float speed) throws ParseException {
			this.dateString = dt;
			DateTimeFormatter df = DateTimeFormat.forPattern(WsData.dateFormat);
			DateTime givenDateTime = df.parseDateTime(dt);
			this.date = roundToHour(givenDateTime);
			this.wspeed = speed;
		}
		
		public static DateTime roundToHour(DateTime givenDateTime) {
			return givenDateTime.plusMinutes(30).withMinuteOfHour(0);	
		}
		
		public void convertToDate() {
			DateTimeFormatter df = DateTimeFormat.forPattern(WsData.dateFormat);
			DateTime givenDateTime = df.parseDateTime(this.dateString);
			this.date = roundToHour(givenDateTime);
			return;
		}

		@Override
		public boolean equals(Object ob) {
			if (ob == null) {
				return false;
			}
			if (ob instanceof WeatherReport) {
				WeatherReport wr = (WeatherReport) ob;
				boolean eql = (this.compareTo(wr) == 0);
				return eql;
				
			} else {
				return false;
			}
		}
		

		public int compareTo (WeatherReport wr) {
			DateTime myDate, hisDate;
			DateTimeFormatter dtfmt = DateTimeFormat.forPattern(WsData.dateFormat);
			myDate = dtfmt.parseDateTime(this.dateString).withMinuteOfHour(0);
			hisDate = dtfmt.parseDateTime(wr.getDateString());
			
			if (myDate.isBefore(hisDate)) {
				return -1;
			} else if (myDate.isAfter(hisDate)) {
				return 1;
				
			} else {
				return 0;
			}
			
		}
		
		public DateTime getDate() {
			return this.date;
		}
		
		public String getDateString() {
			return this.dateString;
		}
		
		public float getWindSpeed() {
			return this.wspeed;
		}
		
	}
	
	@XStreamAlias("weatherForecast")
	public static class WeatherForecast implements Comparable<WeatherForecast>
	{
		@XStreamAlias("date")
		@XStreamAsAttribute
		private String dateString;	
		
		@XStreamOmitField
		private DateTime date;
		
		@XStreamAlias("id")
		@XStreamAsAttribute
		private int id;
		
		@XStreamAlias("origin")
		@XStreamAsAttribute
		private String originString;
		
		@XStreamOmitField
		private DateTime origin;
		
		@XStreamAlias("temp")
		@XStreamAsAttribute
		private int temp;
		
		@XStreamAlias("windspeed")
		@XStreamAsAttribute
		private float windspeed;
		
		@XStreamOmitField
		private float wsError = 0; //wind speed error
		
		@XStreamOmitField
		private float windSpeedObservation = 0;
		
		@XStreamOmitField
		private boolean noObservation = true;
		
		public WeatherForecast (String dt, int myId, String org, int tmp, float spd) throws ParseException {
			DateTimeFormatter df = DateTimeFormat.forPattern(WsData.dateFormat);
			this.date = df.parseDateTime(dt);
			this.dateString = dt;
			this.id = myId;
			this.origin = df.parseDateTime(org);
			this.originString = org;
			this.temp = tmp;
			this.windspeed = spd;
		}
		
		public void convertToDate() {
			DateTimeFormatter df = DateTimeFormat.forPattern(WsData.dateFormat);
			this.date = df.parseDateTime(this.dateString);
			this.origin = df.parseDateTime(this.originString);
			return;
		}
		
		public DateTime getDate() {
			return this.date;
		}
		
		public String getDateString() {
			return this.dateString;
		}
		
		public int getId() {
			return this.id;
		}
		
		public int getLeadHours() {
			long origSec = this.origin.getMillis() / 1000;
			long fcstSec = this.date.getMillis() / 1000;
			return (int) ((fcstSec - origSec) / 3600);
		}
		
		public DateTime getOrigin() {
			return this.origin;
		}
		
		public int getTemp() {
			return this.temp;
		}
		
		public float getWindSpeed() {
			return this.windspeed;
		}	
		
		public void setWindSpeedObservation(float ws) {
			if (ws < -90) { //bad data
				this.windSpeedObservation = 0;
				this.wsError = 0;
				this.noObservation = true;
			} else {
				this.windSpeedObservation = ws;
				this.wsError = ws - this.windspeed;
				this.noObservation = false;
			}
			return;
		}
		
		public boolean windSpeedObservationNotAvailable() {
			return this.noObservation;
		}
		
		public boolean windSpeedObservationAvailable() {
			return !this.noObservation;
		}
		public float getWindSpeedObservation() {
			return this.windSpeedObservation;
		}
		
		public float getWindSPeedError() {
			return this.wsError;
		}
		
		public String getOriginString() {
			return this.originString;
		}

		@Override
		public boolean equals(Object ob) {
			if (ob == null) {
				return false;
			}
			if (ob instanceof WeatherForecast) {
				WeatherForecast wf = (WeatherForecast) ob;
				boolean eql = (this.compareTo(wf) == 0);
				return eql;
				
			} else {
				return false;
			}
		}
		public int compareTo (WeatherForecast wr) {
			DateTime myDate, hisDate;
			DateTime myOrigin, hisOrigin;
			
			DateTimeFormatter dtfmt = DateTimeFormat.forPattern(WsData.dateFormat);
			myDate = dtfmt.parseDateTime(this.dateString);
			hisDate = dtfmt.parseDateTime(wr.getDateString());
			myOrigin = dtfmt.parseDateTime(this.originString);
			hisOrigin = dtfmt.parseDateTime(wr.getOriginString());
			
			if (myOrigin.isBefore(hisOrigin)) {
				return -1;
			} else if (myOrigin.isAfter(hisOrigin)) {
				return 1;
			} else if (myDate.isBefore(hisDate)) {
				return -1;
			} else if (myDate.isAfter(hisDate)) {
				return 1;				
			} else {
				return 0;
			}
			
		}
		
	} //class WeatherForecast

	@XStreamAlias("weatherReports")
	public static class WeatherReports {
		@XStreamImplicit
		private SortedSet<WeatherReport> wReports = new TreeSet<WeatherReport>();
		@XStreamOmitField
		private Map<DateTime, Float> mapDateWindSpeed = null;
		
		public WeatherReports() {}
		
		public void bildMaps() {
			this.mapDateWindSpeed = new HashMap<DateTime, Float>();
			for (WeatherReport wr : wReports) {
				mapDateWindSpeed.put(wr.date, wr.wspeed);
			}
		}
		
		public float getWindSpeed(DateTime dt) {
			if (mapDateWindSpeed.containsKey(dt)) {
				return mapDateWindSpeed.get(dt);
			}
			else {
				return -100; //garbage value
			}
		}
		
		public void addWeatherReport(WeatherReport wr) {
			if (wr != null) {
				wReports.add(wr);
			}
		}

		public void addWeatherReports(WeatherReports wr) {
			if (wr != null) {
				SortedSet<WeatherReport> wrs = new TreeSet<WeatherReport>(wr.getWeatherReports());
				wReports.addAll(wrs);
			}
		}
		
		public Set<WeatherReport> getWeatherReports() {
			return Collections.unmodifiableSortedSet(wReports);
		}
		
		public void convertToDate() {
			for (WeatherReport wr : wReports) {
				wr.convertToDate();
			}
		}
	}
	
	@XStreamAlias("weatherForecasts")
	public static class WeatherForecasts {
	    @XStreamImplicit
		private SortedSet<WeatherForecast> wForecasts = new TreeSet<WeatherForecast>();
	    
	    public WeatherForecasts() {}
	    
	    public void convertToDate1() {
	    	for (WeatherForecast wf : wForecasts) {
	    		wf.convertToDate();
	    	}
	    }
	    
	    public void addWeatherForecast(WeatherForecast wf) {
	    	if (wf != null) {
	    		wForecasts.add(wf);
	    	}
	    }
	    
	    public void addWeatherForecasts(WeatherForecasts wf) {
	    	if (wf != null) {
	    		SortedSet<WeatherForecast> wfc = new TreeSet<WeatherForecast>(wf.getWeatherForecasts());
	    		wForecasts.addAll(wfc); //make a copy to add
	    	}
	    }	  
	    
		public Set<WeatherForecast> getWeatherForecasts() {
			return Collections.unmodifiableSortedSet(wForecasts);
		}
		
		public void calcWindSpeedForecastErrors (WeatherReports wrps) {
			
			for (WeatherForecast wf : this.wForecasts) {
				//get forecast date
				DateTime forecastDateTime = wf.getDate();
				if (forecastDateTime == null) continue;

				//get observation for the origin
				float observedWindSpeed = wrps.getWindSpeed(forecastDateTime);
				if (observedWindSpeed < 0) continue;
				
				// set wind speed observation
				wf.setWindSpeedObservation(observedWindSpeed);
				
			} //for each weather forecast	
		}
	}
	
	@XStreamAlias("weatherReports")
	private WeatherReports weatherReports;
	
	@XStreamAlias("weatherForecasts")
	private WeatherForecasts weatherForecasts;

	public WsData(WeatherReports rep, WeatherForecasts wfc) {
		weatherReports = rep;
		weatherForecasts = wfc;
	}
	
	public void convertToDate() {
		this.weatherReports.convertToDate();
		this.weatherForecasts.convertToDate1();
		return;
	}
	
	public WeatherReports getWeatherReports() {
		return weatherReports;
	}
	
	public WeatherForecasts getWeatherForecasts() {
		return weatherForecasts;
	}

	public void calcWindSpeedForecastErrors() {
		this.weatherForecasts.calcWindSpeedForecastErrors(this.weatherReports);
		return;
	}
	
} //class WsData