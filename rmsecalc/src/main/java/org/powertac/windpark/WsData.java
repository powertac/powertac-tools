package org.powertac.windpark;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
//import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

@XStreamAlias("data")
public class WsData 
{
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
		private Date date;
				
		public WeatherReport(String dt, float speed) throws ParseException {
			this.dateString = dt;
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			this.date = df.parse(dt);
			this.wspeed = speed;
		}
		
		public void convertToDate() throws ParseException {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			this.date = df.parse(this.dateString);
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
			Date myDate, hisDate;
			SimpleDateFormat dtfmt = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			try {
				myDate = dtfmt.parse(this.dateString);
				hisDate = dtfmt.parse(wr.getDateString());
				
			} catch (ParseException e) {
				e.printStackTrace();
				return 0;
			}
			
			if (myDate.before(hisDate)) {
				return -1;
			} else if (myDate.after(hisDate)) {
				return 1;
				
			} else {
				return 0;
			}
			
		}
		
		public Date getDate() {
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
		private Date date;
		
		@XStreamAlias("id")
		@XStreamAsAttribute
		private int id;
		
		@XStreamAlias("origin")
		@XStreamAsAttribute
		private String originString;
		
		@XStreamOmitField
		private Date origin;
		
		@XStreamAlias("windspeed")
		@XStreamAsAttribute
		private float windspeed;
		
		@XStreamOmitField
		private float wsError = 0; //wind speed error
		
		@XStreamOmitField
		private float windSpeedObservation = 0;
		
		@XStreamOmitField
		private boolean noObservation = true;
		
		public WeatherForecast (String dt, int myId, String org, float spd) throws ParseException {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			this.date = df.parse(dt);
			this.dateString = dt;
			this.id = myId;
			this.origin = df.parse(org);
			this.originString = org;
			this.windspeed = spd;
		}
		
		public void convertToDate() throws ParseException {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			this.date = df.parse(this.dateString);
			this.origin = df.parse(this.originString);
			return;
		}
		
		public Date getDate() {
			return this.date;
		}
		
		public String getDateString() {
			return this.dateString;
		}
		
		public int getId() {
			return this.id;
		}
		
		public Date getOrigin() {
			return this.origin;
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
			Date myDate, hisDate;
			Date myOrigin, hisOrigin;
			
			SimpleDateFormat dtfmt = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			try {
				myDate = dtfmt.parse(this.dateString);
				hisDate = dtfmt.parse(wr.getDateString());
				myOrigin = dtfmt.parse(this.originString);
				hisOrigin = dtfmt.parse(wr.getOriginString());
				
			} catch (ParseException e) {
				e.printStackTrace();
				return 0;
			}
			
			if (myOrigin.before(hisOrigin)) {
				return -1;
			} else if (myOrigin.after(hisOrigin)) {
				return 1;
			} else if (myDate.before(hisDate)) {
				return -1;
			} else if (myDate.after(hisDate)) {
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
		private Map<Date, Float> mapDateWindSpeed = null;
		
		public WeatherReports() {}
		
		public void bildMaps() {
			this.mapDateWindSpeed = new HashMap<Date, Float>();
			for (WeatherReport wr : wReports) {
				mapDateWindSpeed.put(wr.date, wr.wspeed);
			}
		}
		
		public float getWindSpeed(Date dt) {
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
		
		public void convertToDate() throws ParseException {
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
	    
	    public void convertToDate() throws ParseException {
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
		
		public void calcWindSpeedForecastErrors(WeatherReports wrps) {
			Date currDate = null;
			float currWspeed = 0;
			for (WeatherForecast wf : wForecasts) {
				Date forecastDate = wf.getDate();
				if ((currDate == null) ||
					(currDate.compareTo(forecastDate)!= 0)) {
					currWspeed = wrps.getWindSpeed(forecastDate);
					currDate = forecastDate;					
				}
				wf.setWindSpeedObservation(currWspeed); //this calculates the error too
			} //for all weather forecasts
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
	
	public void convertToDate() throws ParseException {
		this.weatherReports.convertToDate();
		this.weatherForecasts.convertToDate();
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