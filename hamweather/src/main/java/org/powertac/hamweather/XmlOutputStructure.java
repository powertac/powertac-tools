/*
 * Copyright (c) 2014 by the original author
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.powertac.hamweather;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Builds batches of weather reports and corresponding forecasts.
 * Each batch is 24h of data and the corresponding 24h of 24h forecasts.
 * Incomplete batches are discarded.
 * @author John Collins
 */
public class XmlOutputStructure implements OutputStructure
{
  static enum XmlState {WAITING, GATHERING}
  static final double KPH_MPS = 1000.0 / 3600.0;
  static final int HOUR = 3600 * 1000;
  static final int BLOCKSIZE = 24;

  private XmlState state = XmlState.WAITING;
  //private DateTime start = null;
  private Integer batchStartHour;
  private ArrayList<Observation> observations;
  private ArrayList<Forecast> forecasts;

  private String outputFile;
  private Document doc;
  private Element rootElement;

  private DateTimeFormatter iso;
  //private DateTime start;

  public XmlOutputStructure ()
  {
    iso = ISODateTimeFormat.dateTimeNoMillis();
    observations = new ArrayList<Observation>();
    forecasts = new ArrayList<Forecast>();
    DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder docBuilder;
    try {
      docBuilder = docFactory.newDocumentBuilder();

      // Root element
      doc = docBuilder.newDocument();
      doc.setXmlStandalone(true);
      rootElement = doc.createElement("data");
      doc.appendChild(rootElement);
    }
    catch (ParserConfigurationException e) {
      e.printStackTrace();
    }
  }

  /* (non-Javadoc)
   * @see org.powertac.hamweather.OutputStructure#addObservation(org.joda.time.DateTime, long, long, long, long)
   */
  @Override
  public void addObservation (DateTime when, long temp, long dewpoint,
                              long pressure, long windKPH)
  {
    startMaybe(when);
    if (state == XmlState.GATHERING) {
      if (observations.size() == BLOCKSIZE) {
        // write out and re-initialize
        // this is here because we have to check after gathering up the
        // forecasts from the previous observation.
        buildBlock();
        state = XmlState.WAITING;
        startMaybe(when);
      }
      else if (skipped(when)) {
        // we've lost data - start over
        state = XmlState.WAITING;
        startMaybe(when);
        if (state == XmlState.WAITING)
          // discard and wait for start of next block
          return;
      }
      observations.add(new Observation(when, temp, dewpoint, pressure,
                                       windKPH * KPH_MPS));
    }
  }

  /* (non-Javadoc)
   * @see org.powertac.hamweather.OutputStructure#addForecast(org.joda.time.DateTime, int, org.joda.time.DateTime, long, long, long, long)
   */
  @Override
  public void addForecast (DateTime when, int index, DateTime hour,
                           long temp, long dewpoint, long sky, long windKPH)
  {
    if (state == XmlState.GATHERING) {
      forecasts.add(new Forecast(when, index, hour, temp,
                                 dewpoint, windKPH * KPH_MPS));
    }
  }

  /* (non-Javadoc)
   * @see org.powertac.hamweather.OutputStructure#forecastMissing()
   */
  @Override
  public void forecastMissing ()
  {
    state = XmlState.WAITING;
  }

  @Override
  public void setOutputFile (String filename)
  {
    outputFile = filename;
  }

  @Override
  public void setBatchStartHour (Integer hour)
  {
    batchStartHour = hour;
  }

  /* (non-Javadoc)
   * @see org.powertac.hamweather.OutputStructure#write()
   */
  @Override
  public void write ()
  {
    FileOutputStream out;
    try {
      out = new FileOutputStream(new File(outputFile));
      TransformerFactory tFactory = TransformerFactory.newInstance();
      Transformer transformer = tFactory.newTransformer();
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");

      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(out);
      transformer.transform(source, result);
      out.close();
    }
    catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    catch (TransformerConfigurationException e) {
      e.printStackTrace();
    }
    catch (TransformerException e) {
      e.printStackTrace();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  // If it's time to start, then re-initialize
  private void startMaybe (DateTime current)
  {
    if (state == XmlState.GATHERING)
      return;
    if (state == XmlState.WAITING) {
      if (timeToStart(current)) {
        //start = current;
        state = XmlState.GATHERING;
        observations.clear();
        forecasts.clear();
      }
    }
    else {
      System.out.println("Bogus state " + state);
    }
  }

  private boolean timeToStart (DateTime current)
  {
    if (null == batchStartHour)
      return true;
    int hour = current.hourOfDay().get();
    if (current.minuteOfHour().get() >= 30)
      hour += 1;
    if (hour == batchStartHour)
      return true;
    return false;
  }

  private boolean skipped(DateTime when)
  {
    if (observations.size() == 0)
      return false;
    Observation last = observations.get(observations.size() - 1);
    DateTime fence = last.when.plus(HOUR + HOUR/2);
    if (when.isAfter(fence)) {
      System.out.println("Observation skipped at " + iso.print(fence));
      return true;
    }
    return false;
  }

  private void buildBlock ()
  {
    // weatherReports elements
    Element weatherReports = doc.createElement("weatherReports");
    rootElement.appendChild(weatherReports);

    for (Observation weather: observations) {
      Element weatherReport = doc.createElement("weatherReport");
      weatherReport.setAttribute("date", weather.when.toString(iso));
      weatherReport.setAttribute("windspeed", weather.windMPS.toString());
      weatherReports.appendChild(weatherReport);
    }

    // weatherForecasts elements
    Element weatherForecasts = doc.createElement("weatherForecasts");
    rootElement.appendChild(weatherForecasts);

    for (Forecast forecast: forecasts) {
      Element weatherForecast = doc.createElement("weatherForecast");
      weatherForecast.setAttribute("date", forecast.when.toString(iso));
      weatherForecast.setAttribute("id", forecast.id.toString());
      weatherForecast.setAttribute("origin", forecast.origin.toString(iso));
      weatherForecast.setAttribute("temp", forecast.temp.toString());
      weatherForecast.setAttribute("windspeed", forecast.windMPS.toString());
      weatherForecasts.appendChild(weatherForecast);
    }
  }

  // Data structures
  class Observation
  {
    DateTime when;
    Long temp;
    Long dewpoint;
    Long pressure;
    Double windMPS;

    Observation (DateTime when, Long temp, Long dewpoint,
                        Long pressure, double windMPS)
    {
      this.when = when;
      this.temp = temp;
      this.dewpoint = dewpoint;
      this.pressure = pressure;
      this.windMPS = windMPS;
    }
  }

  class Forecast
  {
    DateTime when;
    Integer id;
    DateTime origin;
    Long temp;
    Long dewpoint;
    Double windMPS;

    Forecast (DateTime when, int id, DateTime origin, Long temp,
              Long dewpoint, Double windMPS)
    {
      this.when = when;
      this.id = id;
      this.origin = origin;
      this.temp = temp;
      this.dewpoint = dewpoint;
      this.windMPS = windMPS;
    }
  }
}
