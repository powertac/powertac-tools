package org.powertac.logtool.common;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.powertac.common.Broker;
import org.powertac.common.DistributionTransaction;
import org.powertac.common.Order;
import org.powertac.common.Timeslot;
import org.powertac.common.WeatherForecast;
import org.powertac.common.WeatherForecastPrediction;

public class DomainObjectReaderTest
{
  private DomainObjectReader dor;
  
  @Before
  public void setUp () throws Exception
  {
    dor = new DomainObjectReader();
  }

  @Test
  public void testReadSingleObject ()
  {
    String aston = "144669:org.powertac.common.Broker::603::new::AstonTAC";
    Object result = dor.readObject(aston);
    assertNotNull("created an instance", result);
    assertEquals("correct class", "org.powertac.common.Broker", result.getClass().getName());
    assertEquals("correct id", 603, ((Broker)result).getId());
    assertEquals("object stored in map", result, dor.getById(603));
  }

  @Test
  public void read2Objects ()
  {
    String aston = "144669:org.powertac.common.Broker::603::new::AstonTAC";
    Broker broker = (Broker)dor.readObject(aston);
    String dt = "189426:org.powertac.common.DistributionTransaction::3459::new::603::2009-01-03T03:00:00.000Z::-0.0::0.0";
    Object result = dor.readObject(dt);
    assertEquals("correct class", "org.powertac.common.DistributionTransaction", result.getClass().getName());
    DistributionTransaction dtx = (DistributionTransaction)result;
    assertEquals("correct id", 3459, dtx.getId());
    assertEquals("object stored in map", result, dor.getById(3459));
    assertEquals("broker stored", broker, dtx.getBroker());
  }
  
  @Test
  public void readRRObject ()
  {
    String aston = "144669:org.powertac.common.Broker::603::new::AstonTAC";
    Broker broker = (Broker)dor.readObject(aston);
    String ts = "13678:org.powertac.common.Timeslot::579::new::362::2009-01-03T02:00:00.000Z::578";
    Timeslot timeslot = (Timeslot)dor.readObject(ts);
    assertNotNull("timeslot created", timeslot);
    String order = "180915:org.powertac.common.Order::400000393::new::603::579::2.109375::-31.835472671068615";
    Object result = dor.readObject(order);
    assertNotNull("order created", result);
    assertEquals("correct class", "org.powertac.common.Order", result.getClass().getName());
    Order o = (Order)result;
    assertEquals("correct id", 400000393, o.getId());
    assertEquals("order in map", o, dor.getById(400000393));
    assertEquals("correct broker", broker, o.getBroker());
    assertEquals("correct mwh", 2.109375, o.getMWh(), 1e-6);
    assertEquals("correct price", -31.835472671068615, o.getLimitPrice(), 1e-6);
  }
  
  @Test
  public void readList ()
  {
    String fp1 = "176271:org.powertac.common.WeatherForecastPrediction::1203::new::23::-6.447321391818082::2.657257536071654::121.71284773822428::0.375";
    String fp2 = "176272:org.powertac.common.WeatherForecastPrediction::1204::new::24::-7.327664553479619::1.8344251307130162::114.31156703428204::0.375";
    String forecast = "176272:org.powertac.common.WeatherForecast::1205::new::597::(1203,1204)";
    WeatherForecastPrediction wfp1 = (WeatherForecastPrediction)dor.readObject(fp1);
    WeatherForecastPrediction wfp2 = (WeatherForecastPrediction)dor.readObject(fp2);
    Object result = dor.readObject(forecast);
    assertNotNull("read a forecast", result);
    assertEquals("correct class", "org.powertac.common.WeatherForecast", result.getClass().getName());
    WeatherForecast wf = (WeatherForecast)result;
    List<WeatherForecastPrediction> predictions = wf.getPredictions();
    assertEquals("correct number of predictions", 2, predictions.size());
    assertEquals("correct first prediction", wfp1, predictions.get(0));
    assertEquals("correct second prediction", wfp2, predictions.get(1));
  }
}
