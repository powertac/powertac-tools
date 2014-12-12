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

import org.joda.time.DateTime;

/**
 * @author jcollins
 */
public interface OutputStructure
{

  /**
   * adds an observation, starting a new block if necessary.
   * discards accumulated data and starts over in case there's a gap
   * in the sequence.
   */
  public void
  addObservation (DateTime obTime, long temp,
                  long dewpoint, long pressure, long windKPH);

  /**
   * Adds a forecast to the current block.
   */
  public void
  addForecast (DateTime fcTime, int index, DateTime hour,
               long temp, long dewpoint, long sky, long windKPH);

  public void forecastMissing ();

  public void setOutputFile (String filename);

  public void setBatchStartHour (Integer hour);

  public void write ();
}
