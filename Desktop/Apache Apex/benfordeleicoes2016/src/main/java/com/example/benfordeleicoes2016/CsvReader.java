/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.example.benfordeleicoes2016;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Extracts columns from csv input line
 *
 * @since 1.0
 */
public class CsvReader extends BaseOperator {
  // default pattern for column separators
  private static final String csvSplit = ";";

  /**
   * Output port on which party and value from the current file are emitted
   */
  public final transient DefaultOutputPort<String[]> output = new DefaultOutputPort<>();

  /**
   * Input port on which lines from the current file are received
   */
  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {

    @Override
    public void process(String line)
    {
      // line; split it into party and value and emit them
      String[] spl = line.split(csvSplit);
      String[] data = new String[2];
      data[0] = spl[8].replace("\"", "");   // Party name
      data[1] = spl[22].replace("\"", "");  // Value
      output.emit(data);
    }
  };
}
