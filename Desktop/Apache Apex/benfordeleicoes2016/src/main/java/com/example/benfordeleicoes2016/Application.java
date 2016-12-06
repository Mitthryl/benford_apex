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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "Benford")
public class Application implements StreamingApplication {
  private static final Logger LOG = LoggerFactory.getLogger(Application.class);

  /**
   * Populates the DAG with operators and connecting streams
   *
   * @param dag The directed acyclic graph of operators to populate
   * @param conf The configuration
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // create operators
    LineReader lineReader            = dag.addOperator("lineReader", new LineReader());
    CsvReader csvReader              = dag.addOperator("csvReader", new CsvReader());
    WindowWordCount windowWordCount  = dag.addOperator("windowWordCount", new WindowWordCount());
//    FileWordCount fileWordCount      = dag.addOperator("fileWordCount", new FileWordCount());
    WordCountWriter wcWriter         = dag.addOperator("wcWriter", new WordCountWriter());

    // create streams
    dag.addStream("lines",   lineReader.outputPort,  csvReader.input);
    dag.addStream("control", lineReader.control, windowWordCount.control);
    dag.addStream("words",   csvReader.output,  windowWordCount.input);
//    dag.addStream("windowWordCounts", windowWordCount.output, fileWordCount.input);
    dag.addStream("fileWordCounts", windowWordCount.output, wcWriter.input);
  }

}
