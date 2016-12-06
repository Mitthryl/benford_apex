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

import java.io.*;

import com.datatorrent.lib.io.SimpleSinglePortInputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

/**
 * Reads lines from input file and returns them. If EOF is reached, a control tuple
 * is emitted on the control port
 *
 * @since 1.0
 */
public class LineReader extends SimpleSinglePortInputOperator<String> implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(LineReader.class);

  protected String fileName = "/user/dtadmin/data/test.txt";

  private static Integer nLines = 0;
  /**
   * Control port on which the current file name is emitted to indicate EOF
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Integer> control = new DefaultOutputPort<>();


  @Override
  public void run() {
    BufferedReader br = null;
    DataInputStream in = null;
    InputStream fstream = null;

    try {
      String line;
      fstream = new FileInputStream(fileName);// this.getClass().getClassLoader().getResourceAsStream(fileName);

      in = new DataInputStream(fstream);
      br = new BufferedReader(new InputStreamReader(in));

      while ((line = br.readLine()) != null) {
        outputPort.emit(line);
        nLines++;
      }
      if (control.isConnected()) {
        LOG.info("readEntity: EOF for {}", fileName);
        control.emit(nLines);
      }

    } catch (IOException ex) {
      LOG.debug(ex.toString());
    } finally {
      try {
        if (br != null) {
          br.close();
        }
        if (in != null) {
          in.close();
        }
        if (fstream != null) {
          fstream.close();
        }
      } catch (IOException exc) {
        // nothing
      }
    }
  }
}
