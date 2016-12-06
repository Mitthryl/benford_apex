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

import java.awt.*;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Computes word frequencies per window and emits them at each {@code endWindow()}. The output is a
 * list of (word, frequency) pairs
 *
 * @since 1.0
 */
public class WindowWordCount extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(WindowWordCount.class);
    private static Integer nLines = Integer.MAX_VALUE;
    private static Integer counter = 0;
    /**
     * {@literal (partyMap => numbers frequency)} map for current window
     */
    protected Map<String, PartyData> partyMap = new HashMap<>();
    private static boolean EOF = false;
    private final DecimalFormat df;

    public WindowWordCount() {
        df = new DecimalFormat();
        DecimalFormatSymbols dfs = new DecimalFormatSymbols();
        dfs.setDecimalSeparator(',');
        df.setDecimalFormatSymbols(dfs);
    }

    /**
     * Input port on which words are received
     */
    public final transient DefaultInputPort<String[]> input = new DefaultInputPort<String[]>() {
        @Override
        public void process(String[] data) {
            if(counter < nLines) {
                counter++;
                String party = data[0];
                double value = 0.0;
                try {
                    value = df.parse(data[1]).doubleValue();
                } catch (ParseException e) {
                    LOG.info("Benford number value invalid!");
                    e.printStackTrace();
                }

                int ben = 0;
                try {
                    ben = benford(value);
                } catch (Exception e) {
                    LOG.info("Benford invalid result!");
                }

                PartyData pData = partyMap.get(party);
                if (null != pData) {    // party seen previously
                    pData.count[ben] += 1;
                    return;
                }

                // novo partido
                pData = new PartyData(party);
                pData.count[ben] = 1;
                partyMap.put(party, pData);
            }// else {
//                final ArrayList<PartyData> list = new ArrayList<>(partyMap.values());
//                output.emit(list);
//            }
        }
    };

    private int benford(double n) throws Exception {
        double tam = Math.floor(Math.log10(n));
        int ben = (int) Math.floor(n / (Math.pow(10, tam)));
        if (ben < 0 || ben > 9)
            throw new Exception("Benford failed!");
        return ben;
    }

    /**
     //   * Control port on which the current file name is received to indicate EOF
     //   */
  public final transient DefaultInputPort<Integer> control = new DefaultInputPort<Integer>() {
        @Override
        public void process(Integer count) {
            nLines = count;
        }
    };

    /**
     * Output port which emits the list of word frequencies for current window
     */
    public final transient DefaultOutputPort<List<PartyData>> output = new DefaultOutputPort<>();

    /**
     * {@inheritDoc}
     * If we've seen some words in this window, emit the map and clear it for next window
     */
    @Override
    public void endWindow() {
        if(counter >= nLines) {
            // got EOF; if no words found, do nothing
            if (partyMap.isEmpty()) {
                return;
            }

            // have some words; emit single map and reset for next file
            final ArrayList<PartyData> list = new ArrayList<>(partyMap.values());
            output.emit(list);
            list.clear();
            partyMap.clear();
        }
    }
}
