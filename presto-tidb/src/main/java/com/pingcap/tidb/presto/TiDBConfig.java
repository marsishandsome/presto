/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pingcap.tidb.presto;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class TiDBConfig {

  private String pdaddresses;
  private boolean enablePPD;

  @NotNull
  public String getPdaddresses() {
    return pdaddresses;
  }

  @NotNull
  public boolean isEnablePPD()
  {
    return enablePPD;
  }

  @Config("spark.tispark.pd.addresses")
  public TiDBConfig setPdaddresses(String pdaddresses) {
    this.pdaddresses = pdaddresses;
    return this;
  }

  @Config("tipresto.enable.ppd")
  public TiDBConfig setEnablePPD(boolean enablePPD) {
    this.enablePPD = enablePPD;
    return this;
  }
}
