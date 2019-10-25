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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class TiDBSplit
        implements ConnectorSplit
{
    private final long idx;
    private final String connectorId;
    private final String pdaddresses;
    private final String schemaName;
    private final String tableName;
    private final long tableId;

    private final String startKey;
    private final String endKey;

    private final TupleDomain<ColumnHandle> tupleDomain;

    private final boolean enablePPD;

    private final boolean remotelyAccessible;

    @JsonCreator
    public TiDBSplit(
            @JsonProperty("idx") long idx,
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("pdaddresses") String pdaddresses,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("startKey") String startKey,
            @JsonProperty("endKey") String endKey,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("enablePPD") boolean enablePPD)
    {
        this.idx = requireNonNull(idx, "idx is null");
        this.pdaddresses = requireNonNull(pdaddresses, "pdaddresses name is null");
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.tableId = requireNonNull(tableId, "table id is null");

        this.startKey = requireNonNull(startKey, "startKey is null");

        this.endKey = requireNonNull(endKey, "endKey is null");

        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");

        this.enablePPD = enablePPD;

        remotelyAccessible = true;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain() {
        return tupleDomain;
    }

    @JsonProperty
    public long getIdx() {
        return idx;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getPdaddresses() {
        return pdaddresses;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public long getTableId() {
        return tableId;
    }

    @JsonProperty
    public String getStartKey() {
        return startKey;
    }

    @JsonProperty
    public String getEndKey() {
        return endKey;
    }

    @JsonProperty
    public boolean isEnablePPD() {
        return enablePPD;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        // only http or https is remotely accessible
        return remotelyAccessible;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return new ArrayList<>();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
