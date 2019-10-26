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
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.pingcap.tikv.expression.Expression;

import java.util.Objects;

public class TiDBTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final TiDBTableHandle table;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final RowExpression predicate;

    @JsonCreator
    public TiDBTableLayoutHandle(@JsonProperty("table") TiDBTableHandle table,
                                 @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
                                 @JsonProperty("predicate") RowExpression predicate)
    {
        this.table = table;
        this.tupleDomain = tupleDomain;
        this.predicate = predicate;
    }

    @JsonProperty
    public RowExpression getPredicate() {
        return predicate;
    }

    @JsonProperty
    public TiDBTableHandle getTable()
    {
        return table;
    }

    public TupleDomain<ColumnHandle> getTupleDomain() {
        return tupleDomain;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TiDBTableLayoutHandle that = (TiDBTableLayoutHandle) o;
        return Objects.equals(table, that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
