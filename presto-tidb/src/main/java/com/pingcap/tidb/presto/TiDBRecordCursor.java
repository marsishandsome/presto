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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import com.pingcap.tikv.operation.iterator.CoprocessIterator;
import com.pingcap.tikv.row.Row;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.chrono.ISOChronology;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static com.facebook.presto.spi.type.Decimals.encodeShortScaledValue;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.joda.time.DateTimeZone.UTC;

public class TiDBRecordCursor
        implements RecordCursor
{
    private final List<TiDBColumnHandle> columnHandles;
    private final CoprocessIterator<Row> iterator;
    private Row row = null;

    public TiDBRecordCursor(List<TiDBColumnHandle> columnHandles, CoprocessIterator<Row> iterator)
    {
        this.columnHandles = columnHandles;
        this.iterator = iterator;
    }

    @Override
    public long getCompletedBytes()
    {
        return 100L;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if(iterator.hasNext()) {
            row = iterator.next();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        return (boolean) row.get(field, null);
    }

    @Override
    public long getLong(int field)
    {
        Type targetType = getType(field);
        if (targetType instanceof DecimalType) {
            BigDecimal bd = (BigDecimal) row.get(field, null);
            // long decimal type should not reach here
            return encodeShortScaledValue(bd, ((DecimalType) targetType).getScale());
        } else if (targetType instanceof DateType) {
            Date date = (Date) row.get(field, null);
            // Convert it to a ~midnight in UTC.
            long utcMillis = ISOChronology.getInstance().getZone().getMillisKeepLocal(UTC, date.getTime());
            // convert to days
            return MILLISECONDS.toDays(utcMillis);
        } else {
            return (long)row.get(field, null);
        }
    }

    @Override
    public double getDouble(int field)
    {
        return (double) row.get(field, null);
    }

    @Override
    public Slice getSlice(int field)
    {
        Type targetType = getType(field);
        if (targetType instanceof DecimalType)
        {
            BigDecimal bd = (BigDecimal) row.get(field, null);
            return encodeScaledValue(bd, ((DecimalType) targetType).getScale());
        }
        String v = row.getString(field);
        return Slices.utf8Slice(v);
    }

    @Override
    public Object getObject(int field)
    {
        return row.get(field, null);
    }

    @Override
    public boolean isNull(int field)
    {
        return row.isNull(field);
    }

    @Override
    public void close()
    {
    }
}
