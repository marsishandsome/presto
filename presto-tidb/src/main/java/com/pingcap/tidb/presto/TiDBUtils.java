package com.pingcap.tidb.presto;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.spi.type.Type;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.MySQLType;

import java.util.ArrayList;
import java.util.List;


public class TiDBUtils {
  public static List<ColumnMetadata> getColumnsMetadata(TiTableInfo tiTableInfo) {
    ArrayList<ColumnMetadata> result = new ArrayList<>();
    for (TiColumnInfo tiColumnInfo : tiTableInfo.getColumns()) {
      result.add(toColumnMetadata(tiColumnInfo));
    }
    return result;
  }

  private static ColumnMetadata toColumnMetadata(TiColumnInfo tiColumnInfo) {
    Type t = toType(tiColumnInfo.getType());
    if(t == null) {
      System.out.println("null");
    }
    ColumnMetadata result = new ColumnMetadata(tiColumnInfo.getName(), t);
    return result;
  }

  private static Type toType(DataType type) {
    if (type.getType() == MySQLType.TypeDecimal) {
      return null;
    } else if (type.getType() == MySQLType.TypeTiny) {
      return BigintType.BIGINT;
    } else if (type.getType() == MySQLType.TypeShort) {
      return BigintType.BIGINT;
    } else if (type.getType() == MySQLType.TypeLong) {
      return BigintType.BIGINT;
    } else if (type.getType() == MySQLType.TypeFloat) {
      return DoubleType.DOUBLE;
    } else if (type.getType() == MySQLType.TypeDouble) {
      return DoubleType.DOUBLE;
    } else if (type.getType() == MySQLType.TypeNull) {
      return null;
    } else if (type.getType() == MySQLType.TypeTimestamp) {
      return TimestampType.TIMESTAMP;
    } else if (type.getType() == MySQLType.TypeLonglong) {
      return BigintType.BIGINT;
    } else if (type.getType() == MySQLType.TypeInt24) {
      return BigintType.BIGINT;
    } else if (type.getType() == MySQLType.TypeDate) {
      return DateType.DATE;
    } else if (type.getType() == MySQLType.TypeDuration) {
      return TimeType.TIME;
    } else if (type.getType() == MySQLType.TypeDatetime) {
      return null;
    } else if (type.getType() == MySQLType.TypeYear) {
      return null;
    } else if (type.getType() == MySQLType.TypeNewDate) {
      return DateType.DATE;
    } else if (type.getType() == MySQLType.TypeBit) {
      return BigintType.BIGINT;
    } else if (type.getType() == MySQLType.TypeJSON) {
      return VarcharType.createUnboundedVarcharType();
    } else if (type.getType() == MySQLType.TypeNewDecimal) {
      return DecimalType.createDecimalType((int)type.getLength(), type.getDecimal());
    } else if (type.getType() == MySQLType.TypeEnum) {
      return VarcharType.createUnboundedVarcharType();
    } else if (type.getType() == MySQLType.TypeSet) {
      return VarcharType.createUnboundedVarcharType();
    } else if (type.getType() == MySQLType.TypeTinyBlob) {
      return null;
    } else if (type.getType() == MySQLType.TypeMediumBlob) {
      return null;
    } else if (type.getType() == MySQLType.TypeLongBlob) {
      return null;
    } else if (type.getType() == MySQLType.TypeBlob) {
      return null;
    } else if (type.getType() == MySQLType.TypeVarString) {
      return VarcharType.createUnboundedVarcharType();
    } else if (type.getType() == MySQLType.TypeGeometry) {
      return null;
    } else if (type.getType() == MySQLType.TypeVarchar) {
      return VarcharType.createUnboundedVarcharType();
    } else if (type.getType() == MySQLType.TypeString) {
      return VarcharType.createUnboundedVarcharType();
    } else {
      return null;
    }
  }
}
