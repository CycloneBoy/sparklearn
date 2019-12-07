package com.cycloneboy.bigdata.communication.utils;

import com.cycloneboy.bigdata.communication.converter.DimensionConverterImpl;
import com.cycloneboy.bigdata.communication.kv.key.DateDimension;
import java.sql.Connection;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/** Create by sl on 2019-12-07 11:30 */
@Slf4j
public class JdbcUtilsTest {

  @Test
  public void getConnection() {
    Connection connection = null;

    DateDimension dateDimension = new DateDimension();
    dateDimension.setYear(2017);
    dateDimension.setMonth(1);
    dateDimension.setDay(1);

    //    id = execSql(connection, sqls, dimension);
    DimensionConverterImpl dimensionConverter = new DimensionConverterImpl();
    int dimensionId = dimensionConverter.getDimensionId(dateDimension);

    log.info("id :{} , {}", dimensionId, dateDimension.toString());
  }
}
