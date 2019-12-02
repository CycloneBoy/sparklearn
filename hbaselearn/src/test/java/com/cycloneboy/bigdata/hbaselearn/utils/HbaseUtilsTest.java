package com.cycloneboy.bigdata.hbaselearn.utils;

import static com.cycloneboy.bigdata.hbaselearn.common.Constants.TABLE_STUDENT2;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/** Create by sl on 2019-12-02 13:32 */
@Slf4j
public class HbaseUtilsTest {

  @Test
  public void isTableExist() throws IOException {
    log.info("{} 是否存在 {}", TABLE_STUDENT2, HbaseUtils.isTableExist(TABLE_STUDENT2));
  }

  @Test
  public void createTable() throws IOException {
    HbaseUtils.createTable("tab03", "cf01", "cf02", "cf03");
    HbaseUtils.createTable("tab02", "cf01", "cf02", "cf03");
  }

  @Test
  public void dropTable() throws IOException {
    HbaseUtils.dropTable("tab02");
  }

  @Test
  public void addRowData() throws IOException {
    HbaseUtils.addRowData(TABLE_STUDENT2, "1005", "info", "sex", "male");
    HbaseUtils.addRowData(TABLE_STUDENT2, "1005", "info", "name", "Job");
    HbaseUtils.addRowData(TABLE_STUDENT2, "1005", "info", "age", "22");

    HbaseUtils.addRowData(TABLE_STUDENT2, "1006", "info", "sex", "male");
    HbaseUtils.addRowData(TABLE_STUDENT2, "1006", "info", "name", "Job");
    HbaseUtils.addRowData(TABLE_STUDENT2, "1006", "info", "age", "22");
  }

  @Test
  public void deleteMultiRow() throws IOException {
    HbaseUtils.deleteMultiRow(TABLE_STUDENT2, "1005", "1006");
  }

  @Test
  public void getAllRows() throws IOException {
    HbaseUtils.getAllRows(TABLE_STUDENT2);
  }

  @Test
  public void getRow() throws IOException {
    HbaseUtils.getRow(TABLE_STUDENT2, "1002");
  }

  @Test
  public void getRowQualifier() throws IOException {
    HbaseUtils.getRowQualifier(TABLE_STUDENT2, "1002", "info", "name");
    HbaseUtils.getRowQualifier(TABLE_STUDENT2, "1002", "info", "age");
  }
}
