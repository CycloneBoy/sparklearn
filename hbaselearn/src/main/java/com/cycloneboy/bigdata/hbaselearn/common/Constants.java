package com.cycloneboy.bigdata.hbaselearn.common;

/** Create by sl on 2019-12-02 13:46 */
public class Constants {

  public static final String TABLE_STUDENT2 = "student2";

  public static final String NAMESPACE_WEIBO = "weibo";

  public static final String DELIMITER_COLON = ":";
  public static final String DELIMITER_UNDERLINE = "_";

  /** 微博内容表 */
  public static final String TABLE_CONTENT = NAMESPACE_WEIBO + DELIMITER_COLON + "content";

  /** 微博用户关系表 */
  public static final String TABLE_RELATION = NAMESPACE_WEIBO + DELIMITER_COLON + "relation";

  /** 微博收件箱表 */
  public static final String TABLE_INBOX = NAMESPACE_WEIBO + DELIMITER_COLON + "inbox";

  public static final String COLUMN_FAMILY_INFO = "info";
  public static final String COLUMN_FAMILY_ATTENDS = "attends";
  public static final String COLUMN_FAMILY_FANS = "fans";

  public static final String QUALIFIER_CONTENT = "content";

  public static final int WEIBO_INBOX_MAX_VERSION = 1000;
  public static final int WEIBO_INBOX_GET_MAX_VERSION = 5;

  // 2 * 1024 * 1024;
  public static final int COLUMN_BLOCK_SIZE = 2097152;

  public static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
}
