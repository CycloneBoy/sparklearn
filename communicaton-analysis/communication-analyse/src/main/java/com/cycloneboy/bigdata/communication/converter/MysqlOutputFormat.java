package com.cycloneboy.bigdata.communication.converter;

import com.cycloneboy.bigdata.communication.kv.key.ComDimension;
import com.cycloneboy.bigdata.communication.kv.value.CountCallDurationValue;
import com.cycloneboy.bigdata.communication.utils.JdbcInstance;
import com.cycloneboy.bigdata.communication.utils.JdbcUtils;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Create by sl on 2019-12-05 15:33 */
public class MysqlOutputFormat extends OutputFormat<ComDimension, CountCallDurationValue> {
  private OutputCommitter committer = null;

  @Override
  public RecordWriter<ComDimension, CountCallDurationValue> getRecordWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    Connection connection = null;
    connection = JdbcInstance.getConnection();
    try {
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      throw new RuntimeException(e.getMessage());
    }
    return new MysqlRecordWriter(connection);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    // 输出校检
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    if (committer == null) {
      String name = context.getConfiguration().get(FileOutputFormat.OUTDIR);
      Path outputPath = name == null ? null : new Path(name);
      committer = new FileOutputCommitter(outputPath, context);
    }
    return committer;
  }

  static class MysqlRecordWriter extends RecordWriter<ComDimension, CountCallDurationValue> {
    private DimensionConverterImpl dimensionConverter = new DimensionConverterImpl();
    private Connection connection = null;
    private PreparedStatement preparedStatement = null;
    private String insertSql = null;
    private int batchCount = 0;
    private int BATCH_SIZE = 500;

    public MysqlRecordWriter(Connection connection) {
      this.connection = connection;
    }

    @Override
    public void write(ComDimension key, CountCallDurationValue value)
        throws IOException, InterruptedException {

      try {
        // tb_call
        // id_date_contact, id_date_dimension, id_contact, call_sum, call_duration_sum

        // year month day
        int idDatedimension = dimensionConverter.getDimensionId(key.getDateDimension());
        int idContactdimension = dimensionConverter.getDimensionId(key.getContactDimension());

        String idDateContact = idDatedimension + "_" + idContactdimension;

        if (insertSql == null) {
          insertSql =
              "INSERT INTO `tb_call` (`id_date_contact`, `id_date_dimension`, `id_contact`, `call_sum`, `call_duration_sum`) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `id_date_contact` = ?;";
        }

        if (preparedStatement == null) {

          preparedStatement = connection.prepareStatement(insertSql);
        }

        // 本次SQL
        int i = 0;

        preparedStatement.setString(++i, idDateContact);
        preparedStatement.setInt(++i, idDatedimension);
        preparedStatement.setInt(++i, idContactdimension);
        preparedStatement.setInt(++i, value.getCallSum());
        preparedStatement.setInt(++i, value.getCallDurationSum());
        preparedStatement.setString(++i, idDateContact);
        preparedStatement.addBatch();

        batchCount++;
        if (batchCount >= BATCH_SIZE) {
          preparedStatement.executeBatch();
          connection.commit();
          batchCount = 0;
          preparedStatement.clearBatch();
        }

      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      try {

        if (preparedStatement != null) {
          preparedStatement.executeBatch();
          this.connection.commit();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      } finally {
        JdbcUtils.close(connection, preparedStatement, null);
      }
    }
  }
}
