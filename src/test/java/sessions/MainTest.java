package sessions;

import junit.framework.TestCase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

public class MainTest extends TestCase {
  private static SparkSession spark;
  private static Object[] defaultRawData;
  private static Object[] defaultCleanData;
  private static Object[] defaultSessionizedData;
  private StructType rawSchema;
  private StructType cleanedSchema;
  private StructType sessionizedSchema;

  private Object[] updateDataField(
      String fieldName, Object value, StructType schema, Object[] originalRowData) {
    int index = Arrays.stream(schema.fieldNames()).collect(Collectors.toList()).indexOf(fieldName);
    Object[] rowData = originalRowData.clone();
    rowData[index] = value;
    return rowData;
  }

  public void setUp() throws Exception {
    spark = SparkSession.builder().appName("MainTest").master("local[*]").getOrCreate();
    // These methods are essential data "factory" methods to produce realistic test data
    // Having these here keeps the test a lot smaller and clearer; in a production system we'd
    // probably split these out into a data factory helper to share across the application
    rawSchema = getRawSchema();
    cleanedSchema = getCleanedSchema();
    sessionizedSchema = getSessionizedSchema();
    defaultRawData = getDefaultRawData();
    defaultCleanData = getDefaultCleanedData();
    defaultSessionizedData = getDefaultSessionizedData();
    super.setUp();
  }

  // NOTE: test names are written in snake_case contra to Java convention because it's easier to
  // read sentences this way. I think it's worth breaking with convention here to have more legible
  // longer test names expressing intent.
  public void test_clean_splits_ip_and_port() {
    Row defaultRow = RowFactory.create(defaultRawData.clone());
    Dataset<Row> df = spark.createDataFrame(List.of(defaultRow), rawSchema);

    Dataset<Row> cleanedDf = Main.clean(df);
    Row firstRow = cleanedDf.collectAsList().get(0);

    assertEquals("123.242.248.130", firstRow.getAs("client_ip"));
    assertEquals("54635", firstRow.getAs("client_port"));
    assertEquals("10.0.6.158", firstRow.getAs("backend_ip"));
    assertEquals("80", firstRow.getAs("backend_port"));
  }

  public void test_clean_parses_urls_to_endpoints() {
    Row withUrlParams = RowFactory.create(defaultRawData.clone());
    Row withoutUrlPrams =
        RowFactory.create(
            updateDataField(
                "request",
                "\"GET https://paytm.com:443/shop/cart HTTP/1.1\"",
                rawSchema,
                defaultRawData));
    Dataset<Row> df = spark.createDataFrame(List.of(withUrlParams, withoutUrlPrams), rawSchema);

    Dataset<Row> cleanedDf = Main.clean(df);

    Row cleanedWithUrlParams = cleanedDf.collectAsList().get(0);
    Row cleanedWithoutUrlParams = cleanedDf.collectAsList().get(1);
    assertEquals("/shop/authresponse", cleanedWithUrlParams.getAs("endpoint"));
    assertEquals("/shop/cart", cleanedWithoutUrlParams.getAs("endpoint"));
  }

  public void test_clean_labels_api_and_user_requests() {
    Row userRequestRow = RowFactory.create(defaultRawData.clone());

    // the /papi routes appear to be user-facing, resulting from searches, but this could be a
    // mis-interpretation on my part
    Row papiRequestRow =
        RowFactory.create(
            updateDataField(
                "request",
                "\"GET https://paytm.com:443/papi/v1/promosearch/product/17028045/offers?parent_id=5254231&price=599&channel=web&version=2 HTTP/1.1\"",
                rawSchema,
                defaultRawData));
    Row apiRequestRow =
        RowFactory.create(
            updateDataField(
                "request",
                "\"POST https://paytm.com:443/api/v1/expresscart/checkout?wallet=1 HTTP/1.1\"",
                rawSchema,
                defaultRawData));
    Dataset<Row> df =
        spark.createDataFrame(List.of(userRequestRow, papiRequestRow, apiRequestRow), rawSchema);

    Dataset<Row> cleanedDf = Main.clean(df);

    Row userRow = cleanedDf.collectAsList().get(0);
    Row papiRow = cleanedDf.collectAsList().get(1);
    Row apiRow = cleanedDf.collectAsList().get(2);
    assertEquals("user", userRow.getAs("request_type"));
    assertEquals("user", papiRow.getAs("request_type"));
    assertEquals("api", apiRow.getAs("request_type"));
  }

  public void test_sessionize_groups_sessions_by_session_id_after_inactivity_period() {
    Timestamp startTime = Timestamp.valueOf("2015-07-22 09:00:28.019143");
    Row first =
        RowFactory.create(updateDataField("timestamp", startTime, cleanedSchema, defaultCleanData));
    Row tenSecondsLater =
        RowFactory.create(
            updateDataField(
                "timestamp",
                Timestamp.valueOf(startTime.toLocalDateTime().plusSeconds(10)),
                cleanedSchema,
                defaultCleanData));
    Row seventyOneSecondsLater =
        RowFactory.create(
            updateDataField(
                "timestamp",
                Timestamp.valueOf(startTime.toLocalDateTime().plusSeconds(71)),
                cleanedSchema,
                defaultCleanData));

    Dataset<Row> cleanedDf =
        spark.createDataFrame(
            List.of(first, tenSecondsLater, seventyOneSecondsLater), cleanedSchema);
    int sessionLengthMinutes = 1;

    Dataset<Row> sessionizedDf = Main.sessionize(cleanedDf, sessionLengthMinutes);

    Row firstRow = sessionizedDf.collectAsList().get(0);
    Row tenSecondsLaterRow = sessionizedDf.collectAsList().get(1);
    Row seventyOneSecondsLaterRow = sessionizedDf.collectAsList().get(2);

    // The first two rows should have the same Sesssion ID
    String firstSessionId = "123.242.248.130_0";
    assertEquals(firstSessionId, firstRow.getAs("session_id"));
    assertEquals(firstSessionId, tenSecondsLaterRow.getAs("session_id"));

    // The last row should start a new session with a new Session ID
    String secondSessionId = "123.242.248.130_1";
    assertEquals(secondSessionId, seventyOneSecondsLaterRow.getAs("session_id"));
  }

  public void test_sessionize_separates_users_into_separate_sessions_by_client_ip() {
    String user1Ip = "123.242.248.130";
    String user2Ip = "123.122.238.340";
    Row user1 =
        RowFactory.create(updateDataField("client_ip", user1Ip, cleanedSchema, defaultCleanData));
    Row user2 =
        RowFactory.create(updateDataField("client_ip", user2Ip, cleanedSchema, defaultCleanData));

    Dataset<Row> cleanedDf = spark.createDataFrame(List.of(user1, user2), cleanedSchema);
    int sessionLengthMinutes = 1;

    Dataset<Row> sessionizedDf = Main.sessionize(cleanedDf, sessionLengthMinutes);

    Row user1Row = sessionizedDf.collectAsList().get(0);
    Row user2Row = sessionizedDf.collectAsList().get(1);

    // Session IDs are client_ip concatenated with an integer, separated by an underscore
    String user1SessionId = user1Ip + "_0";
    assertEquals(user1SessionId, user1Row.getAs("session_id"));

    String user2SessionId = user2Ip + "_0";
    assertEquals(user2SessionId, user2Row.getAs("session_id"));
  }

  public void test_aggregate_calculates_visits_and_session_length() {
    // Create a session with three visits to unique endpoints over 80 total seconds
    Timestamp startTime = Timestamp.valueOf("2015-07-22 09:00:28.019143");
    Row sessionVisit1 =
        RowFactory.create(
            updateDataField("timestamp", startTime, sessionizedSchema, defaultSessionizedData));
    Row sessionVisit2 =
        RowFactory.create(
            updateDataField(
                "timestamp",
                Timestamp.valueOf(startTime.toLocalDateTime().plusSeconds(25)),
                sessionizedSchema,
                defaultSessionizedData));
    Row sessionVisit3 =
        RowFactory.create(
            updateDataField(
                "endpoint",
                "/shop/cart",
                sessionizedSchema,
                updateDataField(
                    "timestamp",
                    Timestamp.valueOf(startTime.toLocalDateTime().plusSeconds(80)),
                    sessionizedSchema,
                    defaultSessionizedData)));
    Dataset<Row> sessionizedDf =
        spark.createDataFrame(
            List.of(sessionVisit1, sessionVisit2, sessionVisit3), sessionizedSchema);

    Dataset<Row> aggregatedDf = Main.aggregate(sessionizedDf);

    // The aggregates here for the session created above should be
    // An 80-second session with three visits, two unique
    Row aggregateRow = aggregatedDf.collectAsList().get(0);
    assertEquals(startTime, aggregateRow.getAs("start"));
    assertEquals(80L, (long) aggregateRow.getAs("session_length_seconds"));
    assertEquals(3L, (long) aggregateRow.getAs("total_visits"));
    assertEquals(2L, (long) aggregateRow.getAs("unique_visits"));
  }

  // Private methods here to keep the clutter of generating the schemas out of the way of the main
  // test code

  private StructType getRawSchema() {
    return createStructType(
        new StructField[] {
          createStructField("timestamp", DataTypes.TimestampType, true),
          createStructField("elb", DataTypes.StringType, true),
          createStructField("client:port", DataTypes.StringType, true),
          createStructField("backend:port", DataTypes.StringType, true),
          createStructField("request_processing_time", DataTypes.StringType, true),
          createStructField("backend_processing_time", DataTypes.StringType, true),
          createStructField("response_processing_time", DataTypes.StringType, true),
          createStructField("elb_status_code", DataTypes.StringType, true),
          createStructField("backend_status_code", DataTypes.StringType, true),
          createStructField("received_bytes", DataTypes.IntegerType, true),
          createStructField("sent_bytes", DataTypes.IntegerType, true),
          createStructField("request", DataTypes.StringType, true),
          createStructField("user_agent", DataTypes.StringType, true),
          createStructField("ssl_cipher", DataTypes.StringType, true),
          createStructField("ssl_protocol", DataTypes.StringType, true),
        });
  }

  private StructType getCleanedSchema() {
    return createStructType(
        new StructField[] {
          createStructField("timestamp", DataTypes.TimestampType, true),
          createStructField("elb", DataTypes.StringType, true),
          createStructField("client_ip", DataTypes.StringType, true),
          createStructField("client_port", DataTypes.StringType, true),
          createStructField("backend_ip", DataTypes.StringType, true),
          createStructField("backend_port", DataTypes.StringType, true),
          createStructField("request_processing_time", DataTypes.StringType, true),
          createStructField("backend_processing_time", DataTypes.StringType, true),
          createStructField("response_processing_time", DataTypes.StringType, true),
          createStructField("elb_status_code", DataTypes.StringType, true),
          createStructField("backend_status_code", DataTypes.StringType, true),
          createStructField("received_bytes", DataTypes.IntegerType, true),
          createStructField("sent_bytes", DataTypes.IntegerType, true),
          createStructField("http_method", DataTypes.StringType, true),
          createStructField("url", DataTypes.StringType, true),
          createStructField("http_version", DataTypes.StringType, true),
          createStructField("endpoint", DataTypes.StringType, true),
          createStructField("request_type", DataTypes.StringType, true),
          createStructField("user_agent", DataTypes.StringType, true),
          createStructField("ssl_cipher", DataTypes.StringType, true),
          createStructField("ssl_protocol", DataTypes.StringType, true),
        });
  }

  private StructType getSessionizedSchema() {
    return createStructType(
        new StructField[] {
          createStructField("client_ip", DataTypes.StringType, true),
          createStructField("timestamp", DataTypes.TimestampType, true),
          createStructField("endpoint", DataTypes.StringType, true),
          createStructField("request_type", DataTypes.StringType, true),
          createStructField("session_id", DataTypes.StringType, true)
        });
  }

  private Object[] getDefaultRawData() {
    return new Object[] {
      Timestamp.valueOf("2015-07-22 09:00:28.019143"),
      "marketpalce-shop",
      "123.242.248.130:54635",
      "10.0.6.158:80",
      "0.000022",
      "0.026109",
      "0.00002",
      "200",
      "200",
      0,
      699,
      "\"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\"",
      "\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\"",
      "ECDHE-RSA-AES128-GCM-SHA256",
      "TLSv1.2"
    };
  }

  private Object[] getDefaultCleanedData() {
    return new Object[] {
      Timestamp.valueOf("2015-07-22 09:00:28.019143"),
      "marketpalce-shop",
      "123.242.248.130",
      "54635",
      "10.0.6.158",
      "80",
      "0.000022",
      "0.026109",
      "0.00002",
      "200",
      "200",
      0,
      699,
      "GET",
      "https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null",
      "HTTP/1.1",
      "/shop/authresponse",
      "user",
      "\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\"",
      "ECDHE-RSA-AES128-GCM-SHA256",
      "TLSv1.2"
    };
  }

  private Object[] getDefaultSessionizedData() {
    return new Object[] {
      "123.242.248.130",
      Timestamp.valueOf("2015-07-22 09:00:28.019143"),
      "/shop/authresponse",
      "user",
      "123.242.248.130_0"
    };
  }
}
