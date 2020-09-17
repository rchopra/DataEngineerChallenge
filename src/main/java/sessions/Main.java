package sessions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

public class Main {
  public static void main(String[] args) {
    Dataset<Row> rawDf = load();
    Dataset<Row> cleanedDf = clean(rawDf);
    Dataset<Row> sessionDf = sessionize(cleanedDf, 15);
    Dataset<Row> aggregatedDf = aggregate(sessionDf);
    analyze(aggregatedDf);
  }

  public static Dataset<Row> load() {
    SparkSession spark =
        SparkSession.builder().appName("Sessions").master("local[*]").getOrCreate();
    StructType schema =
        createStructType(
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

    return spark
        .read()
        .format("csv")
        .option("delimiter", " ")
        .schema(schema)
        .load("data/2015_07_22_mktplace_shop_web_log_sample.log.gz");
  }

  public static Dataset<Row> clean(Dataset<Row> df) {
    return df.select(
            col("timestamp"),
            col("elb"),
            // Split "client:port" and "backend:port" fields into ip and port columns
            split(col("client:port"), ":").getItem(0).as("client_ip"),
            split(col("client:port"), ":").getItem(1).as("client_port"),
            split(col("backend:port"), ":").getItem(0).as("backend_ip"),
            split(col("backend:port"), ":").getItem(1).as("backend_port"),
            col("request_processing_time"),
            col("backend_processing_time"),
            col("response_processing_time"),
            col("elb_status_code"),
            col("backend_status_code"),
            col("received_bytes"),
            col("sent_bytes"),
            // Split the request string into its parts
            split(col("request"), " ").getItem(0).as("http_method"),
            split(col("request"), " ").getItem(1).as("url"),
            split(col("request"), " ").getItem(2).as("http_version"),
            // Further parse the request string into an endpoint, which strips the host and query
            // params
            regexp_extract(col("request"), "https://paytm.com:443(.*?)[?\\s]+", 1).as("endpoint"),
            // Categorize requests as "user" or "api" as the usage patterns are quite different
            when(col("request").contains("/api/"), "api").otherwise("user").as("request_type"),
            col("user_agent"),
            col("ssl_cipher"),
            col("ssl_protocol"))
        // Drop these now redundant columns
        .drop("client:port", "backend:port", "request");
  }

  public static Dataset<Row> sessionize(Dataset<Row> df, int sessionLengthMinutes) {
    WindowSpec windowByClientIpOrderByTimestamp =
        Window.partitionBy("client_ip").orderBy(col("timestamp"));

    Dataset<Row> windowedDf =
        df.select(
            col("client_ip"),
            col("timestamp"),
            col("endpoint"),
            col("request_type"),
            // Keep track of the time of previous activity by IP
            coalesce(lag("timestamp", 1).over(windowByClientIpOrderByTimestamp), col("timestamp"))
                .as("previous_timestamp"));

    String inactivityThresholdExpr =
        "previous_timestamp + INTERVAL " + sessionLengthMinutes + " minutes";
    Dataset<Row> withSessions =
        windowedDf.select(
            col("client_ip"),
            col("timestamp"),
            col("endpoint"),
            col("request_type"),
            // Mark a new session when the timestamp of the current event is greater than the
            // previous timestamp + the session inactivity threshold
            when(col("timestamp").geq(expr(inactivityThresholdExpr)), lit(1))
                .otherwise(lit(0))
                .as("session_started"));

    return withSessions.select(
        col("client_ip"),
        col("timestamp"),
        col("endpoint"),
        col("request_type"),
        // Assign a unique identifier to each session. This will facilitate later analysis.
        // Session IDs are labeled by client_ip and a monotonically increasing session number, which
        // is the running sum of the number of sessions for that IP
        concat(
                col("client_ip"),
                lit("_"),
                sum("session_started").over(windowByClientIpOrderByTimestamp))
            .alias("session_id"));
  }

  public static Dataset<Row> aggregate(Dataset<Row> df) {
    return df.groupBy("session_id")
        .agg(
            max("client_ip").as("client_ip"),
            min("timestamp").as("start"),
            max("timestamp").as("end"),
            max("timestamp")
                .cast("long")
                .minus(min("timestamp").cast("long"))
                .as("session_length_seconds"),
            count("endpoint").as("total_visits"),
            countDistinct("endpoint").as("unique_visits"),
            max("request_type").as("request_type"));
  }

  private static void analyze(Dataset<Row> aggregatedDf) {
    Dataset<Row> mostEngaged =
        aggregatedDf
            .sort(desc("session_length_seconds"))
            .select("client_ip", "session_length_seconds")
            .limit(10);
    Row averages =
        aggregatedDf
            .select(
                avg("session_length_seconds").as("avg_session_length_seconds"),
                avg("unique_visits").as("unique_visits_per_session"))
            .collectAsList()
            .get(0);

    System.out.printf(
        "Average session time (seconds): %.2f %n", (double)averages.getAs("avg_session_length_seconds"));
    System.out.printf("Unique visits per session: %.2f %n", (double)averages.getAs("unique_visits_per_session"));
    System.out.println("Most engaged users:");
    mostEngaged.show(false);
  }
}
