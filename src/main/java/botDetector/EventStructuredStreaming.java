package botDetector;

import com.fasterxml.jackson.databind.ObjectMapper;
import dto.RequestDto;

//import igniteClient.IginteClient;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Date;


public class EventStructuredStreaming {

    private final static String TOPIC_NAME = "demo-2-distributed";
    private final static String KAFA_HOST = "localhost:9092";
    private final static long CLICK_OVER_VIEW_DIF = 3;
    private final static long EVENT_RATE_THRESHOLD = 1000;
   // private final static IginteClient iginteClient = new IginteClient();
    private final static String WINDOW_DURATION = "600 seconds";
    private final static String SLIDE_DURATION = "50 seconds";

    public static void main(String... args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("StructuredBotDetector")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> ds = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFA_HOST)
                .option("subscribe", TOPIC_NAME)
                .option("startingOffsets", "earliest") //TODO:Set proper value
//                .option("endingOffsets", "latest")//TODO:Set proper value
                .option("group.id", "firstGroup")
                .load()
                .selectExpr("CAST(value AS STRING)");

        Dataset<RequestDto> requestDtoDataset = ds.map((MapFunction<Row, RequestDto>) row ->
        {
            try {
                return new ObjectMapper().readValue(row.<String>getAs("value"), RequestDto.class);
            } catch (Exception e) {
                return new RequestDto("error", "error");
            }

        }, Encoders.bean(RequestDto.class));

        Dataset<Row> windowedCounts = requestDtoDataset
              //  .withWatermark("unix_time", "15 seconds")
                .groupBy(
                        functions.window(requestDtoDataset.col("unix_time"), WINDOW_DURATION, SLIDE_DURATION),
                        requestDtoDataset.col("ip")
                ).sum("click", "view");

        Dataset<Row> windowedCounts1 = windowedCounts.select("window.start", "window.end", "ip", "sum(click)", "sum(view)");

        StreamingQuery query = windowedCounts1.writeStream()
                .outputMode("complete")
                .foreach(new IgniteWriter())
                .start();
        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }

    private static boolean isRobot(long clicks, long views) {
        return views > 0 && clicks / views >= CLICK_OVER_VIEW_DIF || clicks + views > EVENT_RATE_THRESHOLD;
    }

    static class IgniteWriter extends ForeachWriter<Row> {

        @Override
        public boolean open(long partitionId, long version) {
            return true;
        }

        //TODO: implement a result saving logic (Ignite persistence)
        @Override
        public void process(Row value) {
            long clicks = value.getLong(value.fieldIndex("sum(click)"));
            long views = value.getLong(value.fieldIndex("sum(view)"));
            views = views == 0 && clicks >= CLICK_OVER_VIEW_DIF ? 1 : views;

            if (isRobot(clicks, views)) {
                System.out.println("Bot detected: " + value.getString(value.fieldIndex("ip")) + " clicks:" + clicks + " views:" + views);
               // iginteClient.putBot(value.getString(value.fieldIndex("ip")));
            }
        }

        @Override
        public void close(Throwable errorOrNull) {

        }
    }
}
