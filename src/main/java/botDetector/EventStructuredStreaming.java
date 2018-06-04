package botDetector;

import com.fasterxml.jackson.databind.ObjectMapper;
import dto.RequestDto;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;


public class EventStructuredStreaming {

    private final static String TOPIC_NAME = "demo-2-distributed";
    private final static String KAFA_HOST = "localhost:9092";
    private final static long CLICK_OVER_VIEW_DIF = 3;

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


        Dataset<Row> windowedCounts = requestDtoDataset.groupBy(
                functions.window(requestDtoDataset.col("unix_time"), "30 seconds", "15 seconds"),
                requestDtoDataset.col("ip")
        ).sum("click", "view");

        Dataset<Row> windowedCounts1 = windowedCounts.select("window.start", "window.end", "ip", "sum(click)", "sum(view)");
//        Dataset<Row> windowedCounts1 = windowedCounts
//                .selectExpr("window.start", "window.end", "ip", "type" ,"count", "CASE WHEN type = 'click' THEN count ELSE 0 END as click_count", "CASE WHEN type = 'view' THEN count ELSE 0 END as view_count");
//
//        Dataset<Row> windowedCounts2 = windowedCounts1.groupBy("start", "ip").sum("click_count", "view_count");

//        StreamingQuery query = windowedCounts1.writeStream()
//                .outputMode("complete")
//                .format("console")
//                .start();

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

            if (views > 0 && clicks/views > CLICK_OVER_VIEW_DIF) {
                System.out.println("Bot detected: " + value.getString(value.fieldIndex("ip")));
            }
        }

        @Override
        public void close(Throwable errorOrNull) {

        }
    }
}
