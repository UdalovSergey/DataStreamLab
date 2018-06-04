package botDetector;

import com.fasterxml.jackson.databind.ObjectMapper;
import dto.RequestDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class EventStream {

    private static final int WINDOW_DURATION = 10000; //ms
    private static final int SLIDE_DURATION = 1000; //ms

    public static void main(String... arg) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "firstGroup");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("demo-2-distributed");
        JavaStreamingContext streamingContext = new JavaStreamingContext("local[*]", "botDetector", new Duration(1000));

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );


        JavaDStream<RequestDto> requests = stream.map(r -> {
            try {
                return new ObjectMapper().readValue(r.value(), RequestDto.class);
            } catch (Exception e) {
                return new RequestDto("error", "error");
            }
        });

        JavaPairDStream<String, String> clickView = requests.mapToPair((PairFunction<RequestDto, String, String>) r ->
                new Tuple2<>(r.getIp(), r.getType()));

        clickView.groupByKeyAndWindow(new Duration(WINDOW_DURATION), new Duration(SLIDE_DURATION)).foreachRDD((rdd, time) -> {
            System.out.println("--- New RDD with " + rdd.partitions().size() + " partitions and " + rdd.count() + " records");
            rdd.foreach(record -> {
                int views = 0, clicks = 0; //TODO: make a trick to solve dividing by zero problem
                for (String type : record._2) {
                    if (type.equals("click")) {
                        clicks++;
                    } else {
                        views++;
                    }
                }
                if (clicks / views > 3 || clicks + views > 1000) {
                    System.out.println("Bot detected : " + record._1);
                }
            });
        });

        streamingContext.checkpoint("checkpoints/");
        streamingContext.start();
        try {
//            streamingContext.awaitTerminationOrTimeout(10000);
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
