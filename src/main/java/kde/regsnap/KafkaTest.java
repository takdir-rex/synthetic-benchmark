package kde.regsnap;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;
import java.util.Random;

public class KafkaTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("transaction.timeout.ms", "10000"); // e.g., 2 hours

        DataStream<String> LongStream =
                env.addSource(new SourceFunction<String>() {
                            @Override
                            public void run(SourceContext<String> ctx) throws Exception {
                                for (Integer i = 0; i < 10; i++) {
                                    ctx.collect(i.toString());
                                }
                            }

                            @Override
                            public void cancel() {

                            }
                        })
                        .returns(TypeInformation.of(String.class));

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("100.98.224.25:9092")
                .setKafkaProducerConfig(properties)
                .setTransactionalIdPrefix("my-trx-id-prefix")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("test")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        LongStream.sinkTo(sink);

//        LongStream.print();

        env.execute("Test kafka");
    }

}
