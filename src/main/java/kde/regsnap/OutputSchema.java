package kde.regsnap;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class OutputSchema implements KafkaRecordSerializationSchema<Tuple3<Long, char[], Long>> {

    final String TOPIC;

    final DescriptiveStatistics statistics;

    public OutputSchema(final String TOPIC){
        this.TOPIC = TOPIC;
        statistics = new DescriptiveStatistics();
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple3<Long, char[], Long> tuple, KafkaSinkContext context, Long timestamp) {
        Long now = System.currentTimeMillis();
        double latency = (double) (now - tuple.f2)/1_000_000;
        String value = tuple.f0 + "," + latency;
        return new ProducerRecord<>(TOPIC, null, timestamp, null,
                value.getBytes());
    }
}
