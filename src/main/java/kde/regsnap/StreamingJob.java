/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kde.regsnap;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;


import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool params = ParameterTool.fromArgs(args);
		final String topic = params.get("topic", "test");
		final int sg = params.getInt("sg", 0);
		final long rps = params.getLong("rps", Long.MAX_VALUE);
		final long globalStateSize = params.getLong("statesize", 0);
		final String[] stateRatio = params.get("stateratio", "20,20,20,20,20").split(",");
		final double state1 = Double.valueOf(stateRatio[0]),
				state2 = Double.valueOf(stateRatio[1]),
				state3 = Double.valueOf(stateRatio[2]),
				state4 = Double.valueOf(stateRatio[3]),
				state5 = Double.valueOf(stateRatio[4]);

		Properties producerProps = new Properties();
		producerProps.put("transaction.timeout.ms", 1000*60*5+"");

		DataGeneratorSource<Tuple3<Long, char[], Long>> eventGenerator =  new DataGeneratorSource<>(new SequenceKeyGenerator<Tuple3<Long, char[], Long>>() {

			@Override
			public Tuple3<Long, char[], Long> next() {
				Long offset = getOffset();
				Long time = System.currentTimeMillis();
				String timeStr = jedis.get(offset.toString());
				if(timeStr != null){
					time = Long.valueOf(timeStr);
				} else {
					jedis.set(offset.toString(), time.toString());
				}
				return new Tuple3<>(offset, new char[1024], time);
			}
		}, rps, (Long) null);

		SingleOutputStreamOperator<Tuple3<Long, char[], Long>> source =  env.addSource(eventGenerator).returns(Types.TUPLE(Types.LONG, Types.PRIMITIVE_ARRAY(Types.CHAR), Types.LONG))
				.name("source")
				.setParallelism(1);

		SingleOutputStreamOperator<Tuple3<Long, char[], Long>> op1 = source.partitionCustom(new MyPartitionerOdd(), t -> t.f0)
				.map(new StateMapperFunction("stateful1", (int) (state1/100.0*globalStateSize)))
				.name("op1");

		SingleOutputStreamOperator<Tuple3<Long, char[], Long>> op2 = op1
				.map(new StateMapperFunction("stateful2", (int) (state2/100.0*globalStateSize)))
				.name("op2");

		SingleOutputStreamOperator<Tuple3<Long, char[], Long>> op3 = op2.partitionCustom(new MyPartitionerEven(), t -> t.f0)
				.map(new StateMapperFunction("stateful3", (int) (state3/100.0*globalStateSize)))
				.name("op3");

		SingleOutputStreamOperator<Tuple3<Long, char[], Long>> op4 = op3
				.map(new StateMapperFunction("stateful4", (int) (state4/100.0*globalStateSize)))
				.name("op4");

		SingleOutputStreamOperator<Tuple3<Long, char[], Long>> op5 = op4.partitionCustom(new MyPartitionerOdd(), t -> t.f0)
				.map(new StateMapperFunction("stateful5", (int) (state5/100.0*globalStateSize)))
				.name("op5");

		KafkaSink<Tuple3<Long, char[], Long>> kafkaSink = KafkaSink.<Tuple3<Long, char[], Long>>builder()
				.setBootstrapServers("10.0.1.32:9092")
				.setTransactionalIdPrefix("flink")
				.setKafkaProducerConfig(producerProps)
				.setRecordSerializer(new OutputSchema(topic))
				.build();

		DataStreamSink<Tuple3<Long, char[], Long>> sink = op5.sinkTo(kafkaSink)
				.name("sink");

		if(sg == 1){
			op3.snapshotRegion(1);
			op4.snapshotRegion(1);
			op5.snapshotRegion(1);
			sink.snapshotRegion(1);
		}
		if(sg == 2){
			op3.snapshotRegion(1);
			op4.snapshotRegion(1);
			op5.snapshotRegion(2);
			sink.snapshotRegion(2);
		}

		// execute program
		env.execute("RegSnap Test Job");
//		System.out.println(env.getExecutionPlan());
	}

	public static class MyPartitionerOdd implements Partitioner<Long> {
		@Override
		public int partition(Long key, int numPartitions) {
			return (int) (key % numPartitions);
		}
	}

	public static class MyPartitionerEven implements Partitioner<Long> {
		@Override
		public int partition(Long key, int numPartitions) {
			return (int) ((key/numPartitions) % numPartitions);
		}
	}


}
