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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;

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
public class StreamingJobBak {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool params = ParameterTool.fromArgs(args);
		final int sg = params.getInt("sg", 0);
		final long rps = params.getLong("rps", Long.MAX_VALUE);
		final long globalStateSize = params.getLong("statesize", 0);
		final String[] stateRatio = params.get("stateratio", "20,20,20,20,20").split(",");
		final double state1 = Double.valueOf(stateRatio[0]),
				state2 = Double.valueOf(stateRatio[1]),
				state3 = Double.valueOf(stateRatio[2]),
				state4 = Double.valueOf(stateRatio[3]),
				state5 = Double.valueOf(stateRatio[4]);

		DataGeneratorSource<Tuple2<Integer, char[]>> eventGenerator =  new DataGeneratorSource<>(new DataGenerator<Tuple2<Integer, char[]>>() {

			private int index;

			@Override
			public void open(String s, FunctionInitializationContext functionInitializationContext, RuntimeContext runtimeContext) throws Exception {
				index = runtimeContext.getIndexOfThisSubtask();
			}

			@Override
			public boolean hasNext() {
				return true;
			}

			@Override
			public Tuple2<Integer, char[]> next() {
				return new Tuple2<>(index, new char[1024]);
			}
		}, rps, (Long) null);

		SingleOutputStreamOperator<Tuple2<Integer, char[]>> source =  env.addSource(eventGenerator).returns(Types.TUPLE(Types.INT, Types.PRIMITIVE_ARRAY(Types.CHAR)))
				.slotSharingGroup("source");

		SingleOutputStreamOperator<Tuple2<Integer, char[]>> op1 = source.keyBy(t -> t.f0)
				.map(new StateMapper("stateful1", (int) (state1/100.0*globalStateSize)))
				.slotSharingGroup("1");

//		DataStream<Integer> op2 = op1.flatMap(new FlatMapFunction<Integer, Integer>() {
//			@Override
//			public void flatMap(Integer in, Collector<Integer> out) throws Exception {
//				out.collect(in);
//			}
//		});

		SingleOutputStreamOperator<Tuple2<Integer, char[]>> op2 = op1.keyBy(t -> t.f0)
				.map(new StateMapper("stateful2", (int) (state2/100.0*globalStateSize)))
				.slotSharingGroup("2");

		SingleOutputStreamOperator<Tuple2<Integer, char[]>> op3 = op2.keyBy(t -> t.f0)
				.map(new StateMapper("stateful3", (int) (state3/100.0*globalStateSize)))
				.slotSharingGroup("3");

//		SingleOutputStreamOperator<Integer> op4 = op3.flatMap(new FlatMapFunction<Integer, Integer>() {
//					@Override
//					public void flatMap(Integer in, Collector<Integer> out) throws Exception {
//						out.collect(in);
//					}
//				});
//				.slotSharingGroup("1");

		SingleOutputStreamOperator<Tuple2<Integer, char[]>> op4 = op3.keyBy(t -> t.f0)
				.map(new StateMapper("stateful4", (int) (state4/100.0*globalStateSize)))
				.slotSharingGroup("4");

		SingleOutputStreamOperator<Tuple2<Integer, char[]>> op5 = op4.keyBy(t -> t.f0)
				.map(new StateMapper("stateful5", (int) (state5/100.0*globalStateSize)))
				.slotSharingGroup("5");

		DataStreamSink<Tuple2<Integer, char[]>> sink = op5.addSink(new DiscardingSink<>())
				.slotSharingGroup("sink");

//		if(sg == 1){
//			op3.snapshotRegion(1);
//			op4.snapshotRegion(1);
//			op5.snapshotRegion(1);
//			sink.snapshotRegion(1);
//		}
//		if(sg == 2){
//			op3.snapshotRegion(1);
//			op4.snapshotRegion(1);
//			op5.snapshotRegion(2);
//			sink.snapshotRegion(2);
//		}

		// execute program
		env.execute("RegSnap Test Job");
//		System.out.println(env.getExecutionPlan());
	}

}
