package kde.regsnap;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class StateMapperFunction implements MapFunction<Tuple3<Long, char[], Long>, Tuple3<Long, char[], Long>>, CheckpointedFunction {

    private final List<String> currentState;
    private transient ListState<String> checkpointedState;
    private final String opName;
    private final int stateSize;

    public StateMapperFunction(String opName, int stateSize){
        this.opName = opName;
        this.stateSize = stateSize;
        this.currentState = new ArrayList<>();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.update(currentState);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        currentState.clear();
        ListStateDescriptor<String> descriptor =
                new ListStateDescriptor<>(
                        opName,
                        TypeInformation.of(new TypeHint<String>() {}));
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        checkpointedState.get().forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                currentState.add(s);
            }
        });
    }

    @Override
    public Tuple3<Long, char[], Long> map(Tuple3<Long, char[], Long> evt) throws Exception {
        currentState.add(String.copyValueOf(evt.f1));
        while (currentState.size() > stateSize*1024){
            currentState.remove(0);
//            Thread.sleep(stateSize/10);
        }
        return evt;
    }
}
