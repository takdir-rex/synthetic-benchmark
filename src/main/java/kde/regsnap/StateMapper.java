package kde.regsnap;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class StateMapper extends RichMapFunction<Tuple2<Integer, char[]>, Tuple2<Integer, char[]>> {

    private ValueState<List<String>> currentState;
    private final String opName;
    private final int stateSize;

    public StateMapper(String opName, int stateSize){
        this.opName = opName;
        this.stateSize = stateSize;
    }

    @Override
    public void open(Configuration conf) {
        // get access to the state object
        currentState =
                getRuntimeContext().getState(new ValueStateDescriptor<>(opName,
                        TypeInformation.of(new TypeHint<List<String>>() {})));
    }

    @Override
    public Tuple2<Integer, char[]> map(Tuple2<Integer, char[]> evt) throws Exception {
        List<String> state = currentState.value();
        if(state == null){
            state = new ArrayList<>();
        }
        state.add(String.copyValueOf(evt.f1));
        while (state.size() > stateSize*1024){
            state.remove(0);
//            Thread.sleep(stateSize/10);
        }
        currentState.update(state);
        return evt;
    }
}