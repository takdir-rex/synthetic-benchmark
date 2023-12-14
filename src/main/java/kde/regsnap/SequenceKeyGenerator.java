package kde.regsnap;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;

public abstract class SequenceKeyGenerator<T> implements DataGenerator<T> {
    private Long offset = 0L;

    private transient ListState<Long> sequenceNumberState;

    @Override
    public void open(String name, FunctionInitializationContext context, RuntimeContext runtimeContext) throws Exception {
        this.sequenceNumberState = context.getOperatorStateStore().getListState(new ListStateDescriptor(name + "-sequence-number", LongSerializer.INSTANCE));
        offset = 0L;
        if (context.isRestored()) {
            for(Long l : this.sequenceNumberState.get()){
                offset = l;
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.sequenceNumberState.clear();
        this.sequenceNumberState.add(offset);
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    public Long getOffset() {
        return offset++;
    }
}
