package util;

import core.TemperingWoker;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

/**
 * Created by jens on 30.07.16.
 */
public class JobFunction extends RichMapFunction<Integer, String> {

    private RuntimeContext runtimeContext;

    @Override
    public void open(Configuration parameters) throws Exception {
        runtimeContext = getRuntimeContext();
    }

    @Override
    public String map(Integer id) throws Exception {
        return new TemperingWoker(id, runtimeContext, ParamGenerator.getBetaForID(id)).run();
    }
}
