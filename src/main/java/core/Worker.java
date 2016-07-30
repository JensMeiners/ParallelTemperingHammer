package core;

import org.apache.flink.api.common.functions.RuntimeContext;
import util.JobParameters;

/**
 * Created by jens on 30.07.16.
 */
public class Worker {

    private RuntimeContext context;
    private Integer id;

    public Worker(Integer id, RuntimeContext context) {
        this.id = id;
        this.context = context;
    }

    public String work() {
        JobParameters params = getParams();
        return params.getString(id);
    }

    private JobParameters getParams() {
        return (JobParameters) context.getExecutionConfig().getGlobalJobParameters();
    }

    private void setParams(JobParameters params) {
        context.getExecutionConfig().setGlobalJobParameters(params);
    }
}
