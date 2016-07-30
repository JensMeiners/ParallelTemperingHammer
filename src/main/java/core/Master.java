package core;

import org.apache.flink.api.java.ExecutionEnvironment;
import util.JobParameters;

/**
 * Created by jens on 30.07.16.
 */
public class Master implements Runnable {

    private static ExecutionEnvironment env;

    public Master(ExecutionEnvironment env) {
        this.env = env;
    }

    @Override
    public void run() {

        JobParameters params = getParams();
        setParams(params);

    }

    private JobParameters getParams() {
        return (JobParameters) env.getConfig().getGlobalJobParameters();
    }

    private void setParams(JobParameters params) {
        env.getConfig().setGlobalJobParameters(params);
    }
}
