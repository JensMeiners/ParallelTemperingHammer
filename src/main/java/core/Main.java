package core;

import common.Globals;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import util.JobFunction;
import util.JobParameters;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by jens on 30.07.16.
 */
public class Main {
    private static int NUM_WORKERS = 1;
    private static int NUM_CORES_PER_WORKER = 4;
    private static int NUM_JOBS = NUM_WORKERS * NUM_CORES_PER_WORKER;

    public static ExecutionEnvironment env;

    public static void main(String[] argv) {
        // init configurations and generate execution environment
        Configuration conf = new Configuration();
        conf.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_CORES_PER_WORKER);
        env = ExecutionEnvironment.createLocalEnvironment(conf);
        env.setParallelism(NUM_JOBS);
        Globals.MAX_ID = NUM_JOBS;
        Globals.MIN_ID = 1;

        // create list of ID's
        List<Integer> ids = IntStream.rangeClosed(Globals.MIN_ID, Globals.MAX_ID).boxed()
                .collect(Collectors.toList());
        System.out.println("ids generated = " + ids);

        env.getConfig().setGlobalJobParameters(new JobParameters(ids));

        DataSource<Integer> data = env.fromCollection(ids);

        // start application master
        Thread master = new Thread(new Master(env));
        master.start();

        // map ID's using custom mapping function.
        // parallel tempering is applied in this mapping phase
        try {
            DataSet map = data.map(new JobFunction()).setParallelism(NUM_JOBS);
            List list = map.collect();
            System.out.println("list = " + list);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
