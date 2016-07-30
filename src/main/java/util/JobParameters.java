package util;

import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by jens on 30.07.16.
 */
public class JobParameters extends GlobalJobParameters {

    private HashMap<Integer, Params> params = new HashMap<>();

    public JobParameters(List<Integer> ids) {
        ids.forEach(id -> params.put(id, new Params()));
    }

    @Override
    public Map<String, String> toMap() {
        HashMap<String, String> map = new HashMap();
        params.entrySet().forEach(entry -> map.put(
                Integer.toString(entry.getKey()),
                entry.getValue().toString()));
        return map;
    }

    public Double getTemp(Integer id) {
        return params.get(id).temperature;
    }

    public Integer getFreq(Integer id) {
        return params.get(id).frequency;
    }

    public String getString(Integer id) {
        return params.get(id).toString();
    }

    private class Params implements Serializable {
        Double temperature;
        Integer frequency;

        Random r = new Random();

        public Params(){
            this.temperature = r.nextDouble();
            this.frequency = r.nextInt();
        }

        @Override
        public String toString() {
            return "(T: "+String.format("%.5g", temperature)+", "+"F: "+frequency+")";
        }
    }
}
