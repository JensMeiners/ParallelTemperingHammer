package util;

import functions.SpectralSample;
import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jens on 30.07.16.
 */
public class JobParameters extends GlobalJobParameters {

    private HashMap<Integer, Params> params = new HashMap<>();
    private HashMap<Integer, Params> suggests = new HashMap<>();
    private HashMap<Integer, Params> pending = new HashMap<>();

    public JobParameters(List<Integer> ids) {
        ParamGenerator paramGenerator = new ParamGenerator();
        ids.forEach(id -> params.put(id, new Params(paramGenerator)));
        System.out.println("params = " + params);
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

    public void suggest(Integer id, SpectralSample sampleparams, Integer owner, Double beta) {
        Params params = new Params(sampleparams.getTemperatur(), sampleparams.getFrequency(), beta);
        params.setOwnerID(owner);
        suggests.put(id, params);
    }

    public void sendParams(Integer ownerid, SpectralSample sampleparams) {
        Params params = new Params(sampleparams.getTemperatur(), sampleparams.getFrequency());
        pending.put(ownerid, params);
    }

    public Params lookupSuggest(Integer id) {
        return suggests.get(id);
    }

    public Params lookupPending(Integer id) {
        return pending.get(id);
    }

    public void delSuggest(Integer id) {
        suggests.remove(id);
    }

    public Params generateParams(Double temperature, Integer frequency, Double beta) {
        return new Params(temperature, frequency, beta);
    }

    public void delPending(Integer id) {
        pending.remove(id);
    }

    public class Params implements Serializable {
        Double temperature;
        Integer frequency;
        Double beta;
        Integer ownerID;

        public Params(ParamGenerator paramGenerator){
            Tuple2<Double, Integer> rand = paramGenerator.rand();
            this.temperature = rand.f0;
            this.frequency = rand.f1;
        }

        public Params(Double temperature, Integer frequency) {
            this.temperature = temperature;
            this.frequency = frequency;
        }

        public Params(Double temperature, Integer frequency, Double beta) {
            this.temperature = temperature;
            this.frequency = frequency;
            this.beta = beta;
        }

        public Double getTemperature() {
            return temperature;
        }

        public void setTemperature(Double temperature) {
            this.temperature = temperature;
        }

        public Integer getFrequency() {
            return frequency;
        }

        public void setFrequency(Integer frequency) {
            this.frequency = frequency;
        }

        public Double getBeta() {
            return beta;
        }

        public void setBeta(Double beta) {
            this.beta = beta;
        }

        public Integer getOwnerID() {
            return ownerID;
        }

        public void setOwnerID(Integer ownerID) {
            this.ownerID = ownerID;
        }

        @Override
        public String toString() {
            return "(T: "+String.format("%.5g", temperature)+", "+"f: "+frequency+")";
        }
    }
}
