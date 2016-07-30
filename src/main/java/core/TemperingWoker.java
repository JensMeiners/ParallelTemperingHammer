package core;

import common.Globals;
import functions.SimulatedTempering;
import functions.SpectralSample;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import util.JobParameters;
import util.JobParameters.Params;
import util.ParamGenerator;

/**
 * Created by jens on 30.07.16.
 */
public class TemperingWoker extends Worker {


    private SpectralSample currentSample;
    private double beta = 1.0; // tempering temperatur
    public static int n = 1000;
    public static int n_lookup = 50;

    public TemperingWoker(Integer id, RuntimeContext context, double beta, double initTemperatur, int initFrequency) {
        super(id, context);
        currentSample = new SpectralSample(initTemperatur, initFrequency);
        this.beta = beta;
    }

    public TemperingWoker(Integer id, RuntimeContext context, double beta) {
        super(id, context);
        Tuple2<Double, Integer> p = ParamGenerator.rand();
        currentSample = new SpectralSample(p.f0, p.f1);
        this.beta = beta;
    }

    @Override
    public String run() {
        System.out.println("start "+"id: "+getId()+" {T=" + currentSample.getTemperatur() + ", f=" + currentSample.getFrequency()+", beta="+beta+"}");
        int t = 0;
        while (true) {
            // obtain new Sample
            SpectralSample newSample = new SpectralSample(currentSample);

            // compute ratio
            double ratio = SimulatedTempering.getRatio(newSample, currentSample, beta);
            // acceptance check
            if (Math.random() <= ratio) {
                currentSample = newSample;
//				System.out.println("========   NEW SAMPLE   ======");
                //System.out.println("id: "+getId()+" {T=" + currentSample.getTemperatur() + ", f=" + currentSample.getFrequency()+"}");
            }

            // TODO logging etc
            // log iteration number, samples, acceptance rate, ...

            t++;
            if(Math.random() <= 1.0/n_lookup) {
                // look for pending accept
                if(!acceptPending()) {
                    // else
                    Params lookup = lookupSuggest();
                    if (lookup != null) {
                        if (testForSwap(lookup)) {
                            acceptSwap(lookup);
                        }
                    }
                }
            }
            // TODO
            if (Math.random() <= 1.0 / n) {
                suggestSwap();
            }

            if (t > 90000) return "";
//			System.out.println("f=" + currentSample.getFrequency() + ",  T=" + currentSample.getTemperatur());
        }
    }

    private boolean acceptPending() {
        JobParameters jobParameters = getParams();
        Params lookup = jobParameters.lookupPending(getId());
        if (lookup == null) return false;

        System.out.println("worker "+getId()+ " accepts swap");
        currentSample = new SpectralSample(lookup.getTemperature(), lookup.getFrequency());
        jobParameters.delPending(getId());
        setParams(jobParameters);
        return true;
    }

    private Params lookupSuggest() {
        JobParameters jobParameters = getParams();
        Params lookup = jobParameters.lookupSuggest(getId());
        if (lookup == null) return null;
        jobParameters.delSuggest(getId());
        setParams(jobParameters);
        return lookup;
    }

    private void acceptSwap(Params lookup) {
        if (sendCurrentSample(lookup.getOwnerID(), currentSample))
            currentSample = new SpectralSample(lookup.getTemperature(), lookup.getFrequency());
    }

    private boolean sendCurrentSample(Integer ownerID, SpectralSample currentSample) {
        JobParameters jobParameters = getParams();
        Params params = jobParameters.lookupPending(ownerID);
        if (params != null) return false;
        jobParameters.sendParams(ownerID, currentSample);
        return true;
    }

    private boolean testForSwap(Params lookup) {
        double lookupBeta = lookup.getBeta();
        SpectralSample lookupSample = new SpectralSample(lookup.getTemperature(), lookup.getFrequency());
        double num = SimulatedTempering.tempering(lookupSample, beta)
                * SimulatedTempering.tempering(currentSample, lookupBeta);
        double denom = SimulatedTempering.tempering(currentSample, beta)
                * SimulatedTempering.tempering(lookupSample, lookupBeta);
        double r = Math.min(1, num / denom);
        return Math.random() <= r;
    }

    private void suggestSwap() {
        Integer adjacentID = getAdjacentWorker();
        JobParameters params = getParams();
        Params adjaparams = params.lookupSuggest(adjacentID);
        if (adjaparams == null)
            params.suggest(adjacentID, currentSample, getId(), beta);
    }

    private Integer getAdjacentWorker() {
        return ParamGenerator.boxedRandInt(Globals.MIN_ID, Globals.MAX_ID);
    }

}
