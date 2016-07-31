package core;

import common.Globals;
import functions.SimulatedTempering;
import functions.SpectralSample;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import util.ElasticLogger;
import util.JobParameters;
import util.JobParameters.Params;
import util.ParamGenerator;

import java.net.UnknownHostException;

/**
 * Created by jens on 30.07.16.
 */
public class TemperingWoker extends Worker {


    private SpectralSample currentSample;
    private double beta = 1.0; // tempering temperatur
    public static int n = 100;
    public static int n_lookup = 50;
    private long n_persist_params = 100L;

    private ParamGenerator paramGenerator;

    private ElasticLogger logger;


    @Deprecated
    public TemperingWoker(Integer id, RuntimeContext context, double beta, double initTemperatur, int initFrequency) throws UnknownHostException {
        super(id, context);
        currentSample = new SpectralSample(initTemperatur, initFrequency);
        this.beta = beta;
        this.logger = new ElasticLogger(id);
    }

    public TemperingWoker(Integer id, RuntimeContext context) throws UnknownHostException {
        super(id, context);
        paramGenerator = new ParamGenerator();
        this.beta = paramGenerator.getBetaForID(id);
        Tuple2<Double, Integer> p = paramGenerator.rand();
        currentSample = new SpectralSample(p.f0, p.f1);
        this.logger = new ElasticLogger(id);
    }

    @Override
    public String run() {
        System.out.println("start "+"id: "+getId()+" {T=" + currentSample.getTemperatur() + ", f=" + currentSample.getFrequency()+", beta="+beta+"}");
        long t = 0;
        while (true) {
            // obtain new Sample
            SpectralSample newSample = new SpectralSample(currentSample);

            // compute ratio
            double ratio = SimulatedTempering.getRatio(newSample, currentSample, beta);
            // acceptance check
            if (Math.random() <= ratio) {
                currentSample = newSample;
                if (t % n_persist_params == 0)
                logger.prepareParams()
                        .setTemperature(currentSample.getTemperatur()*1000)
                        .setFrequency(currentSample.getFrequency())
                        .setIteration(t)
                        .setBeta(beta).persist();
            }

            // TODO logging etc
            // log iteration number, samples, acceptance rate, ...

            if(Math.random() <= 1.0/n_lookup) {
                // look for pending accept
                logger.prepareStatus().setLookupUpdate(true).persist();
                if(!acceptPending()) {
                    // else
                    logger.prepareStatus().setLookupSuggest(true).persist();
                    Params lookup = lookupSuggest();
                    if (lookup != null) {
                        logger.prepareStatus().setTestForSwap(true).persist();
                        if (testForSwap(lookup)) {
                            logger.prepareStatus().setAcceptSwap(true).persist();
                            acceptSwap(lookup);
                        }
                    }
                } else {
                    logger.prepareStatus().setAcceptPending(true).persist();
                }
            }

            if (Math.random() <= 1.0 / n) {
                logger.prepareStatus().setSuggestSwap(true)
                        .setIteration(t).persist();
                suggestSwap();
            }

            t++;
            if (t > 40000) return "id: "+getId()
                    +" {T="+currentSample.getTemperatur()
                    +", f="+currentSample.getFrequency()
                    +", beta="+beta+"}";
        }
    }

    private boolean acceptPending() {
        JobParameters jobParameters = getParams();
        Params lookup = jobParameters.lookupPending(getId());
        if (lookup == null) return false;

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
        return paramGenerator.boxedRandInt(Globals.MIN_ID, Globals.MAX_ID);
    }

}
