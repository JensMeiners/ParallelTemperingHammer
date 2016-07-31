package util;

import com.google.gson.Gson;
import common.Globals;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by jens on 11.07.16.
 */
public class ElasticLogger {

    static class Hook extends Thread {
        public void run() {
            if (client != null) {
                System.out.println("closing ES client");
                client.close();
            }
        }
    }

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH.mm.ss.SSSZ");


    private static Long runID;
    private Integer id;


    private static TransportClient client;

    public ElasticLogger(Integer id) throws UnknownHostException {
        this.id = id;
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", Globals.indexClusterName).build();

        client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(Globals.indexServer), Globals.indexPort));

        Runtime.getRuntime().addShutdownHook(new Hook());
        runID = System.currentTimeMillis();

    }


    public Log prepare() {
        Log log = new Log().setRunID(runID).setId(id);
        return log;
    }

    public Status prepareStatus() {
        Status log = new Status().setRunID(runID).setId(id);
        return log;
    }

    public ParameterLog prepareParams() {
        ParameterLog log = new ParameterLog().setRunID(runID).setId(id);
        return log;
    }

    private void peristLog(Log log) {
        try {
            IndexRequest indexRequest = new IndexRequest(Globals.indexName, log.getType());
            String json = new Gson().toJson(log);
            //System.out.println("json = " + json);
            indexRequest.source(json);
            client.index(indexRequest).actionGet();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public class Log {

        private Long timestamp;
        private String date;

        public Long runID;
        public Long iteration;
        public Integer id;

        public Log(Long runID,
                   Long iteration,
                   Integer id) {
            super();
            this.runID = runID;
            this.iteration = iteration;
            this.id = id;
        }

        public Log() {
            this.timestamp = System.currentTimeMillis();
            this.date = sdf.format(new Date(timestamp));
        }

        public void persist() {
            peristLog(this);
        }
        public Long getTimestamp() {
            return timestamp;
        }

        public Log setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public String getDate() {
            return date;
        }

        public Log setDate(String date) {
            this.date = date;
            return this;
        }

        public Long getRunID() {
            return runID;
        }

        public Log setRunID(Long runID) {
            this.runID = runID;
            return this;
        }

        public Long getIteration() {
            return iteration;
        }

        public Log setIteration(Long iteration) {
            this.iteration = iteration;
            return this;
        }

        public Integer getId() {
            return id;
        }

        public Log setId(Integer id) {
            this.id = id;
            return this;
        }

        public String getType() {
            return this.getClass().getSimpleName().toLowerCase();
        }

    }

    public class ParameterLog extends Log {

        private Double temperature;
        private Integer frequency;
        private Double beta;

        public ParameterLog(Double temperature, Integer frequency, Double beta) {
            this.temperature = temperature;
            this.frequency = frequency;
            this.beta = beta;
        }

        public ParameterLog() {
            super();
        }

        @Override
        public ParameterLog setRunID(Long runID) {
            this.runID = runID;
            return this;
        }

        @Override
        public ParameterLog setId(Integer id) {
            this.id = id;
            return this;
        }

        @Override
        public ParameterLog setIteration(Long iteration) {
            this.iteration = iteration;
            return this;
        }

        public Double getTemperature() {
            return temperature;
        }

        public ParameterLog setTemperature(Double temperature) {
            this.temperature = temperature;
            return this;
        }

        public Integer getFrequency() {
            return frequency;
        }

        public ParameterLog setFrequency(Integer frequency) {
            this.frequency = frequency;
            return this;
        }

        public Double getBeta() {
            return beta;
        }

        public ParameterLog setBeta(Double beta) {
            this.beta = beta;
            return this;
        }
    }

    public class Status extends Log {

        private Boolean suggestSwap = false;
        private Boolean acceptSwap = false;
        private Boolean acceptPending = false;
        private Boolean lookupSuggest = false;
        private Boolean lookupUpdate = false;
        private Boolean testForSwap = false;

        public Status(Boolean suggestSwap,
                      Boolean acceptSwap,
                      Boolean acceptPending,
                      Boolean lookupSuggest,
                      Boolean lookupUpdate,
                      Boolean testForSwap) {
            this.suggestSwap = suggestSwap;
            this.acceptSwap = acceptSwap;
            this.acceptPending = acceptPending;
            this.lookupSuggest = lookupSuggest;
            this.lookupUpdate = lookupUpdate;
            this.testForSwap = testForSwap;
        }

        public Status() {
            super();
        }

        @Override
        public Status setRunID(Long runID) {
            this.runID = runID;
            return this;
        }

        @Override
        public Status setId(Integer id) {
            this.id = id;
            return this;
        }

        @Override
        public Status setIteration(Long iteration) {
            this.iteration = iteration;
            return this;
        }

        public Boolean getSuggestSwap() {
            return suggestSwap;
        }

        public Status setSuggestSwap(Boolean suggestSwap) {
            this.suggestSwap = suggestSwap;
            return this;
        }

        public Boolean getAcceptSwap() {
            return acceptSwap;
        }

        public Status setAcceptSwap(Boolean acceptSwap) {
            this.acceptSwap = acceptSwap;
            return this;
        }

        public Boolean getLookupSuggest() {
            return lookupSuggest;
        }

        public Status setLookupSuggest(Boolean lookupSuggest) {
            this.lookupSuggest = lookupSuggest;
            return this;
        }

        public Boolean getLookupUpdate() {
            return lookupUpdate;
        }

        public Status setLookupUpdate(Boolean lookupUpdate) {
            this.lookupUpdate = lookupUpdate;
            return this;
        }

        public Boolean getTestForSwap() {
            return testForSwap;
        }

        public Status setTestForSwap(Boolean testForSwap) {
            this.testForSwap = testForSwap;
            return this;
        }

        public Boolean getAcceptPending() {
            return acceptPending;
        }

        public Status setAcceptPending(Boolean acceptPending) {
            this.acceptPending = acceptPending;
            return this;
        }
    }
}
