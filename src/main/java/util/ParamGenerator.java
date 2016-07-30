package util;

import common.Globals;
import functions.SpectralConstants;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

/**
 * Created by jens on 30.07.16.
 */
public class ParamGenerator {

    private static Random r = new Random();
    private static HashMap<Integer, Double> betas;

    public static Tuple2<Double, Integer> rand() {
        Tuple2<Double, Integer> t = new Tuple2<>();
        // temperature
        t.f0 = r.nextDouble() * SpectralConstants.tMax + SpectralConstants.tMin;
        // frequency
        t.f1 = r.nextInt(SpectralConstants.N + 1);
        return t;
    }

    public static Integer boxedRandInt(Integer min, Integer max) {
        return r.nextInt(max-min+1)+min;
    }

    private static void generateBetas(Integer numIDs) {
        double d = 1.0 / numIDs;
        List<Integer> ids = IntStream.rangeClosed(1, numIDs).boxed()
                .collect(Collectors.toList());
        betas = new HashMap<>();
        ids.forEach(i -> betas.put(i, i*d));
    }

    public static Double getBetaForID(Integer id) {
        if (betas == null) generateBetas(Globals.MAX_ID - Globals.MIN_ID + 1);
        return betas.get(id - Globals.MIN_ID + 1);
    }
}
