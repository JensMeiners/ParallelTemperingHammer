package functions;


public class SimulatedTempering {


    /**
	 * if ratio is smaller than a U ~ U(0,1) accept the new sample
	 * 
	 * 
	 * maybe the old/current sample is still available?
	 * 
	 */
	public static double getRatio(SpectralSample newSample, SpectralSample currentSample, Double beta) {

        if(testRanges(newSample)==false) return 0.0;

		double newSampleProbability = getPrior(newSample) * tempering(newSample, beta);
		double currentSampleProbability = getPrior(currentSample) * tempering(currentSample, beta);

        if (currentSampleProbability == 0) return 1;
		return newSampleProbability / currentSampleProbability;
	}

    private static boolean testRanges(SpectralSample sample) {
        return sample.getTemperatur() < SpectralConstants.tMax
                && sample.getTemperatur() > SpectralConstants.tMin
                && sample.getFrequency() > 0
                && sample.getFrequency() < SpectralConstants.N;
    }

    public static double tempering(SpectralSample sample, Double beta) {
		return Math.exp(beta * Math.log(getPosterior(sample)));
	}

	private static double getPosterior(SpectralSample sample) {
		double modelError = 0.0;
		for (int i = 0; i < SpectralConstants.data.length; i++) {
			modelError += Math.pow(
					SpectralConstants.data[i] - sample.getTemperatur() * Math.exp(
							(-Math.pow(i - sample.getFrequency(), 2)) / (2 * Math.pow(SpectralConstants.sigmaL, 2))),
					2);
		}
		return Math.pow(2 * Math.PI, -SpectralConstants.N / 2) * Math.pow(SpectralConstants.sigma, -SpectralConstants.N)
				* Math.exp(-(modelError / 2 * Math.pow(SpectralConstants.sigma, 2)));
	}

	private static double getPrior(SpectralSample sample) {
		return getUniformPrior(sample.getFrequency()) * getJeffreysPrior(sample.getTemperatur());
	}

	private static double getJeffreysPrior(double temperatur) {
		return 1 / (temperatur * Math.log(SpectralConstants.tMax / SpectralConstants.tMin));
	}

	private static double getUniformPrior(int frequency) {
		return 1 / (SpectralConstants.tMax - SpectralConstants.tMin);
	}
}
