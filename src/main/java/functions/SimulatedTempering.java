package functions;

public class SimulatedTempering {

	private SpectralSample currentSample;
	private double beta = 1.0; // tempering temperatur

	public SimulatedTempering(double initTemperatur, int initFrequency) {
		currentSample = new SpectralSample(initTemperatur, initFrequency);
	}

	public void run() { // void??
		int t = 0;
		while (true) {
			// obtain new Sample
			SpectralSample newSample = new SpectralSample(currentSample);

			// compute ratio
			double ratio = getRatio(newSample, currentSample);

			// acceptance check
			if (Math.random() <= ratio) {
				currentSample = newSample;
//				System.out.println("========   NEW SAMPLE   ======");
				System.out.println("f=" + currentSample.getFrequency() + ",  T=" + currentSample.getTemperatur());
			}

			// TODO logging etc
			// log iteration number, samples, acceptance rate, ...
			
			t++;
			if(t>1000) return;
			
//			System.out.println("f=" + currentSample.getFrequency() + ",  T=" + currentSample.getTemperatur());
		}
	}

	/**
	 * if ratio is smaller than a U ~ U(0,1) accept the new sample
	 * 
	 * 
	 * maybe the old/current sample is still available?
	 * 
	 */
	public double getRatio(SpectralSample newSample, SpectralSample currentSample) {
		double newSampleProbability = getPrior(newSample) * tempering(newSample);
		double currentSampleProbability = getPrior(currentSample) * tempering(currentSample);

		return newSampleProbability / currentSampleProbability;
	}

	private double tempering(SpectralSample sample) {
		return Math.exp(beta * Math.log(getPosterior(sample)));
	}

	private double getPosterior(SpectralSample sample) {
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

	private double getPrior(SpectralSample sample) {
		return getUniformPrior(sample.getFrequency()) * getJeffreysPrior(sample.getTemperatur());
	}

	private double getJeffreysPrior(double temperatur) {
		return 1 / (temperatur * Math.log(SpectralConstants.tMax / SpectralConstants.tMin));
	}

	private double getUniformPrior(int frequency) {
		return 1 / (SpectralConstants.tMax - SpectralConstants.tMin);
	}
}
