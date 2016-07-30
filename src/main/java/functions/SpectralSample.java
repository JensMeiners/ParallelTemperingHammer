package functions;

import java.util.Random;

public class SpectralSample {
	
	private double temperatur;
	private int frequency;

	public double getTemperatur() {
		return temperatur;
	}

	public void setTemperatur(double temperatur) {
		this.temperatur = temperatur;
	}

	public int getFrequency() {
		return frequency;
	}

	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}
	
	public SpectralSample(SpectralSample currentSample){
		Random r = new Random();
		//r.nextGaussian()*desiredStandardDeviation+desiredMean;
		temperatur = r.nextGaussian() + currentSample.getTemperatur();
		frequency = (int) Math.round(r.nextGaussian() + currentSample.getFrequency());// this might be a problem???
	}
	
	public SpectralSample(double initTemperatur, int initFrequency){
		temperatur = initTemperatur;
		frequency = initFrequency;
	}

}
