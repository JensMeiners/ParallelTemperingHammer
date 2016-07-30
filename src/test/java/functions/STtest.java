package functions;

import java.util.Random;

public class STtest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SimulatedTempering st = new SimulatedTempering(5, 30);
		st.run();
		
//		
//		Random r = new Random();
//		double test = 20;
//		for (int i = 0; i < 1000; i++) {
//			test =  (r.nextGaussian() + test);
//			System.out.println( (int) Math.round(test));
//		}

	}

}
