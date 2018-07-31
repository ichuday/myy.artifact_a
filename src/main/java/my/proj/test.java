package my.proj;

import java.text.ParseException;

public class test {
	public static Double eval(Double a) throws ParseException {

		Double g = (double) 0;
		if (a!=null) {
			g = a;
		} else {
			g = 0.0;
		}
		return g;
	}

	public static void main(String[] args) throws ParseException {
		Double b=null;
		System.out.println(eval(b));
	}

}
