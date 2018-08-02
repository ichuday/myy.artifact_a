package my.proj;

import java.text.ParseException;

public class test {
	public static Double eval(Double a,Double b,Double c) throws ParseException {
		Double g = (double) 0;
		if(a/b <= c) {
			g = a/b;
		}else {
			g = c ;
		}
		return g;
	}
	
	public static void main(String[] args) throws ParseException {

	}
}

