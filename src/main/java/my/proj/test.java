package my.proj;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class test {
	public static Double eval(Double a,Double b) throws ParseException {
		Double g = (double) 0;
		if(b == 0.0) {
			g = 0.0;
		}else {
			g = a/b*100 ;
		}
		return g;
	}
	
	public static void main(String[] args) throws ParseException {
		
		System.out.println(eval(10.0,1.0));
	}
}

