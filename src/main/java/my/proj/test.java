package my.proj;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class test {
	public static Double eval(Double a) throws ParseException {
		Double g = (double) 0;
		if(a == null) {
			g = 0.0;
		}else {
			g = a ;
		}
		return g;
	}
	
	public static void main(String[] args) throws ParseException {
		
		System.out.println(eval(null));
	}
}

