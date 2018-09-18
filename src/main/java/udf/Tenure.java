package udf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Tenure extends UDF {

	public long evaluate(String mbrCreate, String mbrJoin) throws ParseException {

		DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date today = new Date();
		Date currentDate = new java.sql.Date(today.getTime());

		if (mbrCreate == null || mbrCreate.equals("0001-01-01")) {

			if (mbrJoin == null) {
				return -1;
			}

			Date date = format.parse(mbrJoin);
			long diff = currentDate.getTime() - date.getTime();
			int result = (int) ((int)TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS) / 365.25) ;
			return result;

		} else {
			Date date = format.parse(mbrCreate);
			long diff = currentDate.getTime() - date.getTime();
			int result = (int) ((int)TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS) / 365.25) ;
			return result;
		}
	}
	
	public static void main(String[] args) throws ParseException {
		Tenure ob = new Tenure();
		System.out.println(ob.evaluate("1983-08-31", null));
	}
}
