package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Distance extends UDF {

	public double evaluate(Double clubLatitude, Double clubLongitude, Double hhLatitude, Double hhLongitude) {

		if (clubLatitude == null || clubLongitude == null || hhLatitude == null || hhLongitude == null
				|| clubLatitude == 0.0 || clubLongitude == 0.0 || hhLatitude == 0.0 || hhLongitude == 0.0) {
			return -1.0;
		} else {
			double distance;
			distance = 3963.0 * Math.acos(Math.sin(clubLatitude / 57.2958) * Math.sin(hhLatitude / 57.2958)
					+ Math.cos(clubLatitude / 57.2958) * Math.cos(hhLatitude / 57.2958) * 
					Math.cos((hhLongitude / 57.2958) - (clubLongitude / 57.2958)));
			
			return distance;
		}
	}
}
