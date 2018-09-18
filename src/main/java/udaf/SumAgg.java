package udaf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

public class SumAgg extends UDAF {

	public static class SumEvaluator implements UDAFEvaluator{

		public static class Result {
			Map<String, ArrayList<Double>> output = new HashMap<String, ArrayList<Double>>();
		}

		private Result result = null;

		public SumEvaluator() {
			super();
			init();
		}
		
		public void init() {
			result = new Result();
		}
		
		public boolean iterate(Map<String , ArrayList<Double>> map,String visit_date,String new_date,String current_ind,int flag) throws HiveException, ParseException {

			if (result == null)
				throw new HiveException("Result is not initialized");
			
			if(!current_ind.equals("Y"))
				return true;
			
			if(flag == 1){
				for (String key : map.keySet()) {
					ArrayList<Double> arr = null;
					if(result.output.get(key) == null){
						arr = new ArrayList<Double>();
						arr.add(map.get(key).get(0));
						arr.add(map.get(key).get(1));
						arr.add(map.get(key).get(2));
						arr.add(map.get(key).get(3));
						arr.add(map.get(key).get(4));
						arr.add(map.get(key).get(5));
						arr.add(map.get(key).get(6));
						arr.add(map.get(key).get(7));
						result.output.put(key , arr);
					}
					else{
						arr = result.output.get(key);
						arr.set(0, arr.get(0) + map.get(key).get(0));
						arr.set(1, arr.get(1) + map.get(key).get(1));
						arr.set(2, arr.get(2) + map.get(key).get(2));
						arr.set(3, arr.get(3) + map.get(key).get(3));
						arr.set(4, arr.get(4) + map.get(key).get(4));
						arr.set(5, arr.get(5) + map.get(key).get(5));
						arr.set(6, arr.get(6) + map.get(key).get(6));
						arr.set(7, arr.get(7) + map.get(key).get(7));
					}
				}
				return true;
			}
			
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			Date d1 = format.parse(visit_date);
			Date d2 = format.parse(new_date);
			long diff = d2.getTime() - d1.getTime();
			long days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
			
			if(days < 364){
				for (String key : map.keySet()) {
					ArrayList<Double> arr = null;
					if(result.output.get(key) == null){
						arr = new ArrayList<Double>();
						arr.add(map.get(key).get(0));
						arr.add(map.get(key).get(1));
						arr.add(map.get(key).get(2));
						arr.add(0.0);
						arr.add(0.0);
						arr.add(0.0);
						arr.add(map.get(key).get(3));
						arr.add(0.0);
					}
					else{
						arr = result.output.get(key);
						arr.set(0, arr.get(0) + map.get(key).get(0));
						arr.set(1, arr.get(1) + map.get(key).get(1));
						arr.set(2, arr.get(2) + map.get(key).get(2));
						arr.set(3, arr.get(3) + 0.0);
						arr.set(4, arr.get(4) + 0.0);
						arr.set(5, arr.get(5) + 0.0);
						arr.set(6, arr.get(6) + map.get(key).get(3));
						arr.set(7, arr.get(7) + 0.0);
					}
					
					result.output.put(key , arr);
				}
			}
			else{
				for (String key : map.keySet()) {
					ArrayList<Double> arr = null;
					if(result.output.get(key) == null){
						arr = new ArrayList<Double>();
						arr.add(0.0);
						arr.add(0.0);
						arr.add(0.0);
						arr.add(map.get(key).get(0));
						arr.add(map.get(key).get(1));
						arr.add(map.get(key).get(2));
						arr.add(0.0);
						arr.add(map.get(key).get(3));
					}else{
						arr = result.output.get(key);
						arr.set(0, arr.get(0) + 0.0);
						arr.set(1, arr.get(1) + 0.0);
						arr.set(2, arr.get(2) + 0.0);
						arr.set(3, arr.get(3) + map.get(key).get(0));
						arr.set(4, arr.get(4) + map.get(key).get(1));
						arr.set(5, arr.get(5) + map.get(key).get(2));
						arr.set(6, arr.get(6) + 0.0);
						arr.set(7, arr.get(7) + map.get(key).get(3));
					}
					
					result.output.put(key, arr);
				}
			}
			
			return true;
		}
		
		public Map<String , ArrayList<Double>> terminate() {
			return result.output;
		}
		
		public Result terminatePartial() {
			return result;
		}
		
		public boolean merge(Result another) {
			if (another == null || another.output.isEmpty())
				return true;
			
			Set<String> set1 = result.output.keySet();
			Set<String> set2 = another.output.keySet();
			Set<String> set = new HashSet<String>();
			set.addAll(set1);
			set.addAll(set2);
			
			for (String key : set) {
				if(result.output.containsKey(key) && another.output.containsKey(key)){
					ArrayList<Double> arr1 = result.output.get(key);
					ArrayList<Double> arr2 = another.output.get(key);
					for(int i = 0 ; i < 8 ; i++){
						arr1.set(i, arr1.get(i) + arr2.get(i));
					}
					result.output.put(key, arr1);
				}
				
				if(!result.output.containsKey(key)){
					ArrayList<Double> arr = another.output.get(key);
					ArrayList<Double> ar = new ArrayList<Double>();
					for(int i = 0 ; i < 8 ; i++){
						ar.add(arr.get(i));
					}
					result.output.put(key, ar);
				}
			}
			
			return true;
		}
	}
}