package udaf;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class Calculate extends UDAF {

	public static class CalEvaluator implements UDAFEvaluator {

		public static class Result {
			Map<String, ArrayList<Double>> output = new HashMap<String, ArrayList<Double>>();
		}

		private Result result = null;

		public CalEvaluator() {
			super();
			init();
		}

		public void init() {
			result = new Result();

		}

		public boolean iterate(String visit_date, String new_date, String current_ind,
				Map<String, ArrayList<Double>> mp,int flag) throws HiveException, ParseException {

			if (result == null)
				throw new HiveException("Result is not initialized");

			if (visit_date.equals(new_date) && flag == 1) {
				if (current_ind.equals("Y")) {
					for (String key : mp.keySet()) {
						if (!result.output.containsKey(key)) {
							ArrayList<Double> arr = new ArrayList<Double>();
							arr.add(mp.get(key).get(0));
							arr.add(mp.get(key).get(1));
							arr.add(mp.get(key).get(2));
							arr.add(mp.get(key).get(3));
							result.output.put(key, arr);
						} else {
							ArrayList<Double> arr = result.output.get(key);
							for (int i = 0; i < 4; i++) {
								arr.set(i, arr.get(i) + mp.get(key).get(i));
							}
							result.output.put(key, arr);
						}
					}
				}
			}

			if (visit_date.equals(new_date) && flag == 0) {
				if (current_ind.equals("Y")) {
					for (String key : mp.keySet()) {
						if (!result.output.containsKey(key)) {
							ArrayList<Double> arr = new ArrayList<Double>();
							arr.add(mp.get(key).get(0));
							arr.add(mp.get(key).get(1));
							arr.add(mp.get(key).get(2));
							arr.add(mp.get(key).get(3));
							result.output.put(key, arr);
						} else {
							ArrayList<Double> arr = result.output.get(key);
							for (int i = 0; i < 4; i++) {
								arr.set(i, arr.get(i) + mp.get(key).get(i));
							}
							result.output.put(key, arr);
						}
					}
				} else {

					for (String key : mp.keySet()) {
						if (!result.output.containsKey(key)) {
							ArrayList<Double> a = new ArrayList<Double>();
							a.add(-1 * mp.get(key).get(0));
							a.add(-1 * mp.get(key).get(1));
							a.add(-1 * mp.get(key).get(2));
							a.add(-1 * mp.get(key).get(3));
							result.output.put(key, a);
						} else {
							ArrayList<Double> arr = result.output.get(key);
							for (int i = 0; i < 4; i++) {
								arr.set(i, arr.get(i) - mp.get(key).get(i));
							}

							result.output.put(key, arr);
						}
					}
				}
			}
			
			return true;
		}

		public Map<String, ArrayList<Double>> terminate() {
			return result.output;
		}

		public Result terminatePartial() {
			return result;
		}

		public boolean merge(Result another) {
			if (another.output.isEmpty()) {
				return true;
			}
			
			Set<String> set1 = result.output.keySet();
			Set<String> set2 = another.output.keySet();
			Set<String> set = new HashSet<String>();
			set.addAll(set1);
			set.addAll(set2);

			for (String key : set) {
				if (result.output.containsKey(key) && another.output.containsKey(key)) {
					ArrayList<Double> arr1 = result.output.get(key);
					ArrayList<Double> arr2 = another.output.get(key);
					ArrayList<Double> arr3 = new ArrayList<Double>();
					for (int i = 0; i < 4; i++) {
						arr3.add(arr1.get(i) + arr2.get(i));
					}
					result.output.put(key, arr3);
				}

				if (!result.output.containsKey(key)) {
					ArrayList<Double> arr = another.output.get(key);
					ArrayList<Double> arr1 = new ArrayList<Double>();
					for (int i = 0; i < 4; i++)
						arr1.add(arr.get(i));
					result.output.put(key, arr1);
				}
			}

			return true;
		}
	}
}