package udaf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import udaf.Calculate.CalEvaluator.Result;

public class CalculateTemp extends UDAF {

//	public static void main(String[] args) {
//
//		Map<Integer, ArrayList<Double>> output2 = new HashMap<Integer, ArrayList<Double>>();
//		ArrayList<Double> l2 = new ArrayList<Double>(Arrays.asList(new Double[] { -12d, -10d, -11d, -2d }));
//		output2.put(1, l2);
//
//		CalEvaluator eval = new CalEvaluator();
//		eval.merge(new Result(output2));
//
//		Map<Integer, ArrayList<Double>> output3 = new HashMap<Integer, ArrayList<Double>>();
//		ArrayList<Double> l3 = new ArrayList<Double>(Arrays.asList(new Double[] { -121d, -101d, -111d, -2d }));
//		output3.put(1, l3);
//		
//		eval.merge(new Result(output3));
//
//		Map<Integer, ArrayList<Double>> output4 = new HashMap<Integer, ArrayList<Double>>();
//		ArrayList<Double> l4 = new ArrayList<Double>(Arrays.asList(new Double[] { 112d, 110d, -11d, -2d }));
//		output4.put(1, l4);
//		
//		eval.merge(new Result(output4));
//
//	}

	public static class CalEvaluator implements UDAFEvaluator {

		public static class Result {
			Map<Integer, ArrayList<Double>> output = new HashMap<Integer, ArrayList<Double>>();

//			public Result() {
//
//			}
//
//			public Result(Map<Integer, ArrayList<Double>> output1) {
//				output = output1;
//			}
		}

		private Result result = null;

		public CalEvaluator() {
			
			super();
			System.out.println("------------------------Constructor");
			init();
		}

		public void init() {
			System.out.println("------------------------Init");
//			Map<Integer, ArrayList<Double>> output1 = new HashMap<Integer, ArrayList<Double>>();
//			ArrayList<Double> l1 =  new ArrayList<Double>( Arrays.asList(new Double[] { 23d, 21d, 12d, 1d }));
//			output1.put(1, l1);
			result = new Result();
//			result.output = output1;

		}

		public boolean iterate(String visit_date, String new_date, String current_ind,
				Map<Integer, ArrayList<Double>> mp) throws HiveException, ParseException {

			if (result == null)
				throw new HiveException("Result is not initialized");

			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Date today = new Date();
			Date currentDate = new java.sql.Date(today.getTime());

			if (visit_date.equals(new_date) && new_date.equals(df.format(currentDate))) {
				if (current_ind.equals("Y")) {
					for (Integer key : mp.keySet()) {
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

			if (visit_date.equals(new_date)) {
				if (current_ind.equals("Y")) {
					for (Integer key : mp.keySet()) {
						if (!result.output.containsKey(key)) {
							ArrayList<Double> arr = new ArrayList<Double>();
							arr.add(mp.get(key).get(0));
							arr.add(mp.get(key).get(1));
							arr.add(mp.get(key).get(2));
							arr.add(mp.get(key).get(3));
							System.out.println("--------Y NOKEY----------");
							result.output.put(key, arr);
							for (Integer str : result.output.keySet()) {
								ArrayList<Double> x = result.output.get(str);
								System.out.println("KEY IS - > " + str);
								for (int i = 0; i < 4; i++)
									System.out.println(x.get(i));
							}
						} else {
							ArrayList<Double> arr = result.output.get(key);
							for (int i = 0; i < 4; i++) {
								arr.set(i, arr.get(i) + mp.get(key).get(i));
							}
							System.out.println("--------Y KEY----------");
							result.output.put(key, arr);
							for (Integer str : result.output.keySet()) {
								ArrayList<Double> x = result.output.get(str);
								System.out.println("KEY IS - > " + str);
								for (int i = 0; i < 4; i++)
									System.out.println(x.get(i));
							}
						}
					}
				} else {

					for (Integer key : mp.keySet()) {
						if (!result.output.containsKey(key)) {
							ArrayList<Double> a = new ArrayList<Double>();
							a.add(-1 * mp.get(key).get(0));
							a.add(-1 * mp.get(key).get(1));
							a.add(-1 * mp.get(key).get(2));
							a.add(-1 * mp.get(key).get(3));
							System.out.println("--------N NOKEY----------");
							result.output.put(key, a);
							for (Integer str : result.output.keySet()) {
								ArrayList<Double> x = result.output.get(str);
								System.out.println("KEY IS - > " + str);
								for (int i = 0; i < 4; i++)
									System.out.println(x.get(i));
							}
						} else {
							ArrayList<Double> arr = result.output.get(key);
							for (int i = 0; i < 4; i++) {
								arr.set(i, arr.get(i) - mp.get(key).get(i));
							}
							System.out.println("--------N KEY----------");

							result.output.put(key, arr);
							for (Integer str : result.output.keySet()) {
								ArrayList<Double> x = result.output.get(str);
								System.out.println("KEY IS - > " + str);
								for (int i = 0; i < 4; i++)
									System.out.println(x.get(i));
							}
						}
					}
				}
			}

			return true;
		}

		public Map<Integer, ArrayList<Double>> terminate() {
			System.out.println("--------TERMINATE---------");
			for (Integer str : result.output.keySet()) {
				ArrayList<Double> x = result.output.get(str);
				System.out.println("KEY IS - > " + str);
				for (int i = 0; i < 4; i++)
					System.out.println(x.get(i));
			}
			return result.output;
		}

		public Result terminatePartial() {
			System.out.println("--------TERMINATE-PARTIAL---------");
			for (Integer str : result.output.keySet()) {
				ArrayList<Double> x = result.output.get(str);
				System.out.println("KEY IS - > " + str);
				for (int i = 0; i < 4; i++)
					System.out.println(x.get(i));
			}
			return result;
		}

		public boolean merge(Result another) {
			if (another.output.isEmpty()) {
			System.out.println("-----------------------------Another is null!");
				return true;
			}

			
			Map<Integer, ArrayList<Double>> output2 = new HashMap<Integer, ArrayList<Double>>();
			
			Set<Integer> set1 = result.output.keySet();
			Set<Integer> set2 = another.output.keySet();
			Set<Integer> set = new HashSet<Integer>();
			set.addAll(set1);
			set.addAll(set2);

			System.out.println("--------MERGE-ANOTHER---------");
			for (Integer str : another.output.keySet()) {
				ArrayList<Double> x = another.output.get(str);
				System.out.println("KEY IS - > " + str);
				for (int i = 0; i < 4; i++)
					System.out.println(x.get(i));
			}

			System.out.println("--------MERGE--RESULT--------");
			for (Integer str : result.output.keySet()) {

				ArrayList<Double> x = result.output.get(str);
				System.out.println("KEY IS - > " + str);
				for (int i = 0; i < 4; i++)
					System.out.println(x.get(i));
			}

			for (Integer key : set) {
				if (result.output.containsKey(key) && another.output.containsKey(key)) {
					ArrayList<Double> arr1 = result.output.get(key);
					ArrayList<Double> arr2 = another.output.get(key);
					ArrayList<Double> arr3 = new ArrayList<Double>();
					for (int i = 0; i < 4; i++) {
//						arr1.set(i, arr1.get(i) + arr2.get(i));
						arr3.add(arr1.get(i) + arr2.get(i));
					}
//					result.output.put(key, arr1);
					output2.put(key, arr3);
				}

				if (!result.output.containsKey(key)) {
					ArrayList<Double> arr = another.output.get(key);
					ArrayList<Double> arr1 = new ArrayList<Double>();
					for (int i = 0; i < 4; i++)
						arr1.add(arr.get(i));
//					result.output.put(key, arr1);
					output2.put(key, arr1);
				}
			}

			System.out.println("------AFTER--MERGE--RESULT--------");
			for (Integer str : output2.keySet()) {
//				for (Integer str : result.output.keySet()) {
//				ArrayList<Double> x = result.output.get(str);
				ArrayList<Double> x = output2.get(str);
				System.out.println("KEY IS - > " + str);
				for (int i = 0; i < 4; i++)
					System.out.println(x.get(i));
			}

			result.output = output2;
			return true;
		}
	}
}