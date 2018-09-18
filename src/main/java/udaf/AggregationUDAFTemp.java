package udaf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class AggregationUDAFTemp extends UDAF {

	public static class ProdUDAFEvaluator implements UDAFEvaluator {

		public static class Result {
			// List<String> list = new ArrayList<>();
			Map<Integer, Double> spendMap = new HashMap<Integer, Double>();
			Map<Integer, Double> unitsMap = new HashMap<Integer, Double>();
			Map<Integer, List<String>> visitsMap = new HashMap<Integer, List<String>>();
			Map<Integer, Double> returnedItemSpendMap = new HashMap<Integer, Double>();
		}

		private Result result = null;

		public ProdUDAFEvaluator() {
			super();
			init();
		}

		public void init() {
			result = new Result();
		}

		public boolean iterate(Integer key, int returnedItemInd, double retail_price, double unit_qty, int storeNbr,
				long visitNbr) throws HiveException {

			if (result == null)
				throw new HiveException("Result is not initialized");
			
			if(key == null)
				return true;

			if (!result.spendMap.containsKey(key)) {
				result.spendMap.put(key, retail_price);
			} else {
				double spend_sum = result.spendMap.get(key);
				result.spendMap.put(key, spend_sum + retail_price);
			}

			if (!result.unitsMap.containsKey(key)) {
				result.unitsMap.put(key, unit_qty);
			} else {
				double units_sum = result.unitsMap.get(key);
				result.unitsMap.put(key, units_sum + unit_qty);
			}

			String value = storeNbr + "_" + visitNbr;

			if (!result.visitsMap.containsKey(key)) {
				List<String> list = new ArrayList<>();
				list.add(value);
				result.visitsMap.put(key, list);
			} else {

				if (!result.visitsMap.get(key).contains(value)) {
					result.visitsMap.get(key).add(value);
				}
			}

			if (returnedItemInd != 0) {
				if (!result.returnedItemSpendMap.containsKey(key)) {
					result.returnedItemSpendMap.put(key, retail_price);
				} else {
					double spend_sum = result.returnedItemSpendMap.get(key);
					result.returnedItemSpendMap.put(key, spend_sum + retail_price);
				}
			}

			return true;
		}

		public Map<Integer, ArrayList<Double>> terminate() {
			Map<Integer, ArrayList<Double>> temp = new HashMap<Integer, ArrayList<Double>>();
			for (Entry<Integer, Double> entry : result.spendMap.entrySet()) {
				ArrayList<Double> temp1 = new ArrayList<Double>();
				
				temp1.add(entry.getValue());
				temp1.add(result.unitsMap.get(entry.getKey()));
				temp1.add((double) result.visitsMap.get(entry.getKey()).size());
				if (result.returnedItemSpendMap.containsKey(entry.getKey())) {
					temp1.add(result.returnedItemSpendMap.get(entry.getKey()));
				} else {
					temp1.add(0.0);
				}

				temp.put(entry.getKey(), temp1);
			}
			return temp;
		}

		public Result terminatePartial() {
			return result;
		}

		public boolean merge(Result another) {
			if (another == null)
				return true;

			for (Entry<Integer, Double> entry : another.spendMap.entrySet()) {
				if (!result.spendMap.containsKey(entry.getKey())) {
					result.spendMap.put(entry.getKey(), entry.getValue());
				} else {
					double value = result.spendMap.get(entry.getKey());
					result.spendMap.put(entry.getKey(), entry.getValue() + value);
				}
			}

			for (Entry<Integer, Double> entry : another.unitsMap.entrySet()) {
				if (!result.unitsMap.containsKey(entry.getKey())) {
					result.unitsMap.put(entry.getKey(), entry.getValue());
				} else {
					double value = result.unitsMap.get(entry.getKey());
					result.unitsMap.put(entry.getKey(), entry.getValue() + value);
				}
			}

			for (Entry<Integer, List<String>> entry : another.visitsMap.entrySet()) {
				if (!result.visitsMap.containsKey(entry.getKey())) {
					result.visitsMap.put(entry.getKey(), entry.getValue());
				} else {
					List<String> value = result.visitsMap.get(entry.getKey());
					for (int i = 0; i < entry.getValue().size(); i++) {
						if (!(value.contains(entry.getValue().get(i)))) {
							value.add(entry.getValue().get(i));
						}
					}
					result.visitsMap.put(entry.getKey(), value);
				}
			}

			for (Entry<Integer, Double> entry : another.returnedItemSpendMap.entrySet()) {
				if (!result.returnedItemSpendMap.containsKey(entry.getKey())) {
					result.returnedItemSpendMap.put(entry.getKey(), entry.getValue());
				} else {
					double value = result.returnedItemSpendMap.get(entry.getKey());
					result.returnedItemSpendMap.put(entry.getKey(), entry.getValue() + value);
				}
			}

			return true;
		}
	}
}