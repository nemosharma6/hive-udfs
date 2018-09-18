package udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import Category.*;

public class AggregationUDAF extends UDAF {

	public static class SUMUDAFEvaluator implements UDAFEvaluator {

		public static class Result {
			double sum = 0.0;
		}

		private Result result = null;

		public SUMUDAFEvaluator() {
			super();
			init();
		}

		public void init() {
			result = new Result();
		}

		public boolean iterate(String category , String orderLineTypeCode, String orderLineTypeDesc, String oderLineStatusCode,
				int deptNbr, String returnedItemInd, String retailCode , String siteTypeDesc , double value) throws HiveException {

			if (result == null)
				throw new HiveException("Result is not initialized");
			
			if(!returnedItemInd.equals("0"))
				return true;
			
			if (category.equals("TOTAL_MIX")) {
				TOTAL_MIX obj = new TOTAL_MIX(deptNbr);
				if (obj.check()) {
					result.sum = result.sum + value;
				}
			}

			if (category.equals("AUCTION")) {
				AUCTION obj = new AUCTION(orderLineTypeDesc, retailCode, deptNbr);
				if (obj.check()) {
					result.sum = result.sum + value;
				}
			}

			if (category.equals("DTH")) {
				DTH obj = new DTH(deptNbr, orderLineTypeCode, siteTypeDesc, oderLineStatusCode , retailCode);
				if (obj.check()) {
					result.sum = result.sum + value;
				}
			}
			
			if (category.equals("CPU")) {
				CPU obj = new CPU(deptNbr, orderLineTypeCode, oderLineStatusCode);
				if (obj.check()) {
					result.sum = result.sum + value;
				}
			}
			
			if (category.equals("FUEL")) {
				FUEL obj = new FUEL(deptNbr , retailCode);
				if (obj.check()) {
					result.sum = result.sum + value;
				}
			}
			
			if (category.equals("INCLUB")) {
				INCLUB obj = new INCLUB(retailCode, deptNbr);
				if (obj.check()) {
					result.sum = result.sum + value;
				}
			}
			
			if (category.equals("NONMERCH")) {
				NONMERCH obj = new NONMERCH(deptNbr, retailCode);
				if (obj.check()) {
					result.sum = result.sum + value;
				}
			}
			
			if (category.equals("NONMERCH_ONLINE")) {
				NONMERCH_ONLINE obj = new NONMERCH_ONLINE(deptNbr, retailCode);
				if (obj.check()) {
					result.sum = result.sum + value;
				}
			}
			
			if (category.equals("SOT")) {
				SOT obj = new SOT(deptNbr, orderLineTypeCode , retailCode);
				if (obj.check()) {
					result.sum = result.sum + value;
				}
			}

			return true;
		}

		public double terminate() {
			return result.sum;
		}

		public Result terminatePartial() {
			return result;
		}

		public boolean merge(Result another) {
			if (another == null)
				return true;

			result.sum += another.sum;
			return true;
		}
	}

	public static class DistinctUDAFEvaluator implements UDAFEvaluator {

		public static class Result {
			List<String> list = new ArrayList<>();
		}

		private Result result = null;

		public DistinctUDAFEvaluator() {
			super();
			init();
		}

		public void init() {
			result = new Result();
		}

		public boolean iterate(String category , String orderLineTypeCode, String orderLineTypeDesc, String oderLineStatusCode,
				int deptNbr, String returnedItemInd, String retailCode , String siteTypeDesc , int storeNbr , int visitNbr)
				throws HiveException {

			if (result == null)
				throw new HiveException("Result is not initialized");

			String value = storeNbr + "_" + visitNbr;
			
			if (category.equals("TOTAL")) {
				TOTAL_MIX obj = new TOTAL_MIX(deptNbr);
				if (obj.check()) {
					if (!result.list.contains(value))
						result.list.add(value);
				}
			}

			if (category.equals("AUCTION")) {
				AUCTION obj = new AUCTION(orderLineTypeDesc, retailCode , deptNbr);
				if (obj.check()) {
					if (!result.list.contains(value))
						result.list.add(value);
				}
			}

			if (category.equals("DTH")) {
				DTH obj = new DTH(deptNbr, orderLineTypeCode, siteTypeDesc, oderLineStatusCode , retailCode);
				if (obj.check()) {
					if (!result.list.contains(value))
						result.list.add(value);
				}
			}
			
			if (category.equals("CPU")) {
				CPU obj = new CPU(deptNbr, orderLineTypeCode, oderLineStatusCode);
				if (obj.check()) {
					if (!result.list.contains(value))
						result.list.add(value);
				}
			}
			
			if (category.equals("FUEL")) {
				FUEL obj = new FUEL(deptNbr , retailCode);
				if (obj.check()) {
					if (!result.list.contains(value))
						result.list.add(value);
				}
			}
			
			if (category.equals("INCLUB")) {
				INCLUB obj = new INCLUB(retailCode, deptNbr);
				if (obj.check()) {
					if (!result.list.contains(value))
						result.list.add(value);
				}
			}
			
			if (category.equals("NONMERCH")) {
				NONMERCH obj = new NONMERCH(deptNbr , retailCode);
				if (obj.check()) {
					if (!result.list.contains(value))
						result.list.add(value);
				}
			}
			
			if (category.equals("NONMERCH_ONLINE")) {
				NONMERCH_ONLINE obj = new NONMERCH_ONLINE(deptNbr , retailCode);
				if (obj.check()) {
					if (!result.list.contains(value))
						result.list.add(value);
				}
			}
			
			if (category.equals("SOT")) {
				SOT obj = new SOT(deptNbr, orderLineTypeCode , retailCode);
				if (obj.check()) {
					if (!result.list.contains(value))
						result.list.add(value);
				}
			}

			return true;
		}

		public int terminate() {
			return result.list.size();
		}

		public Result terminatePartial() {
			return result;
		}

		public boolean merge(Result another) {
			if (another == null)
				return true;

			for (String value : another.list) {
				if (!result.list.contains(value))
					result.list.add(value);
			}
			return true;
		}
	}
}