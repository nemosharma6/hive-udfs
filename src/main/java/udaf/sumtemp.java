package udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class sumtemp extends UDAF {

	public static class ProdUDAFEvaluator implements UDAFEvaluator {

		private Integer x = null;

		public ProdUDAFEvaluator() {
			super();
			init();
		}

		public void init() {
			System.out.println("Initializing................................");
			x = 0;
		}

		public boolean iterate(int a) throws HiveException {
			System.out.println("Iterate ........................" + x + "-------" + a);
			x = x+a; 
			return true;
		}

		public Integer terminate() {
			System.out.println("Terminate............................." + x);
			return x;
		}

		public Integer terminatePartial() {
			System.out.println("Terminate Partial ...................." + x);
			return x;
		}

		public boolean merge(Integer another) {
			System.out.println("Merge.................." + x +"------another-------" + another);
			if(x==null || another == null){
				return true;
			}
			
			x = x + another;
			return true;
		}
	}
}