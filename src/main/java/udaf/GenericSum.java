package udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public class GenericSum extends AbstractGenericUDAFResolver {

	@Override
	public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
		return new SumEvaluator();
	}

	public static class SumEvaluator extends GenericUDAFEvaluator {

		PrimitiveObjectInspector inputOI;
        ObjectInspector outputOI;

		public static class SumAgg implements AggregationBuffer {
			Integer sum = 0;

			void add(int num) {
				sum += num;
			}
			
			void set(int num){
				sum = num;
			}
		}

		@Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
        	
            super.init(m, parameters);
            
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            }
            outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
                    ObjectInspectorOptions.JAVA);
            return outputOI;
        }
		
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			SumAgg ob = new SumAgg();
			return ob;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			((SumAgg)agg).set(0);
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			if (parameters[0] != null) {
				SumAgg myagg = (SumAgg) agg;
				Object p1 = inputOI.getPrimitiveJavaObject(parameters[0]);
				myagg.add((Integer) p1);
			}
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			return (SumAgg) agg;
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			if (partial != null) {
				((SumAgg) agg).add(((SumAgg)partial).sum);
			}
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			SumAgg myagg = (SumAgg) agg;
            int total = myagg.sum;
            return total;
		}

	}
}