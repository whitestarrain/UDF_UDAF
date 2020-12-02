package cn.edu.neu.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * @author liyu
 */
public class GetMinDayUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] param) throws SemanticException {
        if (param.length != 3) {
            throw new UDFArgumentLengthException("Exactly three argument is expected.");
        }
        return new GetMinDayUDAFEvaluator();
    }

    public static class GetMinDayUDAFEvaluator extends GenericUDAFEvaluator {
        PrimitiveObjectInspector mapInput1;
        PrimitiveObjectInspector mapInput2;
        PrimitiveObjectInspector mapInput3;
        ObjectInspector out;// map 和out 输出都为int类型。所以这里就定义一个
        PrimitiveObjectInspector reduceInput; // int 类型
        int minDate = 0;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            // map阶段
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                // 其实都是String类型
                mapInput1 = (PrimitiveObjectInspector) parameters[0];
                mapInput2 = (PrimitiveObjectInspector) parameters[1];
                mapInput3 = (PrimitiveObjectInspector) parameters[2];
            } else {
                // 其余阶段
                reduceInput = (PrimitiveObjectInspector) parameters[0];
            }
            out = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
                    ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            // 返回输出类型
            return out;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MaxDateAgg minDateAgg = new MaxDateAgg();
            return minDateAgg;
        }

        /**
         * 重置为0
         *
         * @param agg
         * @throws HiveException
         */
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MaxDateAgg) agg).minDate = 0;
        }

        /**
         * map阶段，每一行执行的逻辑
         *
         * @param agg
         * @param parameters
         * @throws HiveException
         */
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters == null) {
                return;
            }

            ((MaxDateAgg) agg).compareDate(365 * Integer.parseInt(String.valueOf(parameters[0]))
                    + 30 * Integer.parseInt(String.valueOf(parameters[1]))
                    + Integer.parseInt(String.valueOf(parameters[2])));
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MaxDateAgg agg1 = (MaxDateAgg) agg;
            agg1.compareDate(minDate);
            return agg1.minDate;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            MaxDateAgg agg1 = (MaxDateAgg) agg;
            Integer i = (Integer) reduceInput.getPrimitiveJavaObject(partial);
            agg1.compareDate(i);
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MaxDateAgg agg1 = (MaxDateAgg) agg;
            agg1.compareDate(minDate);
            return agg1.minDate;
        }

        static class MaxDateAgg implements AggregationBuffer {
            int minDate = 0;

            void compareDate(int date) {
                if (date < minDate) {
                    minDate = date;
                }
            }
        }
    }
}
