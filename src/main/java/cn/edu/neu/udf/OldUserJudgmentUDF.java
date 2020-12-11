package cn.edu.neu.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author liyu
 */
public class OldUserJudgmentUDF extends GenericUDF {

    private transient ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 3) {
            throw new UDFArgumentLengthException("ScoreUDF() requires 3 argument, got " + arguments.length);
        }

        // 长度确定了，初始化转换器
        converters = new ObjectInspectorConverters.Converter[arguments.length];


        for (int i = 0; i < 3; i++) {
            // 三个参数的类型校验。都是int类型
            if (!arguments[i].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
                throw new UDFArgumentTypeException(0,
                        "Only primitive type arguments are accepted but " + arguments[i].getCategory() + " " +
                                "is passed.");
            } else {
                if (!(((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory().
                        equals(PrimitiveObjectInspector.PrimitiveCategory.INT)
                        ||
                        ((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory().
                                equals(PrimitiveObjectInspector.PrimitiveCategory.LONG))) {
                    throw new UDFArgumentTypeException(0,
                            "Only primitive int or long type arguments are accepted but " + ((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory() + " is passed.");
                }
            }
        }

        converters[0] = ObjectInspectorConverters.getConverter(arguments[0],
                PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        converters[1] = ObjectInspectorConverters.getConverter(arguments[1],
                PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        converters[2] = ObjectInspectorConverters.getConverter(arguments[2],
                PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        // 返回类型
        return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        long minDay = (long) converters[0].convert(arguments[0].get());
        long maxDay = (long) converters[1].convert(arguments[1].get());
        long countDay = (long) converters[2].convert(arguments[2].get());
        int flag = 0;
        if (maxDay - minDay >= 3 && (float) countDay / (maxDay - minDay) > 0.7) {
            flag = 1;
        }
        return flag;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "";
    }
}
