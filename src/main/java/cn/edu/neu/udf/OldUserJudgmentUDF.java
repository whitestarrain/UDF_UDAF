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

//
//        for (int i = 0; i < 3; i++) {
//            // 三个参数的类型校验。都是int类型
//            if (!arguments[i].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
//                throw new UDFArgumentTypeException(0,
//                        "Only primitive type arguments are accepted but " + arguments[i].getCategory() + " " +
//                                "is passed.");
//            } else {
//                if (!((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory().equals(PrimitiveObjectInspector.PrimitiveCategory.INT)) {
//                    throw new UDFArgumentTypeException(0,
//                            "Only primitive int type arguments are accepted but " + ((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory() + " is passed.");
//                }
//            }
//        }

        converters[0] = ObjectInspectorConverters.getConverter(arguments[0],
                PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        converters[1] = ObjectInspectorConverters.getConverter(arguments[1],
                PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        converters[2] = ObjectInspectorConverters.getConverter(arguments[2],
                PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        // 返回类型
        return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        int minDay = (int) converters[0].convert(arguments[0].get());
        int maxDay = (int) converters[1].convert(arguments[1].get());
        int countDay = (int) converters[2].convert(arguments[2].get());
        int flag = 0;
        if (maxDay > minDay && maxDay - minDay > 60 && countDay / (maxDay - minDay) > 0.2){
            flag = 1;
        }
        return flag;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "";
    }
}
