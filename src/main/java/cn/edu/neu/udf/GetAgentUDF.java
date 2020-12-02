package cn.edu.neu.udf;

import cn.edu.neu.tools.JudgeAgent;
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
public class GetAgentUDF extends GenericUDF {
    private transient ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        // 参数长度校验
        if (objectInspectors.length != 1) {
            throw new UDFArgumentLengthException("ScoreUDF() requires 1 argument, got " + objectInspectors.length);
        }
        // 长度确定了，初始化转换器
        converters = new ObjectInspectorConverters.Converter[objectInspectors.length];

        // 第一个参数的类型校验
        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type objectInspectors are accepted but " + objectInspectors[0].getCategory() + " " +
                            "is passed.");
        } else {
            if (!((PrimitiveObjectInspector) objectInspectors[0]).getPrimitiveCategory().equals(PrimitiveObjectInspector.PrimitiveCategory.STRING)) {
                throw new UDFArgumentTypeException(0,
                        "Only primitive string type objectInspectors are accepted but " + ((PrimitiveObjectInspector) objectInspectors[0]).getPrimitiveCategory() + " is passed.");
            }
        }

        // 创建第一个参数的转换器，转换为string类型
        converters[0] = ObjectInspectorConverters.getConverter(objectInspectors[0],
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        // 返回输出类型
        // 该句和上面没关系。
        return PrimitiveObjectInspectorFactory
                .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        String agent = (String) converters[0].convert(args[0].get());
        return JudgeAgent.judge(agent);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}
