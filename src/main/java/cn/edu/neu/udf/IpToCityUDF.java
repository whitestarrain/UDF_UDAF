package cn.edu.neu.udf;

import cn.edu.neu.udf.tools.IpQueryInterface;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * @author liyu
 */
public class IpToCityUDF extends GenericUDF {
    // 这里会定义好输入和输出

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        // 参数长度校验
        if (objectInspectors.length != 1) {
            throw new UDFArgumentLengthException("ScoreUDF() requires 1 argument, got " + objectInspectors.length);
        }
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

        // 返回输出类型
        return PrimitiveObjectInspectorFactory
                .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        String s = args[0].get().toString();
        Text city = new Text();
        String query = IpQueryInterface.query(s);
        if(query==null){
            city.set("null");
        }else
        {
            city.set(query);
        }
        return city;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}
