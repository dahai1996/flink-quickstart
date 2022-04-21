package function;

import java.io.Serializable;

/**
 * @author wfs
 */
public interface ShuntValue<T> extends Serializable {
    /**
     * 如何划分record的
     * @param value 单条记录
     * @return 通过该记录返回一个int，这个数值将会对集群节点数取模，用于分流到不同节点
     */
    int shunt(T value);
}
