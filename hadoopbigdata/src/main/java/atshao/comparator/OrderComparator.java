package atshao.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderComparator extends WritableComparator{

    protected OrderComparator(){
        super(OrderBean.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = new OrderBean();
        OrderBean bBean = new OrderBean();

        int result;
        if(aBean.getOrder_id() > bBean.getOrder_id()){
            result = 1;
        }else if (aBean.getOrder_id() < bBean.getOrder_id()){
            result = -1;
        }else {
            result = 0;
        }
        return result;
    }
}
