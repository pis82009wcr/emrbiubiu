package com.biubiu.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String prePhoneNum = text.toString().substring(0,3);

        int partition = 4;
        if ("972".equals(prePhoneNum)) {
            partition = 0;
        }else if ("973".equals(prePhoneNum)){
            partition = 1;
        }else if ("974".equals(prePhoneNum)){
            partition = 2;
        }else if ("975".equals(prePhoneNum)){
            partition = 3;
        }
        return partition;
    }
}
