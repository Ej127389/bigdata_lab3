import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Random;
import java.util.Vector;

public class CustomerGroupBy {
    public static class Customer implements WritableComparable<Customer> {
        private String c_custkey = "";
        private String c_name = "";
        private String c_address = "";
        private String c_nationkey = "";
        private String c_phone = "";
        private String c_acctbal = "";
        private String c_mktsegment = "";
        private String c_comment = "";
        private String vis = "";
        public Customer(){
            c_custkey = "";
            c_name = "";
            c_address = "";
            c_nationkey = "";
            c_phone = "";
            c_acctbal = "";
            c_mktsegment = "";
            c_comment = "";
            vis = "0";
        }

        public Customer(Customer c) {
            c_custkey = c.c_custkey;
            c_name = c.c_name;
            c_address = c.c_address;
            c_nationkey = c.c_nationkey;
            c_phone = c.c_phone;
            c_acctbal = c.c_acctbal;
            c_mktsegment = c.c_mktsegment;
            c_comment = c.c_comment;
            vis = c.vis;
        }
        public void setCustomer(String str){
            String s[] = str.split("\\|");
            c_custkey = s[0];
            c_name = s[1];
            c_address = s[2];
            c_nationkey = s[3];
            c_phone = s[4];
            c_acctbal = s[5];
            c_mktsegment = s[6];
            c_comment = s[7];
            vis = "0";
        }

        public String getC_nationkey()
        {
            return c_nationkey;
        }

        public void setVis(){
            vis = "1";
        }

        public String getVis(){
            return vis;
        }

        public String getC_acctbal(){
            return c_acctbal;
        }

        @Override
        public int compareTo(Customer o) {
            return c_nationkey.compareTo(o.c_nationkey);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(c_custkey);
            dataOutput.writeUTF(c_name);
            dataOutput.writeUTF(c_address);
            dataOutput.writeUTF(c_nationkey);
            dataOutput.writeUTF(c_phone);
            dataOutput.writeUTF(c_acctbal);
            dataOutput.writeUTF(c_mktsegment);
            dataOutput.writeUTF(c_comment);
            dataOutput.writeUTF(vis);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            c_custkey = dataInput.readUTF();
            c_name = dataInput.readUTF();
            c_address = dataInput.readUTF();
            c_nationkey = dataInput.readUTF();
            c_phone = dataInput.readUTF();
            c_acctbal = dataInput.readUTF();
            c_mktsegment = dataInput.readUTF();
            c_comment = dataInput.readUTF();
            vis = dataInput.readUTF();
        }

        @Override
        public String toString(){
            return c_custkey + "|" + c_name + "|" + c_address + "|" + c_nationkey  + "|" +
                    c_phone + "|" + c_acctbal + "|" + c_mktsegment + "|" + c_comment;
        }

    }

    public static class CustomerMap extends Mapper<Object, Text, Text, Customer> {
        private Text keyInfo = new Text();
        private Customer valueInfo = new Customer();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String line = itr.nextToken();
                valueInfo.setCustomer(line);
                context.write(keyInfo, valueInfo);
            }
        }
    }

    private static class CustomerReducer extends Reducer<Text, Customer, Text, NullWritable> {
        private Text k = new Text();

        @Override
        protected void reduce(Text key, Iterable<Customer> values, Context context) throws IOException, InterruptedException {
            Vector<Customer> customersList = new Vector<Customer>();
            for (Customer value : values) {
                Customer c = new Customer(value);
                customersList.add(c);
            }

            for(int i = 0; i < customersList.size(); i++)
            {
                if(customersList.get(i).getVis().equals("1"))
                {
                    continue;
                }
                double sum = 0;
                sum += Double.valueOf(customersList.get(i).getC_acctbal());
                customersList.get(i).setVis();
                for(int j = i + 1; j < customersList.size(); j++)
                {
                    if(customersList.get(j).getVis().equals("1"))
                    {
                        continue;
                    }
                    if(customersList.get(j).getC_nationkey().equals(customersList.get(i).getC_nationkey()))
                    {
                        sum += Double.valueOf(customersList.get(j).getC_acctbal());
                        customersList.get(j).setVis();
                    }

                }
                k.set(customersList.get(i).getC_nationkey() + " " + String.valueOf(sum));
                context.write(k,NullWritable.get());
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(CustomerGroupBy.class);

        // 设置Map和Reduce处理类
        job.setMapperClass(CustomerMap.class);
        job.setReducerClass(CustomerReducer.class);

        // 设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Customer.class);

        // 设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
