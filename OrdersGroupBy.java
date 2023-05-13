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
public class OrdersGroupBy {
    public static class Order implements WritableComparable<Order> {
        private String order_key = "";
        private String cust_key = "";
        private String order_status = "";
        private String total_price = "";
        private String order_date = "";
        private String order_priority = "";
        private String clerk = "";
        private String ship_priority = "";
        private String comment = "";
        private String vis = "";

        public Order() {
            order_key = "";
            cust_key = "";
            order_status = "";
            total_price = "";
            order_date = "";
            order_priority = "";
            clerk = "";
            ship_priority = "";
            comment = "";
            vis = "0";
        }

        public Order(Order c) {
            order_key = c.order_key;
            cust_key = c.cust_key;
            order_status = c.order_status;
            total_price = c.total_price;
            order_date = c.order_date;
            order_priority = c.order_priority;
            clerk = c.clerk;
            ship_priority = c.ship_priority;
            comment = c.comment;
            vis = c.vis;
        }

        public void setOrder(String str) {
            String s[] = str.split("\\|");
            order_key = s[0];
            cust_key = s[1];
            order_status = s[2];
            total_price = s[3];
            order_date = s[4];
            order_priority = s[5];
            clerk = s[6];
            ship_priority = s[7];
            comment = s[8];
            vis = "0";
        }
        public void setVis(){
            vis = "1";
        }

        public String getVis(){
            return vis;
        }

        public String getOrder_key(){
            return order_key;
        }

        public String getOrder_priority(){
            return order_priority;
        }
        public String getShip_priority(){
            return ship_priority;
        }
        @Override
        public int compareTo(Order o) {
            return order_key.compareTo(o.order_key);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(order_key);
            dataOutput.writeUTF(cust_key);
            dataOutput.writeUTF(order_status);
            dataOutput.writeUTF(total_price);
            dataOutput.writeUTF(order_date);
            dataOutput.writeUTF(order_priority);
            dataOutput.writeUTF(clerk);
            dataOutput.writeUTF(ship_priority);
            dataOutput.writeUTF(comment);
            dataOutput.writeUTF(vis);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            order_key = dataInput.readUTF();
            cust_key = dataInput.readUTF();
            order_status = dataInput.readUTF();
            total_price = dataInput.readUTF();
            order_date = dataInput.readUTF();
            order_priority = dataInput.readUTF();
            clerk = dataInput.readUTF();
            ship_priority = dataInput.readUTF();
            comment = dataInput.readUTF();
            vis = dataInput.readUTF();
        }

        @Override
        public String toString(){
            return order_key + "|" + cust_key + "|" + order_status + "|" + total_price  + "|" +
                    order_date + "|" + order_priority + "|" + clerk + "|" + ship_priority + "|" + comment;
        }
    }

    public static class OrderMap extends Mapper<Object, Text, Text, Order> {
        private Text keyInfo = new Text();
        private Order valueInfo = new Order();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String line = itr.nextToken();
                valueInfo.setOrder(line);
                context.write(keyInfo, valueInfo);
            }
        }
    }

    private static class OrderReducer extends Reducer<Text, Order, Text, NullWritable> {
        private Text k = new Text();

        @Override
        protected void reduce(Text key, Iterable<Order> values, Context context) throws IOException, InterruptedException {
            Vector<Order> ordersList = new Vector<Order>();
            for (Order value : values) {
                Order c = new Order(value);
                ordersList.add(c);
            }
            for(int i = 0; i < ordersList.size(); i++)
            {
                if(ordersList.get(i).getVis().equals("1"))
                {
                    continue;
                }
                ordersList.get(i).setVis();
                Vector<Order> orders = new Vector<Order>();
                Order c = new Order(ordersList.get(i));
                orders.add(c);
                int ship_priority = Integer.valueOf(ordersList.get(i).getShip_priority());
                for(int j = i + 1; j < ordersList.size(); j++)
                {
                    if(ordersList.get(j).getVis().equals("1"))
                    {
                        continue;
                    }
                    if(ordersList.get(j).getOrder_priority().equals(ordersList.get(i).getOrder_priority()))
                    {
                        ordersList.get(j).setVis();
                        if(ship_priority < Integer.valueOf(ordersList.get(j).getShip_priority()))
                        {
                            orders.clear();
                            ship_priority = Integer.valueOf(ordersList.get(j).getShip_priority());
                            Order temp = new Order(ordersList.get(j));
                            orders.add(temp);
                        }
                        else if(ship_priority == Integer.valueOf(ordersList.get(j).getShip_priority()))
                        {
                            Order temp = new Order(ordersList.get(j));
                            orders.add(temp);
                        }
                        else
                        {
                            continue;
                        }
                    }
                }
                for(Order o:orders)
                {
                    k.set(o.getOrder_key() + " " + o.getOrder_priority() + " " + o.getShip_priority());
                    context.write(k,NullWritable.get());
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(OrdersGroupBy.class);

        // 设置Map和Reduce处理类
        job.setMapperClass(OrderMap.class);
        job.setReducerClass(OrderReducer.class);

        // 设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Order.class);

        // 设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
