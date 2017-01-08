package com.jasongj.kafka.stream;

import com.jasongj.kafka.stream.model.Item;
import com.jasongj.kafka.stream.model.Order;
import com.jasongj.kafka.stream.model.User;
import com.jasongj.kafka.stream.serdes.SerdesFactory;
import com.jasongj.kafka.stream.timeextractor.OrderTimestampExtractor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TheEndAnalysis {

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-purchase-analysis");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka0:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "zookeeper0:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimestampExtractor.class);

        KStreamBuilder streamBuilder = new KStreamBuilder();
        KStream<String, Order> orderStream = streamBuilder.stream(Serdes.String(), SerdesFactory.serdFrom(Order.class), "orders");
        KTable<String, User> userTable = streamBuilder.table(Serdes.String(), SerdesFactory.serdFrom(User.class), "users", "users-state-store");
        KTable<String, Item> itemTable = streamBuilder.table(Serdes.String(), SerdesFactory.serdFrom(Item.class), "items", "items-state-store");


        KStream<String, OrderInfoCount> userInfoKStream = orderStream
                .leftJoin(userTable, (Order order, User user) -> OrderUser.fromOrderUser(order, user), Serdes.String(), SerdesFactory.serdFrom(Order.class))
                .filter((String userName, OrderUser orderUser) -> orderUser.userAddress != null)
                .map((String userName, OrderUser orderUser) -> new KeyValue<String, OrderUser>(orderUser.itemName, orderUser))
                .through(Serdes.String(), SerdesFactory.serdFrom(OrderUser.class), (String key, OrderUser orderUser, int numPartitions) -> (orderUser.getItemName().hashCode() & 0x7FFFFFFF) % numPartitions, "orderuser-repartition-by-item")
                .leftJoin(itemTable, (OrderUser orderUser, Item item) -> OrderUserItem.fromOrderUser(orderUser, item), Serdes.String(), SerdesFactory.serdFrom(OrderUser.class))
                .map( (String localGender, OrderUserItem orderUserItem) ->  KeyValue.pair(orderUserItem.getItemName(), OrderInfoCount.fromOrderUserItem(orderUserItem) ) );


        //userInfoKStream.foreach( (String key, OrderInfoCount value) -> System.out.printf("key=%s, value=%s -- %s\n", key, value.getTotalSales(), value.getOrderUserItem().getItemName()) );

        KTable<String, OrderInfoCount> itemSalesCount = userInfoKStream
                .groupByKey(Serdes.String(), SerdesFactory.serdFrom(OrderInfoCount.class))
                .reduce( (OrderInfoCount orderInfoCount, OrderInfoCount orderInfoCount2) ->  CalcOrderUserItem(orderInfoCount, orderInfoCount2), "local-gender-amount-store" );


        itemSalesCount.foreach( (String key, OrderInfoCount value) -> System.out.printf("key=%s, value=%s -- %s\n", key, value.getTotalSales(), value.getOrderUserItem().getItemType()) );


        KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder, props);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        System.out.println("server start listen");
        System.in.read();
        kafkaStreams.close();
        kafkaStreams.cleanUp();
    }

    private static OrderInfoCount CalcOrderUserItem(OrderInfoCount orderInfoCount, OrderInfoCount orderInfoCount2) {

        if(orderInfoCount2.getOrderUserItem().getAge()>18 && orderInfoCount2.getOrderUserItem().getAge()<35) {
            orderInfoCount.addOrderCount(1);
            orderInfoCount.addSales(orderInfoCount2.getTotalSales());
            orderInfoCount.addItemCount(orderInfoCount2.getItemCount());
        }
        return orderInfoCount;
    }

    public static class SalesOrder {



        public static void fromOrderInfoCount() {
        }


    }

    public static class OrderInfoCount {

        private AtomicInteger orderCount = new AtomicInteger(0);
        private AtomicInteger itemCount = new AtomicInteger(0);
        private BigDecimal totalSales = new BigDecimal(0);

        private OrderUserItem orderUserItem;

        private Lock lock = new ReentrantLock();

        public OrderInfoCount(){

        }

        public void addOrderCount(int orderCount) {
            this.orderCount.addAndGet(orderCount);
        }

        public void addItemCount(int itemCount) {
            this.itemCount.addAndGet(itemCount);
        }

        public void addSales(BigDecimal sales) {

            lock.lock();
            this.totalSales = this.totalSales.add(sales);
            lock.unlock();

        }


        public void addSales(Integer quantity, Double price) {
            lock.lock();
            this.totalSales = this.totalSales.add( new BigDecimal(quantity).multiply(new BigDecimal(price)) );
            lock.unlock();
        }

        public AtomicInteger getOrderCount() {
            return orderCount;
        }

        public AtomicInteger getItemCount() {
            return itemCount;
        }

        public BigDecimal getTotalSales() {
            return totalSales;
        }

        public OrderUserItem getOrderUserItem() {
            return orderUserItem;
        }

        public static OrderInfoCount fromOrderUserItem(OrderUserItem orderUserItem) {
            OrderInfoCount orderInfoCount = new OrderInfoCount();
            orderInfoCount.addItemCount(orderUserItem.getQuantity());
            orderInfoCount.addOrderCount(1);
            orderInfoCount.addSales(orderUserItem.getQuantity() , orderUserItem.getItemPrice());
            orderInfoCount.orderUserItem = orderUserItem;
            return orderInfoCount;
        }

        public void addItemCount(AtomicInteger itemCount) {
            this.addItemCount(itemCount.intValue());
        }

    }

    public static class OrderUser {
        private String userName;
        private String itemName;
        private long transactionDate;
        private int quantity;
        private String userAddress;
        private String gender;
        private int age;

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getItemName() {
            return itemName;
        }

        public void setItemName(String itemName) {
            this.itemName = itemName;
        }

        public long getTransactionDate() {
            return transactionDate;
        }

        public void setTransactionDate(long transactionDate) {
            this.transactionDate = transactionDate;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }

        public String getUserAddress() {
            return userAddress;
        }

        public void setUserAddress(String userAddress) {
            this.userAddress = userAddress;
        }

        public String getGender() {
            return gender;
        }

        public void setGender(String gender) {
            this.gender = gender;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public static OrderUser fromOrder(Order order) {
            OrderUser orderUser = new OrderUser();
            if (order == null) {
                return orderUser;
            }
            orderUser.userName = order.getUserName();
            orderUser.itemName = order.getItemName();
            orderUser.transactionDate = order.getTransactionDate();
            orderUser.quantity = order.getQuantity();
            return orderUser;
        }

        public static OrderUser fromOrderUser(Order order, User user) {
            OrderUser orderUser = fromOrder(order);
            if (user == null) {
                return orderUser;
            }
            orderUser.gender = user.getGender();
            orderUser.age = user.getAge();
            orderUser.userAddress = user.getAddress();
            return orderUser;
        }
    }

    public static class OrderUserItem {
        private String userName;
        private String itemName;
        private long transactionDate;
        private int quantity;
        private String userAddress;
        private String gender;
        private int age;
        private String itemAddress;
        private String itemType;
        private double itemPrice;

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getItemName() {
            return itemName;
        }

        public void setItemName(String itemName) {
            this.itemName = itemName;
        }

        public long getTransactionDate() {
            return transactionDate;
        }

        public void setTransactionDate(long transactionDate) {
            this.transactionDate = transactionDate;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }

        public String getUserAddress() {
            return userAddress;
        }

        public void setUserAddress(String userAddress) {
            this.userAddress = userAddress;
        }

        public String getGender() {
            return gender;
        }

        public void setGender(String gender) {
            this.gender = gender;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getItemAddress() {
            return itemAddress;
        }

        public void setItemAddress(String itemAddress) {
            this.itemAddress = itemAddress;
        }

        public String getItemType() {
            return itemType;
        }

        public void setItemType(String itemType) {
            this.itemType = itemType;
        }

        public double getItemPrice() {
            return itemPrice;
        }

        public void setItemPrice(double itemPrice) {
            this.itemPrice = itemPrice;
        }

        public static OrderUserItem fromOrderUser(OrderUser orderUser) {
            OrderUserItem orderUserItem = new OrderUserItem();
            if (orderUser == null) {
                return orderUserItem;
            }
            orderUserItem.userName = orderUser.userName;
            orderUserItem.itemName = orderUser.itemName;
            orderUserItem.transactionDate = orderUser.transactionDate;
            orderUserItem.quantity = orderUser.quantity;
            orderUserItem.userAddress = orderUser.userAddress;
            orderUserItem.gender = orderUser.gender;
            orderUserItem.age = orderUser.age;
            return orderUserItem;
        }

        public static OrderUserItem fromOrderUser(OrderUser orderUser, Item item) {
            OrderUserItem orderUserItem = fromOrderUser(orderUser);
            if (item == null) {
                return orderUserItem;
            }
            orderUserItem.itemAddress = item.getAddress();
            orderUserItem.itemType = item.getType();
            orderUserItem.itemPrice = item.getPrice();
            return orderUserItem;
        }
    }

}
