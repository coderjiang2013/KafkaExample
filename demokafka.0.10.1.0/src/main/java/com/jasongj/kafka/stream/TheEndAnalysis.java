package com.jasongj.kafka.stream;

import com.jasongj.kafka.stream.model.Item;
import com.jasongj.kafka.stream.model.Order;
import com.jasongj.kafka.stream.model.User;
import com.jasongj.kafka.stream.serdes.SerdesFactory;
import com.jasongj.kafka.stream.timeextractor.OrderTimestampExtractor;
import org.apache.commons.lang3.StringUtils;
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

    //TODO
    private Hashmap<>

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


        itemTable.foreach((String key, Item item) -> System.out.printf("%s -- %s -- %s\n", key, item.getItemName(), item.getAddress()));

        KStream<String, OrderItemCount> orderItemStream = orderStream
                .leftJoin(itemTable, (Order order, Item item) -> OrderItem.fromOrderItem(order, item), Serdes.String(), SerdesFactory.serdFrom(Order.class))
                .map( (String itemName, OrderItem orderItem) -> KeyValue.pair( itemName,  OrderItemCount.fromOrderItem(orderItem)));


        KTable<String, OrderItemCount> orderItemCnt = orderItemStream
                .groupByKey(Serdes.String(), SerdesFactory.serdFrom(OrderItemCount.class))
                .reduce( ( OrderItemCount orderItemCount1, OrderItemCount orderItemCount2 ) -> CalcOrderUserItem(orderItemCount1, orderItemCount2), "local-gender-amount-store" );


        orderItemCnt.foreach( (String itemName, OrderItemCount orderItemCount) -> System.out.printf("%s -- %s -- %s\n", itemName, orderItemCount.getQuantity(), orderItemCount.getSales()) );



        KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder, props);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        System.in.read();
        kafkaStreams.close();
        kafkaStreams.cleanUp();
    }

    private static OrderItemCount CalcOrderUserItem(OrderItemCount orderItemCount, OrderItemCount orderItemCount2) {


        orderItemCount.addQuantity(orderItemCount2.getQuantity());
        orderItemCount.addSales(orderItemCount2.getSales());

        return orderItemCount;

    }

    static class OrderItemCount{

        private String itemType;
        private String itemName;
        private int quantity;
        private double price;
        private double sales;

        private long start;
        private long end;


        static  OrderItemCount fromOrderItem(OrderItem orderItem){

            OrderItemCount orderItemCount = new OrderItemCount();

            orderItemCount.setItemType(orderItem.getItemType());
            orderItemCount.setItemName(orderItem.getItemName());
            orderItemCount.setQuantity(orderItem.getQuantity());
            orderItemCount.setPrice(orderItem.getItemPrice());
            orderItemCount.setSales(orderItem.getItemPrice() * orderItem.getQuantity());

            return orderItemCount;

        }

        public String getItemType() {
            return itemType;
        }

        public void setItemType(String itemType) {
            this.itemType = itemType;
        }

        public String getItemName() {
            return itemName;
        }

        public void setItemName(String itemName) {
            this.itemName = itemName;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }

        public double getSales() {
            return sales;
        }

        public void setSales(double sales) {
            this.sales = sales;
        }

        public void addQuantity(int quantity) {
            this.quantity = this.quantity + quantity;
        }

        public void addSales(double sales) {
            this.sales = this.sales + sales;
        }

        public long getStart() {
            return start;
        }

        public void setStart(long start) {
            this.start = start;
        }

        public long getEnd() {
            return end;
        }

        public void setEnd(long end) {
            this.end = end;
        }
    }

    static class OrderItem {

        private String userName;
        private String itemName;
        private long transactionDate;
        private int quantity;
        private String userAddress;
        private String itemType;
        private double itemPrice;


        public static OrderItem fromOrderItem(Order order, Item item) {

            OrderItem orderItem = new OrderItem();

            orderItem.setUserName(order.getUserName());
            orderItem.setItemName(order.getItemName());
            orderItem.setTransactionDate(order.getTransactionDate());
            orderItem.setQuantity(order.getQuantity());
            orderItem.setUserAddress(item.getAddress());
            orderItem.setItemType(item.getType());
            orderItem.setItemPrice(item.getPrice());

            return orderItem;

        }

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
    }


}
