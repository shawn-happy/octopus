package io.github.shawn.octopus.drools;

import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.Resource;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class DroolsApplication implements ApplicationRunner {
    public static void main(String[] args) {
        SpringApplication.run(DroolsApplication.class, args);
    }

    @Resource
    private KieContainer kieContainer;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<Order> orderList = getInitData();
        for (Order order : orderList) {
            if (order.getPrice() <= 100) {
                order.setScore(0);
                addScore(order);
            } else if (order.getPrice() <= 500) {
                order.setScore(100);
                addScore(order);
            } else if (order.getPrice() <= 1000) {
                order.setScore(500);
                addScore(order);
            } else {
                order.setScore(1000);
                addScore(order);
            }
        }

        KieSession kieSession = kieContainer.newKieSession();
        for (Order order: orderList) {
            // 1-规则引擎处理逻辑
            kieSession.insert(order);
            kieSession.fireAllRules();
            // 2-执行完规则后, 执行相关的逻辑
            addScore(order);
        }
        kieSession.dispose();
    }

    private static void addScore(Order o) {
        System.out.println("用户" + o.getUser() + "享受额外增加积分: " + o.getScore());
    }

    private static List<Order> getInitData() throws Exception {
        List<Order> orderList = new ArrayList<>();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        {
            Order order = new Order();
            order.setPrice(80);
            order.setBookingDate(df.parse("2015-07-01"));
            order.setUser("name1");
            order.setScore(111);
            orderList.add(order);
        }
        {
            Order order = new Order();
            order.setPrice(200);
            order.setBookingDate(df.parse("2015-07-02"));
            order.setUser("name2");
            orderList.add(order);
        }
        {
            Order order = new Order();
            order.setPrice(800);
            order.setBookingDate(df.parse("2015-07-03"));
            order.setUser("name3");
            orderList.add(order);
        }
        {
            Order order = new Order();
            order.setPrice(1500);
            order.setBookingDate(df.parse("2015-07-04"));
            order.setUser("name4");
            orderList.add(order);
        }
        return orderList;
    }
}
