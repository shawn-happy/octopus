package io.github.shawn.octopus.fluxus.engine.connector.source.activemq;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.engine.connector.source.pulsar.PulsarData;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.commons.lang3.StringEscapeUtils;
import org.junit.jupiter.api.Test;

public class ActivemqClientTests {
  private ActiveMQConnectionFactory activeMQConnectionFactory;
  private Connection connection;
  private Session session;
  private ActiveMQMessageConsumer consumer;
  MessageProducer producer;

  String username = "admin";
  String password = "admin";
  String brokerurl = "tcp://192.168.54.207:61616";
  String queueName = "325test";

  protected static final ObjectMapper OM =
      new ObjectMapper()
          .findAndRegisterModules()
          .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature())
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  @Test
  public void testProducer() throws Exception {
    try {
      activeMQConnectionFactory = new ActiveMQConnectionFactory(username, password, brokerurl);
      connection = activeMQConnectionFactory.createConnection();
      connection.start();
      // 创建会话; 参数:  事务, 签收机制;
      session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      // topic还是queuqe？？？
      Topic destination = session.createTopic(queueName);
      //            Destination destination = session.createQueue(queueName);
      producer = session.createProducer(destination);
    } catch (Exception e) {
      throw new DataWorkflowException(e);
    }
    int num = 1;
    for (int j = 0; j < 50; j++) {
      //        for (int j = 0; j < 200; j++) {
      List<String> messages = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {

        String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        String demo1 =
            "<IMFRoot xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\"><SysInfo><MessageSequenceID>109882</MessageSequenceID><TestUUId>";
        String demo2 =
            "</TestUUId><MessageType>FlightData</MessageType><ServiceType>FSS1</ServiceType><OperationMode>MOD</OperationMode><SendDateTime>2023-03-31T00:03:05.818</SendDateTime><CreateDateTime>2023-03-31T00:03:05.795</CreateDateTime><OriginalDateTime>2023-03-31T00:03:05</OriginalDateTime><Receiver>WX</Receiver><Sender>IMF</Sender><Owner>AODB</Owner></SysInfo><Data><PrimaryKey><FlightKey><FlightScheduledDate>2023-03-31</FlightScheduledDate><FlightIdentity>JD5530</FlightIdentity><FlightDirection>A</FlightDirection><BaseAirport><AirportIATACode>HAK</AirportIATACode><AirportICAOCode>ZJHK</AirportICAOCode></BaseAirport><DetailedIdentity><AirlineIATACode>JD</AirlineIATACode><AirlineICAOCode>CBJ</AirlineICAOCode></DetailedIdentity><InternalID>1003782114</InternalID></FlightKey></PrimaryKey><FlightData><General><FlightScheduledDateTime>2023-03-31T00:35:00</FlightScheduledDateTime><Registration>B6723</Registration><CallSign>CBJ5530</CallSign><AircraftType><AircraftIATACode>320</AircraftIATACode><AircraftICAOCode>A320</AircraftICAOCode></AircraftType><FlightServiceType><FlightCAACServiceType>W/Z</FlightCAACServiceType><FlightIATAServiceType>J</FlightIATAServiceType></FlightServiceType><FlightRoute><IATARoute><IATAOriginAirport>NNG</IATAOriginAirport><IATAPreviousAirport>NNG</IATAPreviousAirport><IATAFullRoute><AirportIATACode LegNo=\\\"1\\\">NNG</AirportIATACode></IATAFullRoute></IATARoute><ICAORoute><ICAOOriginAirport>ZGNN</ICAOOriginAirport><ICAOPreviousAirport>ZGNN</ICAOPreviousAirport><ICAOFullRoute><AirportICAOCode LegNo=\\\"1\\\">ZGNN</AirportICAOCode></ICAOFullRoute></ICAORoute></FlightRoute><FlightCountryType>D</FlightCountryType><LinkFlight><FlightScheduledDate>2023-03-31</FlightScheduledDate><FlightIdentity>JD5891</FlightIdentity><FlightDirection>D</FlightDirection><BaseAirport><AirportIATACode>HAK</AirportIATACode><AirportICAOCode>ZJHK</AirportICAOCode></BaseAirport><DetailedIdentity><AirlineIATACode>JD</AirlineIATACode><AirlineICAOCode>CBJ</AirlineICAOCode></DetailedIdentity><InternalID>1003782389</InternalID></LinkFlight><SlaveFlight><FlightIdentity/></SlaveFlight><FreeTextComment><PublicTextComment xsi:nil=\\\"true\\\"/></FreeTextComment></General><OperationalDateTime><PreviousAirportDepartureDateTime><ScheduledPreviousAirportDepartureDateTime>2023-03-30T23:20:00</ScheduledPreviousAirportDepartureDateTime><EstimatedPreviousAirportDepartureDateTime xsi:nil=\\\"true\\\"/><ActualPreviousAirportDepartureDateTime>2023-03-30T23:24:00</ActualPreviousAirportDepartureDateTime></PreviousAirportDepartureDateTime><LandingDateTime><ScheduledLandingDateTime>2023-03-31T00:35:00</ScheduledLandingDateTime><EstimatedLandingDateTime OldValue=\\\"2023-03-31T00:15:00\\\">2023-03-31T00:10:00</EstimatedLandingDateTime><ActualLandingDateTime xsi:nil=\\\"true\\\"/></LandingDateTime><OnBlockDateTime><ScheduledOnBlockDateTime>2023-03-31T00:40:00</ScheduledOnBlockDateTime><EstimatedOnBlockDateTime OldValue=\\\"2023-03-31T00:20:00\\\">2023-03-31T00:15:00</EstimatedOnBlockDateTime><ActualOnBlockDateTime xsi:nil=\\\"true\\\"/></OnBlockDateTime></OperationalDateTime><Status><OperationStatus/><FlightStatus>P</FlightStatus><DelayReason><DelayCode/><DelayFreeText/></DelayReason><DiversionAirport><AirportIATACode/><AirportICAOCode/></DiversionAirport><ChangeLandingAirport><AirportIATACode/><AirportICAOCode/></ChangeLandingAirport></Status><Airport><Terminal><FlightTerminalID>01</FlightTerminalID><AircraftTerminalID/></Terminal><Stand><StandID>25</StandID></Stand><GroundMovement><StandID>25</StandID><ScheduledStandStartDateTime OldValue=\\\"2023-03-31T00:20:00\\\">2023-03-31T00:15:00</ScheduledStandStartDateTime><ScheduledStandEndDateTime>2023-03-31T10:20:00</ScheduledStandEndDateTime><ActualStandStartDateTime xsi:nil=\\\"true\\\"/><ActualStandEndDateTime xsi:nil=\\\"true\\\"/></GroundMovement><Gate><GateID/><GateStatus xsi:nil=\\\"true\\\"/><ScheduledGateStartDateTime xsi:nil=\\\"true\\\"/><ScheduledGateEndDateTime xsi:nil=\\\"true\\\"/><ActualGateStartDateTime xsi:nil=\\\"true\\\"/><ActualGateEndDateTime xsi:nil=\\\"true\\\"/><BoardingStartDateTime xsi:nil=\\\"true\\\"/><LastCallDateTime xsi:nil=\\\"true\\\"/><BoardingEndDateTime xsi:nil=\\\"true\\\"/></Gate><BaggageReclaim><BaggageReclaimID>3</BaggageReclaimID><ScheduledReclaimStartDateTime OldValue=\\\"2023-03-31T00:30:00\\\">2023-03-31T00:25:00</ScheduledReclaimStartDateTime><ScheduledReclaimEndDateTime OldValue=\\\"2023-03-31T00:55:00\\\">2023-03-31T00:50:00</ScheduledReclaimEndDateTime><ActualReclaimStartDateTime xsi:nil=\\\"true\\\"/><ActualReclaimEndDateTime xsi:nil=\\\"true\\\"/><FirstBaggageDateTime xsi:nil=\\\"true\\\"/><LastBaggageDateTime xsi:nil=\\\"true\\\"/></BaggageReclaim></Airport></FlightData></Data></IMFRoot>";

        String uuid = "" + num++;
        String demo3 = demo1 + uuid + demo2;

        String xml2 = StringEscapeUtils.unescapeJson(demo3);
        String[] dataObj = new String[] {xml2};
        PulsarData data =
            PulsarData.builder().data(xml2).sendTime(now).eventTime(now).operate("INSERT").build();
        String msg = OM.writeValueAsString(data);
        TextMessage textMessage = session.createTextMessage(msg);
        producer.send(textMessage);
        System.out.println("Message sent to the queue.");
        //                messages.add(s);
      }
      TimeUnit.SECONDS.sleep(1);
    }
    producer.close();
    session.close();
    connection.close();
  }

  @Test
  public void testConsumer() throws Exception {
    try {
      activeMQConnectionFactory = new ActiveMQConnectionFactory(username, password, brokerurl);
      connection = activeMQConnectionFactory.createConnection();
      connection.start();
      // 创建会话; 参数:  事务, 签收机制;
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      // 创建目的地;例如:队列
      Topic destination = session.createTopic(queueName);
      //            Destination destination = session.createQueue(queueName);
      // 创建消费者;
      AtomicInteger i = new AtomicInteger(1);
      consumer = (ActiveMQMessageConsumer) session.createConsumer(destination);
      consumer.setMessageListener(
          message -> {
            try {
              System.out.println(i.getAndIncrement() + ":msg = " + message.getJMSMessageID());
              if (i.get() % 100 == 0) {
                System.out.println(i + ":倍100");
                TimeUnit.SECONDS.sleep(1);
              }
            } catch (Exception e) {
              e.printStackTrace();
            }
          });
      /*            try {
                      while (true) {
                          Message message = consumer.receive(200000);
                          if (message == null) {
                              break;
                          } else {
                              String msg = ((TextMessage) message).getText();
                              System.out.println(i.getAndIncrement() + ":msg = " + message.getJMSMessageID());
      //                    System.out.println(i++ + ":msg = " + msg);
                              if (i.get() % 100 == 0) {
                                  System.out.println(i+":倍100");
                                  TimeUnit.SECONDS.sleep(1);
                              }
                          }
                      }
      //            consumer.acknowledge();
                  } catch (Exception e) {
                      throw new RuntimeException(e);
                  }*/
    } catch (Exception e) {
      throw new DataWorkflowException(e);
    }
    TimeUnit.MINUTES.sleep(100000);
  }
}
