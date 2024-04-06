package io.github.shawn.octopus.fluxus.engine.connector.source.pulsar;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.junit.jupiter.api.Test;

public class PulsarClientTests {
  private static final String brokerServiceUrl =
      "pulsar://192.168.54.206:6650,192.168.54.207:6650,192.168.54.208:6650";
  /*  "pulsar://192.168.53.133:6650,192.168.53.134:6650,192.168.53.135:6650";*/

  private static final String token =
      "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.yY2MSTBSEk3WkL3yEfSUdyq-EbeO1Lyg5EG1KiuGO4M";

  protected static final ObjectMapper OM =
      new ObjectMapper()
          .findAndRegisterModules()
          .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature())
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  @Test
  public void testProducer() throws Exception {
    ClientBuilder builder = PulsarClient.builder().serviceUrl(brokerServiceUrl);
    if (StringUtils.isNotBlank(token)) {
      builder.authentication(AuthenticationFactory.token(token));
    }
    PulsarClient client = builder.build();
    ProducerImpl<byte[]> producer =
        (ProducerImpl<byte[]>)
            client
                .newProducer()
                .topic("simple-test-wxg-1")
                .enableBatching(
                    true) // 是否开启批量处理消息，默认true,需要注意的是enableBatching只在异步发送sendAsync生效，同步发送send失效。因此建议生产环境若想使用批处理，则需使用异步发送，或者多线程同步发送
                .compressionType(
                    CompressionType
                        .LZ4) // 消息压缩（四种压缩方式：LZ4，ZLIB，ZSTD，SNAPPY），consumer端不用做改动就能消费，开启后大约可以降低3/4带宽消耗和存储（官方测试）
                .sendTimeout(
                    0,
                    TimeUnit
                        .SECONDS) // 设置发送超时0s；如果在sendTimeout过期之前服务器没有确认消息，则会发生错误。默认30s，设置为0代表无限制，建议配置为0
                .batchingMaxMessages(1000) // 批处理中允许的最大消息数。默认1000
                .maxPendingMessages(1000) // 设置等待接受来自broker确认消息的队列的最大大小，默认1000
                .blockIfQueueFull(true) // 设置当消息队列中等待的消息已满时，Producer.send 和 Producer.sendAsync
                // 是否应该block阻塞。默认为false，达到maxPendingMessages后send操作会报错，设置为true后，send操作阻塞但是不报错。建议设置为true
                .batcherBuilder(
                    BatcherBuilder.DEFAULT) // key_Shared模式要用KEY_BASED,才能保证同一个key的message在一个batch里
                //                                .intercept(new MyPulsarProducerInterceptor())
                .create();

    for (int j = 0; j < 2000; j++) {
      List<String> messages = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        Item item =
            Item.builder()
                .id(RandomUtils.nextLong())
                .name(RandomStringUtils.randomAlphabetic(6))
                .goods(
                    JsonUtils.toJson(
                            Arrays.asList(
                                Good.builder()
                                    .id(RandomUtils.nextLong())
                                    .name(RandomStringUtils.randomAlphabetic(6))
                                    .build(),
                                Good.builder()
                                    .id(RandomUtils.nextLong())
                                    .name(RandomStringUtils.randomAlphabetic(6))
                                    .build(),
                                Good.builder()
                                    .id(RandomUtils.nextLong())
                                    .name(RandomStringUtils.randomAlphabetic(6))
                                    .build()))
                        .get())
                .build();
        String s = JsonUtils.toJson(item).get();
        messages.add(s);
      }
      for (String message : messages) {
        producer.newMessage().value(message.getBytes(StandardCharsets.UTF_8)).sendAsync();
      }
    }

    producer.close();
    client.close();
  }

  @Test
  public void testProducer2() throws Exception {
    ClientBuilder builder = PulsarClient.builder().serviceUrl(brokerServiceUrl);
    if (StringUtils.isNotBlank(token)) {
      builder.authentication(AuthenticationFactory.token(token));
    }
    PulsarClient client = builder.build();
    ProducerImpl<byte[]> producer =
        (ProducerImpl<byte[]>)
            client
                .newProducer()
                .topic("persistent://public/default/318wxg_500w_1")
                .enableBatching(
                    true) // 是否开启批量处理消息，默认true,需要注意的是enableBatching只在异步发送sendAsync生效，同步发送send失效。因此建议生产环境若想使用批处理，则需使用异步发送，或者多线程同步发送
                .compressionType(
                    CompressionType
                        .LZ4) // 消息压缩（四种压缩方式：LZ4，ZLIB，ZSTD，SNAPPY），consumer端不用做改动就能消费，开启后大约可以降低3/4带宽消耗和存储（官方测试）
                .sendTimeout(
                    0,
                    TimeUnit
                        .SECONDS) // 设置发送超时0s；如果在sendTimeout过期之前服务器没有确认消息，则会发生错误。默认30s，设置为0代表无限制，建议配置为0
                .batchingMaxMessages(1000) // 批处理中允许的最大消息数。默认1000
                .maxPendingMessages(1000) // 设置等待接受来自broker确认消息的队列的最大大小，默认1000
                .blockIfQueueFull(true) // 设置当消息队列中等待的消息已满时，Producer.send 和 Producer.sendAsync
                // 是否应该block阻塞。默认为false，达到maxPendingMessages后send操作会报错，设置为true后，send操作阻塞但是不报错。建议设置为true
                .batcherBuilder(
                    BatcherBuilder.DEFAULT) // key_Shared模式要用KEY_BASED,才能保证同一个key的message在一个batch里
                //                                .intercept(new MyPulsarProducerInterceptor())
                .create();

    for (int j = 0; j < 5000; j++) {
      List<String> messages = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {

        String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        String demo1 =
            "<IMFRoot xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\"><SysInfo><MessageSequenceID>109882</MessageSequenceID><TestUUId>";
        String demo2 =
            "</TestUUId><MessageType>FlightData</MessageType><ServiceType>FSS1</ServiceType><OperationMode>MOD</OperationMode><SendDateTime>2023-03-31T00:03:05.818</SendDateTime><CreateDateTime>2023-03-31T00:03:05.795</CreateDateTime><OriginalDateTime>2023-03-31T00:03:05</OriginalDateTime><Receiver>WX</Receiver><Sender>IMF</Sender><Owner>AODB</Owner></SysInfo><Data><PrimaryKey><FlightKey><FlightScheduledDate>2023-03-31</FlightScheduledDate><FlightIdentity>JD5530</FlightIdentity><FlightDirection>A</FlightDirection><BaseAirport><AirportIATACode>HAK</AirportIATACode><AirportICAOCode>ZJHK</AirportICAOCode></BaseAirport><DetailedIdentity><AirlineIATACode>JD</AirlineIATACode><AirlineICAOCode>CBJ</AirlineICAOCode></DetailedIdentity><InternalID>1003782114</InternalID></FlightKey></PrimaryKey><FlightData><General><FlightScheduledDateTime>2023-03-31T00:35:00</FlightScheduledDateTime><Registration>B6723</Registration><CallSign>CBJ5530</CallSign><AircraftType><AircraftIATACode>320</AircraftIATACode><AircraftICAOCode>A320</AircraftICAOCode></AircraftType><FlightServiceType><FlightCAACServiceType>W/Z</FlightCAACServiceType><FlightIATAServiceType>J</FlightIATAServiceType></FlightServiceType><FlightRoute><IATARoute><IATAOriginAirport>NNG</IATAOriginAirport><IATAPreviousAirport>NNG</IATAPreviousAirport><IATAFullRoute><AirportIATACode LegNo=\\\"1\\\">NNG</AirportIATACode></IATAFullRoute></IATARoute><ICAORoute><ICAOOriginAirport>ZGNN</ICAOOriginAirport><ICAOPreviousAirport>ZGNN</ICAOPreviousAirport><ICAOFullRoute><AirportICAOCode LegNo=\\\"1\\\">ZGNN</AirportICAOCode></ICAOFullRoute></ICAORoute></FlightRoute><FlightCountryType>D</FlightCountryType><LinkFlight><FlightScheduledDate>2023-03-31</FlightScheduledDate><FlightIdentity>JD5891</FlightIdentity><FlightDirection>D</FlightDirection><BaseAirport><AirportIATACode>HAK</AirportIATACode><AirportICAOCode>ZJHK</AirportICAOCode></BaseAirport><DetailedIdentity><AirlineIATACode>JD</AirlineIATACode><AirlineICAOCode>CBJ</AirlineICAOCode></DetailedIdentity><InternalID>1003782389</InternalID></LinkFlight><SlaveFlight><FlightIdentity/></SlaveFlight><FreeTextComment><PublicTextComment xsi:nil=\\\"true\\\"/></FreeTextComment></General><OperationalDateTime><PreviousAirportDepartureDateTime><ScheduledPreviousAirportDepartureDateTime>2023-03-30T23:20:00</ScheduledPreviousAirportDepartureDateTime><EstimatedPreviousAirportDepartureDateTime xsi:nil=\\\"true\\\"/><ActualPreviousAirportDepartureDateTime>2023-03-30T23:24:00</ActualPreviousAirportDepartureDateTime></PreviousAirportDepartureDateTime><LandingDateTime><ScheduledLandingDateTime>2023-03-31T00:35:00</ScheduledLandingDateTime><EstimatedLandingDateTime OldValue=\\\"2023-03-31T00:15:00\\\">2023-03-31T00:10:00</EstimatedLandingDateTime><ActualLandingDateTime xsi:nil=\\\"true\\\"/></LandingDateTime><OnBlockDateTime><ScheduledOnBlockDateTime>2023-03-31T00:40:00</ScheduledOnBlockDateTime><EstimatedOnBlockDateTime OldValue=\\\"2023-03-31T00:20:00\\\">2023-03-31T00:15:00</EstimatedOnBlockDateTime><ActualOnBlockDateTime xsi:nil=\\\"true\\\"/></OnBlockDateTime></OperationalDateTime><Status><OperationStatus/><FlightStatus>P</FlightStatus><DelayReason><DelayCode/><DelayFreeText/></DelayReason><DiversionAirport><AirportIATACode/><AirportICAOCode/></DiversionAirport><ChangeLandingAirport><AirportIATACode/><AirportICAOCode/></ChangeLandingAirport></Status><Airport><Terminal><FlightTerminalID>01</FlightTerminalID><AircraftTerminalID/></Terminal><Stand><StandID>25</StandID></Stand><GroundMovement><StandID>25</StandID><ScheduledStandStartDateTime OldValue=\\\"2023-03-31T00:20:00\\\">2023-03-31T00:15:00</ScheduledStandStartDateTime><ScheduledStandEndDateTime>2023-03-31T10:20:00</ScheduledStandEndDateTime><ActualStandStartDateTime xsi:nil=\\\"true\\\"/><ActualStandEndDateTime xsi:nil=\\\"true\\\"/></GroundMovement><Gate><GateID/><GateStatus xsi:nil=\\\"true\\\"/><ScheduledGateStartDateTime xsi:nil=\\\"true\\\"/><ScheduledGateEndDateTime xsi:nil=\\\"true\\\"/><ActualGateStartDateTime xsi:nil=\\\"true\\\"/><ActualGateEndDateTime xsi:nil=\\\"true\\\"/><BoardingStartDateTime xsi:nil=\\\"true\\\"/><LastCallDateTime xsi:nil=\\\"true\\\"/><BoardingEndDateTime xsi:nil=\\\"true\\\"/></Gate><BaggageReclaim><BaggageReclaimID>3</BaggageReclaimID><ScheduledReclaimStartDateTime OldValue=\\\"2023-03-31T00:30:00\\\">2023-03-31T00:25:00</ScheduledReclaimStartDateTime><ScheduledReclaimEndDateTime OldValue=\\\"2023-03-31T00:55:00\\\">2023-03-31T00:50:00</ScheduledReclaimEndDateTime><ActualReclaimStartDateTime xsi:nil=\\\"true\\\"/><ActualReclaimEndDateTime xsi:nil=\\\"true\\\"/><FirstBaggageDateTime xsi:nil=\\\"true\\\"/><LastBaggageDateTime xsi:nil=\\\"true\\\"/></BaggageReclaim></Airport></FlightData></Data></IMFRoot>";

        String uuid = IdGenerator.uuid();
        String demo3 = demo1 + uuid + demo2;

        String xml2 = StringEscapeUtils.unescapeJson(demo3);

        PulsarData data =
            PulsarData.builder().data(xml2).sendTime(now).eventTime(now).operate("INSERT").build();
        String s = OM.writeValueAsString(data);
        messages.add(s);
      }
      for (String message : messages) {
        producer.newMessage().value(message.getBytes(StandardCharsets.UTF_8)).sendAsync();
      }
    }

    producer.close();
    client.close();
  }

  @Test
  public void testProducer3() throws Exception {
    ClientBuilder builder = PulsarClient.builder().serviceUrl(brokerServiceUrl);
    if (StringUtils.isNotBlank(token)) {
      builder.authentication(AuthenticationFactory.token(token));
    }
    PulsarClient client = builder.build();
    ProducerImpl<byte[]> producer =
        (ProducerImpl<byte[]>)
            client
                .newProducer()
                .topic("persistent://public/default/318wxg_5000w_1")
                .enableBatching(
                    true) // 是否开启批量处理消息，默认true,需要注意的是enableBatching只在异步发送sendAsync生效，同步发送send失效。因此建议生产环境若想使用批处理，则需使用异步发送，或者多线程同步发送
                .compressionType(
                    CompressionType
                        .LZ4) // 消息压缩（四种压缩方式：LZ4，ZLIB，ZSTD，SNAPPY），consumer端不用做改动就能消费，开启后大约可以降低3/4带宽消耗和存储（官方测试）
                .sendTimeout(
                    0,
                    TimeUnit
                        .SECONDS) // 设置发送超时0s；如果在sendTimeout过期之前服务器没有确认消息，则会发生错误。默认30s，设置为0代表无限制，建议配置为0
                .batchingMaxMessages(1000) // 批处理中允许的最大消息数。默认1000
                .maxPendingMessages(1000) // 设置等待接受来自broker确认消息的队列的最大大小，默认1000
                .blockIfQueueFull(true) // 设置当消息队列中等待的消息已满时，Producer.send 和 Producer.sendAsync
                // 是否应该block阻塞。默认为false，达到maxPendingMessages后send操作会报错，设置为true后，send操作阻塞但是不报错。建议设置为true
                .batcherBuilder(
                    BatcherBuilder.DEFAULT) // key_Shared模式要用KEY_BASED,才能保证同一个key的message在一个batch里
                //                                .intercept(new MyPulsarProducerInterceptor())
                .create();

    for (int j = 0; j < 50000; j++) {
      List<String> messages = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {

        String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        String demo1 =
            "<IMFRoot xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\"><SysInfo><MessageSequenceID>109882</MessageSequenceID><TestUUId>";
        String demo2 =
            "</TestUUId><MessageType>FlightData</MessageType><ServiceType>FSS1</ServiceType><OperationMode>MOD</OperationMode><SendDateTime>2023-03-31T00:03:05.818</SendDateTime><CreateDateTime>2023-03-31T00:03:05.795</CreateDateTime><OriginalDateTime>2023-03-31T00:03:05</OriginalDateTime><Receiver>WX</Receiver><Sender>IMF</Sender><Owner>AODB</Owner></SysInfo><Data><PrimaryKey><FlightKey><FlightScheduledDate>2023-03-31</FlightScheduledDate><FlightIdentity>JD5530</FlightIdentity><FlightDirection>A</FlightDirection><BaseAirport><AirportIATACode>HAK</AirportIATACode><AirportICAOCode>ZJHK</AirportICAOCode></BaseAirport><DetailedIdentity><AirlineIATACode>JD</AirlineIATACode><AirlineICAOCode>CBJ</AirlineICAOCode></DetailedIdentity><InternalID>1003782114</InternalID></FlightKey></PrimaryKey><FlightData><General><FlightScheduledDateTime>2023-03-31T00:35:00</FlightScheduledDateTime><Registration>B6723</Registration><CallSign>CBJ5530</CallSign><AircraftType><AircraftIATACode>320</AircraftIATACode><AircraftICAOCode>A320</AircraftICAOCode></AircraftType><FlightServiceType><FlightCAACServiceType>W/Z</FlightCAACServiceType><FlightIATAServiceType>J</FlightIATAServiceType></FlightServiceType><FlightRoute><IATARoute><IATAOriginAirport>NNG</IATAOriginAirport><IATAPreviousAirport>NNG</IATAPreviousAirport><IATAFullRoute><AirportIATACode LegNo=\\\"1\\\">NNG</AirportIATACode></IATAFullRoute></IATARoute><ICAORoute><ICAOOriginAirport>ZGNN</ICAOOriginAirport><ICAOPreviousAirport>ZGNN</ICAOPreviousAirport><ICAOFullRoute><AirportICAOCode LegNo=\\\"1\\\">ZGNN</AirportICAOCode></ICAOFullRoute></ICAORoute></FlightRoute><FlightCountryType>D</FlightCountryType><LinkFlight><FlightScheduledDate>2023-03-31</FlightScheduledDate><FlightIdentity>JD5891</FlightIdentity><FlightDirection>D</FlightDirection><BaseAirport><AirportIATACode>HAK</AirportIATACode><AirportICAOCode>ZJHK</AirportICAOCode></BaseAirport><DetailedIdentity><AirlineIATACode>JD</AirlineIATACode><AirlineICAOCode>CBJ</AirlineICAOCode></DetailedIdentity><InternalID>1003782389</InternalID></LinkFlight><SlaveFlight><FlightIdentity/></SlaveFlight><FreeTextComment><PublicTextComment xsi:nil=\\\"true\\\"/></FreeTextComment></General><OperationalDateTime><PreviousAirportDepartureDateTime><ScheduledPreviousAirportDepartureDateTime>2023-03-30T23:20:00</ScheduledPreviousAirportDepartureDateTime><EstimatedPreviousAirportDepartureDateTime xsi:nil=\\\"true\\\"/><ActualPreviousAirportDepartureDateTime>2023-03-30T23:24:00</ActualPreviousAirportDepartureDateTime></PreviousAirportDepartureDateTime><LandingDateTime><ScheduledLandingDateTime>2023-03-31T00:35:00</ScheduledLandingDateTime><EstimatedLandingDateTime OldValue=\\\"2023-03-31T00:15:00\\\">2023-03-31T00:10:00</EstimatedLandingDateTime><ActualLandingDateTime xsi:nil=\\\"true\\\"/></LandingDateTime><OnBlockDateTime><ScheduledOnBlockDateTime>2023-03-31T00:40:00</ScheduledOnBlockDateTime><EstimatedOnBlockDateTime OldValue=\\\"2023-03-31T00:20:00\\\">2023-03-31T00:15:00</EstimatedOnBlockDateTime><ActualOnBlockDateTime xsi:nil=\\\"true\\\"/></OnBlockDateTime></OperationalDateTime><Status><OperationStatus/><FlightStatus>P</FlightStatus><DelayReason><DelayCode/><DelayFreeText/></DelayReason><DiversionAirport><AirportIATACode/><AirportICAOCode/></DiversionAirport><ChangeLandingAirport><AirportIATACode/><AirportICAOCode/></ChangeLandingAirport></Status><Airport><Terminal><FlightTerminalID>01</FlightTerminalID><AircraftTerminalID/></Terminal><Stand><StandID>25</StandID></Stand><GroundMovement><StandID>25</StandID><ScheduledStandStartDateTime OldValue=\\\"2023-03-31T00:20:00\\\">2023-03-31T00:15:00</ScheduledStandStartDateTime><ScheduledStandEndDateTime>2023-03-31T10:20:00</ScheduledStandEndDateTime><ActualStandStartDateTime xsi:nil=\\\"true\\\"/><ActualStandEndDateTime xsi:nil=\\\"true\\\"/></GroundMovement><Gate><GateID/><GateStatus xsi:nil=\\\"true\\\"/><ScheduledGateStartDateTime xsi:nil=\\\"true\\\"/><ScheduledGateEndDateTime xsi:nil=\\\"true\\\"/><ActualGateStartDateTime xsi:nil=\\\"true\\\"/><ActualGateEndDateTime xsi:nil=\\\"true\\\"/><BoardingStartDateTime xsi:nil=\\\"true\\\"/><LastCallDateTime xsi:nil=\\\"true\\\"/><BoardingEndDateTime xsi:nil=\\\"true\\\"/></Gate><BaggageReclaim><BaggageReclaimID>3</BaggageReclaimID><ScheduledReclaimStartDateTime OldValue=\\\"2023-03-31T00:30:00\\\">2023-03-31T00:25:00</ScheduledReclaimStartDateTime><ScheduledReclaimEndDateTime OldValue=\\\"2023-03-31T00:55:00\\\">2023-03-31T00:50:00</ScheduledReclaimEndDateTime><ActualReclaimStartDateTime xsi:nil=\\\"true\\\"/><ActualReclaimEndDateTime xsi:nil=\\\"true\\\"/><FirstBaggageDateTime xsi:nil=\\\"true\\\"/><LastBaggageDateTime xsi:nil=\\\"true\\\"/></BaggageReclaim></Airport></FlightData></Data></IMFRoot>";

        String uuid = IdGenerator.uuid();
        String demo3 = demo1 + uuid + demo2;

        String xml2 = StringEscapeUtils.unescapeJson(demo3);

        PulsarData data =
            PulsarData.builder().data(xml2).sendTime(now).eventTime(now).operate("INSERT").build();
        String s = OM.writeValueAsString(data);
        messages.add(s);
      }
      for (String message : messages) {
        producer.newMessage().value(message.getBytes(StandardCharsets.UTF_8)).sendAsync();
      }
    }

    producer.close();
    client.close();
  }

  @Test
  public void testProducer4() throws Exception {
    ClientBuilder builder = PulsarClient.builder().serviceUrl(brokerServiceUrl);
    if (StringUtils.isNotBlank(token)) {
      builder.authentication(AuthenticationFactory.token(token));
    }
    PulsarClient client = builder.build();
    ProducerImpl<byte[]> producer =
        (ProducerImpl<byte[]>)
            client
                .newProducer()
                .topic("persistent://public/default/319wxg_10w_5")
                .enableBatching(
                    true) // 是否开启批量处理消息，默认true,需要注意的是enableBatching只在异步发送sendAsync生效，同步发送send失效。因此建议生产环境若想使用批处理，则需使用异步发送，或者多线程同步发送
                .compressionType(
                    CompressionType
                        .LZ4) // 消息压缩（四种压缩方式：LZ4，ZLIB，ZSTD，SNAPPY），consumer端不用做改动就能消费，开启后大约可以降低3/4带宽消耗和存储（官方测试）
                .sendTimeout(
                    0,
                    TimeUnit
                        .SECONDS) // 设置发送超时0s；如果在sendTimeout过期之前服务器没有确认消息，则会发生错误。默认30s，设置为0代表无限制，建议配置为0
                .batchingMaxMessages(1000) // 批处理中允许的最大消息数。默认1000
                .maxPendingMessages(1000) // 设置等待接受来自broker确认消息的队列的最大大小，默认1000
                .blockIfQueueFull(true) // 设置当消息队列中等待的消息已满时，Producer.send 和 Producer.sendAsync
                // 是否应该block阻塞。默认为false，达到maxPendingMessages后send操作会报错，设置为true后，send操作阻塞但是不报错。建议设置为true
                .batcherBuilder(
                    BatcherBuilder.DEFAULT) // key_Shared模式要用KEY_BASED,才能保证同一个key的message在一个batch里
                //                                .intercept(new MyPulsarProducerInterceptor())
                .create();

    int count = 1;
    for (int j = 0; j < 100; j++) {
      List<String> messages = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {

        String json = "{\"sequenceId\":\"" + count + "\"}";
        count++;
        messages.add(json);
        System.out.println(count);
      }
      for (String message : messages) {
        producer.newMessage().value(message.getBytes(StandardCharsets.UTF_8)).sendAsync();
      }
    }

    producer.close();
    client.close();
  }

  @Test
  public void testProducer5() throws Exception {
    ClientBuilder builder = PulsarClient.builder().serviceUrl(brokerServiceUrl);
    if (StringUtils.isNotBlank(token)) {
      builder.authentication(AuthenticationFactory.token(token));
    }
    PulsarClient client = builder.build();
    ProducerImpl<byte[]> producer =
        (ProducerImpl<byte[]>)
            client
                .newProducer()
                .topic("persistent://public/default/320_1to2_3")
                .enableBatching(
                    true) // 是否开启批量处理消息，默认true,需要注意的是enableBatching只在异步发送sendAsync生效，同步发送send失效。因此建议生产环境若想使用批处理，则需使用异步发送，或者多线程同步发送
                .compressionType(
                    CompressionType
                        .LZ4) // 消息压缩（四种压缩方式：LZ4，ZLIB，ZSTD，SNAPPY），consumer端不用做改动就能消费，开启后大约可以降低3/4带宽消耗和存储（官方测试）
                .sendTimeout(
                    0,
                    TimeUnit
                        .SECONDS) // 设置发送超时0s；如果在sendTimeout过期之前服务器没有确认消息，则会发生错误。默认30s，设置为0代表无限制，建议配置为0
                .batchingMaxMessages(1000) // 批处理中允许的最大消息数。默认1000
                .maxPendingMessages(1000) // 设置等待接受来自broker确认消息的队列的最大大小，默认1000
                .blockIfQueueFull(true) // 设置当消息队列中等待的消息已满时，Producer.send 和 Producer.sendAsync
                // 是否应该block阻塞。默认为false，达到maxPendingMessages后send操作会报错，设置为true后，send操作阻塞但是不报错。建议设置为true
                .batcherBuilder(
                    BatcherBuilder.DEFAULT) // key_Shared模式要用KEY_BASED,才能保证同一个key的message在一个batch里
                //                                .intercept(new MyPulsarProducerInterceptor())
                .create();

    int count = 1;
    for (int j = 0; j < 10; j++) {
      List<String> messages = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {

        String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        String json =
            "[{\"id\":131820526298232442,\"name\":\"DsVEgU\"},{\"id\":914710197443125579,\"name\":\"hWEfQA\"},{\"id\":6118079490213257348,\"name\":\"KAbDEk\"}]";
        String jsonlist = StringEscapeUtils.unescapeJson(json);

        PulsarData data =
            PulsarData.builder()
                .data(jsonlist)
                .sendTime(now)
                .eventTime(now)
                .operate("INSERT")
                .build();
        String s = OM.writeValueAsString(data);
        messages.add(s);
      }
      for (String message : messages) {
        producer.newMessage().value(message.getBytes(StandardCharsets.UTF_8)).sendAsync();
      }
    }

    producer.close();
    client.close();
  }

  @Test
  public void testConsumer() throws Exception {
    ClientBuilder builder = PulsarClient.builder().serviceUrl(brokerServiceUrl);
    if (StringUtils.isNotBlank(token)) {
      builder.authentication(AuthenticationFactory.token(token));
    }
    PulsarClient client = builder.build();
    ConsumerImpl<byte[]> consumer =
        (ConsumerImpl<byte[]>)
            client
                .newConsumer()
                .topic("persistent://public/default/test-pulsar-sink-2")
                .subscriptionName("wxg_test_1")
                .subscriptionType(
                    SubscriptionType
                        .Exclusive) // 指定消费模式，包含：Exclusive，Failover，Shared，Key_Shared。默认Exclusive模式
                .subscriptionInitialPosition(
                    SubscriptionInitialPosition.Earliest) // 指定从哪里开始消费，还有Latest，valueof可选，默认Latest
                .negativeAckRedeliveryDelay(
                    60, TimeUnit.SECONDS) // 指定消费失败后延迟多久broker重新发送消息给consumer，默认60s
                //                                .intercept(new MyPulsarConsumerInterceptor())
                .subscribe();
    int i = 0;
    int h = 0;
    List<Message<byte[]>> messages = new ArrayList<>(100);
    try {
      while (true) {
        Message<byte[]> message = consumer.receive();
        messages.add(message);
        System.out.printf(
            "consumer name: %s, topic: %s, subscription: %s, message_id: %s, message: %s\n",
            consumer.getConsumerName(),
            consumer.getTopic(),
            consumer.getSubscription(),
            ((MessageIdImpl) message.getMessageId()).getLedgerId(),
            new String(message.getValue(), StandardCharsets.UTF_8));
        if (i == 99) {
          consumer.acknowledgeCumulative(message);
          i = 0;
          messages.clear();
          System.out.println("处理批次" + h);
          h++;
        } else {
          i++;
        }
      }
    } catch (Exception e) {
      for (Message<byte[]> message : messages) {
        consumer.negativeAcknowledge(message);
      }
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testConsumerListener() throws Exception {
    ClientBuilder builder = PulsarClient.builder().serviceUrl(brokerServiceUrl);
    if (StringUtils.isNotBlank(token)) {
      builder.authentication(AuthenticationFactory.token(token));
    }
    PulsarClient client = builder.build();
    ConsumerImpl<byte[]> consumer =
        (ConsumerImpl<byte[]>)
            client
                .newConsumer()
                .topic("pulsar_callback_test")
                .subscriptionName("pulsar_callback_test_listener")
                .subscriptionType(
                    SubscriptionType
                        .Shared) // 指定消费模式，包含：Exclusive，Failover，Shared，Key_Shared。默认Exclusive模式
                .subscriptionInitialPosition(
                    SubscriptionInitialPosition.Earliest) // 指定从哪里开始消费，还有Latest，valueof可选，默认Latest
                .negativeAckRedeliveryDelay(
                    60, TimeUnit.SECONDS) // 指定消费失败后延迟多久broker重新发送消息给consumer，默认60s
                .batchReceivePolicy(
                    BatchReceivePolicy.builder()
                        .maxNumMessages(1000)
                        .maxNumBytes(1024 * 1024)
                        .timeout(500, TimeUnit.MILLISECONDS)
                        .build())
                //                                .intercept(new MyPulsarConsumerInterceptor())
                .messageListener(
                    (MessageListener<byte[]>)
                        (consumer1, msg) ->
                            System.out.printf(
                                "consumer name: %s, topic: %s, subscription: %s, message_id: %s, message: %s\n",
                                consumer1.getConsumerName(),
                                consumer1.getTopic(),
                                consumer1.getSubscription(),
                                ((MessageIdImpl) msg.getMessageId()).getLedgerId(),
                                new String(msg.getValue(), StandardCharsets.UTF_8)))
                .subscribe();
    while (true) {}
  }

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Item {
    private long id;
    private String name;
    private String goods;
  }

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Good {
    private long id;
    private String name;
  }

  @Test
  public void generate() {
    for (int i = 0; i < 1000; i++) {
      Item item =
          Item.builder()
              .id(RandomUtils.nextLong())
              .name(RandomStringUtils.randomAlphabetic(6))
              .goods(
                  JsonUtils.toJson(
                          Arrays.asList(
                              Good.builder()
                                  .id(RandomUtils.nextLong())
                                  .name(RandomStringUtils.randomAlphabetic(6))
                                  .build(),
                              Good.builder()
                                  .id(RandomUtils.nextLong())
                                  .name(RandomStringUtils.randomAlphabetic(6))
                                  .build(),
                              Good.builder()
                                  .id(RandomUtils.nextLong())
                                  .name(RandomStringUtils.randomAlphabetic(6))
                                  .build()))
                      .get())
              .build();
      String s = JsonUtils.toJson(item).get();
      System.out.println(s);
    }
  }
}
