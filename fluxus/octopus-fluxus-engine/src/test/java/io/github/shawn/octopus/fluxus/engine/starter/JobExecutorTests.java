package io.github.shawn.octopus.fluxus.engine.starter;

import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.io.Resources;
import com.sun.net.httpserver.HttpServer;
import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContext;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContextManagement;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import org.apache.commons.lang3.StringEscapeUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JobExecutorTests {

  @Test
  public void base64() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/test/pulsar-streaming-job7.json")),
            StandardCharsets.UTF_8);
    String s = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    System.out.println(s);
  }

  @Test
  public void testSimpleBatchJob() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/test/pulsar-streaming-job5.json")),
            StandardCharsets.UTF_8);
    String s = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    PrometheusMeterRegistry prometheusRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    Metrics.globalRegistry.add(prometheusRegistry);
    try {
      HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
      server.createContext(
          "/prometheus",
          httpExchange -> {
            String response = prometheusRegistry.scrape();
            httpExchange.sendResponseHeaders(200, response.getBytes().length);
            try (OutputStream os = httpExchange.getResponseBody()) {
              os.write(response.getBytes());
            }
          });

      new Thread(server::start).start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    JobExecutor jobExecutor = new JobExecutor(s);
    Assertions.assertNotNull(jobExecutor);
    jobExecutor.execute();
    jobExecutor.await();
    JobContext job = JobContextManagement.getJob();

    Thread.sleep(100000);
    assertNull(job);
  }

  @Test
  public void testSimpleStreamingJob() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/test/pulsar-streaming-job5.json")),
            StandardCharsets.UTF_8);
    String s = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    PrometheusMeterRegistry prometheusRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    Metrics.globalRegistry.add(prometheusRegistry);
    try {
      HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
      server.createContext(
          "/prometheus",
          httpExchange -> {
            String response = prometheusRegistry.scrape();
            httpExchange.sendResponseHeaders(200, response.getBytes().length);
            try (OutputStream os = httpExchange.getResponseBody()) {
              os.write(response.getBytes());
            }
          });

      new Thread(server::start).start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    JobExecutor jobExecutor = new JobExecutor(s);
    Assertions.assertNotNull(jobExecutor);
    jobExecutor.execute();
    jobExecutor.await();
  }

  @Test
  public void testSimpleBatchJobWithNoRegistry() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/job/simple-batch-job.json")),
            StandardCharsets.UTF_8);
    String s = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    JobExecutor jobExecutor = new JobExecutor(s);
    Assertions.assertNotNull(jobExecutor);
    jobExecutor.execute();
    jobExecutor.await();
    JobContext job = JobContextManagement.getJob();
    assertNull(job);
  }

  /** xml解析任务测试 */
  @Test
  void testSimpleBatchJobWithXML() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/job/simple-batch-job-xml.json")),
            StandardCharsets.UTF_8);
    String s = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    JobExecutor jobExecutor = new JobExecutor(s);
    Assertions.assertNotNull(jobExecutor);
    jobExecutor.execute();
    jobExecutor.await();
    JobContext job = JobContextManagement.getJob();
    assertNull(job);
  }

  /** xml解析-expression任务测试 */
  @Test
  void testSimpleBatchJobWithExpression() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/job/simple-batch-job-expression.json")),
            StandardCharsets.UTF_8);
    String s = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    JobExecutor jobExecutor = new JobExecutor(s);
    Assertions.assertNotNull(jobExecutor);
    jobExecutor.execute();
    jobExecutor.await();
    JobContext job = JobContextManagement.getJob();
    assertNull(job);
  }

  @Test
  void base64test() throws Exception {

    byte[] data =
        Base64.getDecoder()
            .decode(
                "ew0KICAiam9iTmFtZSI6ICJwdWxzYXItam9iLTIwMHciLA0KICAiam9iTW9kZSI6ICJTVFJFQU1JTkciLA0KICAic291cmNlcyI6IFsNCiAgICB7DQogICAgICAidHlwZSI6ICJwdWxzYXIiLA0KICAgICAgIm5hbWUiOiAicHVsc2FyIiwNCiAgICAgICJvdXRwdXQiOiAicHVsc2FyLW91dHB1dCIsDQogICAgICAib3B0aW9ucyI6IHsNCiAgICAgICAgImNsaWVudFVybCI6ICJwdWxzYXI6Ly8xOTIuMTY4LjU0LjIwNjo2NjUwLDE5Mi4xNjguNTQuMjA3OjY2NTAsMTkyLjE2OC41NC4yMDg6NjY1MCIsDQogICAgICAgICJhZG1pblVybCI6ICJodHRwOi8vMTkyLjE2OC41NC4yMDY6ODA4MCwxOTIuMTY4LjU0LjIwNzo4MDgwLDE5Mi4xNjguNTQuMjA4OjgwODAiLA0KICAgICAgICAidG9rZW4iOiAiZXlKaGJHY2lPaUpJVXpJMU5pSjkuZXlKemRXSWlPaUpoWkcxcGJpSjkueVkyTVNUQlNFazNXa0wzeUVmU1VkeXEtRWJlTzFMeWc1RUcxS2l1R080TSIsDQogICAgICAgICJ0b3BpYyI6ICJ0b3BpYy0yMDB3IiwNCiAgICAgICAgInN1YnNjcmlwdGlvbiI6ICJ0b3BpYy0yMDB3IiwNCiAgICAgICAgInN1YnNjcmlwdGlvblR5cGUiOiAiRWFybGllc3QiLA0KICAgICAgICAiY29tbWl0U2l6ZSI6IDEwMDANCiAgICAgIH0sDQogICAgICAiY29sdW1ucyI6IFsNCiAgICAgICAgew0KICAgICAgICAgICJuYW1lIjogInNlbmRUaW1lIiwNCiAgICAgICAgICAidHlwZSI6ICJ2YXJjaGFyIiwNCiAgICAgICAgICAibnVsbGFibGUiOiBmYWxzZSwNCiAgICAgICAgICAibGVuZ3RoIjogMTAwLA0KICAgICAgICAgICJjb21tZW50IjogIuS4u+mUriIsDQogICAgICAgICAgImRlZmF1bHRWYWx1ZSI6IC0xDQogICAgICAgIH0sDQogICAgICAgIHsNCiAgICAgICAgICAibmFtZSI6ICJldmVudFRpbWUiLA0KICAgICAgICAgICJ0eXBlIjogInZhcmNoYXIiLA0KICAgICAgICAgICJudWxsYWJsZSI6IHRydWUsDQogICAgICAgICAgImxlbmd0aCI6IDEwMCwNCiAgICAgICAgICAiY29tbWVudCI6ICLnlKjmiLflkI0iLA0KICAgICAgICAgICJkZWZhdWx0VmFsdWUiOiBudWxsDQogICAgICAgIH0sDQogICAgICAgIHsNCiAgICAgICAgICAibmFtZSI6ICJvcGVyYXRlIiwNCiAgICAgICAgICAidHlwZSI6ICJ2YXJjaGFyIiwNCiAgICAgICAgICAibnVsbGFibGUiOiB0cnVlLA0KICAgICAgICAgICJsZW5ndGgiOiAxMDAsDQogICAgICAgICAgImNvbW1lbnQiOiAi55So5oi35ZCNIiwNCiAgICAgICAgICAiZGVmYXVsdFZhbHVlIjogbnVsbA0KICAgICAgICB9LA0KICAgICAgICB7DQogICAgICAgICAgIm5hbWUiOiAiZGF0YSIsDQogICAgICAgICAgInR5cGUiOiAidmFyY2hhciIsDQogICAgICAgICAgIm51bGxhYmxlIjogdHJ1ZSwNCiAgICAgICAgICAibGVuZ3RoIjogMTAwLA0KICAgICAgICAgICJjb21tZW50IjogIueUqOaIt+WQjSIsDQogICAgICAgICAgImRlZmF1bHRWYWx1ZSI6IG51bGwNCiAgICAgICAgfQ0KICAgICAgXQ0KICAgIH0NCiAgXSwNCiAgInRyYW5zZm9ybXMiOiBbDQogICAgew0KICAgICAgInR5cGUiOiAieG1sLXBhcnNlIiwNCiAgICAgICJuYW1lIjogInhtbC1wYXJzZSIsDQogICAgICAiaW5wdXRzIjogWw0KICAgICAgICAicHVsc2FyLW91dHB1dCINCiAgICAgIF0sDQogICAgICAib3V0cHV0IjogInhtbC1vdXRwdXQiLA0KICAgICAgIm9wdGlvbnMiOiB7DQogICAgICAgICJ2YWx1ZUZpZWxkIjogImRhdGEiLA0KICAgICAgICAieG1sUGFyc2VGaWVsZHMiOiBbDQogICAgICAgICAgew0KICAgICAgICAgICAgImRlc3RpbmF0aW9uIjogImlkIiwNCiAgICAgICAgICAgICJzb3VyY2VQYXRoIjogIi9Cb2R5L1JlY29yZC9pZCIsDQogICAgICAgICAgICAidHlwZSI6ICJpbnQiDQogICAgICAgICAgfSwNCiAgICAgICAgICB7DQogICAgICAgICAgICAiZGVzdGluYXRpb24iOiAibmFtZSIsDQogICAgICAgICAgICAic291cmNlUGF0aCI6ICIvQm9keS9SZWNvcmQvbmFtZSIsDQogICAgICAgICAgICAidHlwZSI6ICJTdHJpbmciDQogICAgICAgICAgfSwNCiAgICAgICAgICB7DQogICAgICAgICAgICAiZGVzdGluYXRpb24iOiAiYWdlIiwNCiAgICAgICAgICAgICJzb3VyY2VQYXRoIjogIi9Cb2R5L1JlY29yZC9hZ2UiLA0KICAgICAgICAgICAgInR5cGUiOiAiaW50Ig0KICAgICAgICAgIH0sDQogICAgICAgICAgew0KICAgICAgICAgICAgImRlc3RpbmF0aW9uIjogImNyZWF0ZVRpbWUiLA0KICAgICAgICAgICAgInNvdXJjZVBhdGgiOiAiL0JvZHkvUmVjb3JkL2NyZWF0ZVRpbWUiLA0KICAgICAgICAgICAgInR5cGUiOiAiRGF0ZVRpbWUiDQogICAgICAgICAgfSwNCiAgICAgICAgICB7DQogICAgICAgICAgICAiZGVzdGluYXRpb24iOiAidXBkYXRlVGltZSIsDQogICAgICAgICAgICAic291cmNlUGF0aCI6ICIvQm9keS9SZWNvcmQvdXBkYXRlVGltZSIsDQogICAgICAgICAgICAidHlwZSI6ICJEYXRlVGltZSINCiAgICAgICAgICB9DQogICAgICAgIF0NCiAgICAgIH0NCiAgICB9DQogIF0sDQogICJzaW5rIjogew0KICAgICJ0eXBlIjogImpkYmMiLA0KICAgICJuYW1lIjogImpkYmMiLA0KICAgICJpbnB1dCI6ICJ4bWwtb3V0cHV0IiwNCiAgICAib3B0aW9ucyI6IHsNCiAgICAgICJ1cmwiOiAiamRiYzpteXNxbDovLzE5Mi4xNjguNTQuMjA2OjMzMDYvdGVzdD9jaGFyYWN0ZXJFbmNvZGluZz11dGYtOCZ1c2VTU0w9ZmFsc2UmYWxsb3dQdWJsaWNLZXlSZXRyaWV2YWw9dHJ1ZSIsDQogICAgICAidXNlcm5hbWUiOiAicm9vdCIsDQogICAgICAicGFzc3dvcmQiOiAiYmlnZGF0YTMyMSIsDQogICAgICAiZHJpdmVyIjogImNvbS5teXNxbC5jai5qZGJjLkRyaXZlciIsDQogICAgICAiZGF0YWJhc2UiOiAidGVzdCIsDQogICAgICAidGFibGUiOiAidXNlcl8yMDB3IiwNCiAgICAgICJiYXRjaFNpemUiOiAxMDAwLA0KICAgICAgIm1vZGUiOiAiSU5TRVJUIiwNCiAgICAgICJmaWVsZHMiOiBbDQogICAgICAgICJuYW1lIiwNCiAgICAgICAgImFnZSIsDQogICAgICAgICJjcmVhdGVUaW1lIiwNCiAgICAgICAgInVwZGF0ZVRpbWUiDQogICAgICBdDQogICAgfQ0KICB9DQp9");
    String s = new String(data, StandardCharsets.UTF_8);
    System.out.println(s);
  }

  @Test
  void base64encode() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/test/pulsar-streaming-job5.json")),
            StandardCharsets.UTF_8);
    String s = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    System.out.println(s);
  }

  @Test
  void testXml() throws Exception {

    String xmldemo =
        "{\n"
            + "    \"operate\": \"insert\",\n"
            + "    \"senderSysCode\": \"AODB\",\n"
            + "    \"senderSysName\": \"test1\",\n"
            + "    \"seqNum\": \"1\",\n"
            + "    \"datas\": [\n"
            + "        \"<IMFRoot xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\"><SysInfo><MessageSequenceID>109882</MessageSequenceID><MessageType>FlightData</MessageType><ServiceType>FSS1</ServiceType><OperationMode>MOD</OperationMode><SendDateTime>2023-03-31T00:03:05.818</SendDateTime><CreateDateTime>2023-03-31T00:03:05.795</CreateDateTime><OriginalDateTime>2023-03-31T00:03:05</OriginalDateTime><Receiver>WX</Receiver><Sender>IMF</Sender><Owner>AODB</Owner></SysInfo><Data><PrimaryKey><FlightKey><FlightScheduledDate>2023-03-31</FlightScheduledDate><FlightIdentity>JD5530</FlightIdentity><FlightDirection>A</FlightDirection><BaseAirport><AirportIATACode>HAK</AirportIATACode><AirportICAOCode>ZJHK</AirportICAOCode></BaseAirport><DetailedIdentity><AirlineIATACode>JD</AirlineIATACode><AirlineICAOCode>CBJ</AirlineICAOCode></DetailedIdentity><InternalID>1003782114</InternalID></FlightKey></PrimaryKey><FlightData><General><FlightScheduledDateTime>2023-03-31T00:35:00</FlightScheduledDateTime><Registration>B6723</Registration><CallSign>CBJ5530</CallSign><AircraftType><AircraftIATACode>320</AircraftIATACode><AircraftICAOCode>A320</AircraftICAOCode></AircraftType><FlightServiceType><FlightCAACServiceType>W/Z</FlightCAACServiceType><FlightIATAServiceType>J</FlightIATAServiceType></FlightServiceType><FlightRoute><IATARoute><IATAOriginAirport>NNG</IATAOriginAirport><IATAPreviousAirport>NNG</IATAPreviousAirport><IATAFullRoute><AirportIATACode LegNo=\\\"1\\\">NNG</AirportIATACode></IATAFullRoute></IATARoute><ICAORoute><ICAOOriginAirport>ZGNN</ICAOOriginAirport><ICAOPreviousAirport>ZGNN</ICAOPreviousAirport><ICAOFullRoute><AirportICAOCode LegNo=\\\"1\\\">ZGNN</AirportICAOCode></ICAOFullRoute></ICAORoute></FlightRoute><FlightCountryType>D</FlightCountryType><LinkFlight><FlightScheduledDate>2023-03-31</FlightScheduledDate><FlightIdentity>JD5891</FlightIdentity><FlightDirection>D</FlightDirection><BaseAirport><AirportIATACode>HAK</AirportIATACode><AirportICAOCode>ZJHK</AirportICAOCode></BaseAirport><DetailedIdentity><AirlineIATACode>JD</AirlineIATACode><AirlineICAOCode>CBJ</AirlineICAOCode></DetailedIdentity><InternalID>1003782389</InternalID></LinkFlight><SlaveFlight><FlightIdentity/></SlaveFlight><FreeTextComment><PublicTextComment xsi:nil=\\\"true\\\"/></FreeTextComment></General><OperationalDateTime><PreviousAirportDepartureDateTime><ScheduledPreviousAirportDepartureDateTime>2023-03-30T23:20:00</ScheduledPreviousAirportDepartureDateTime><EstimatedPreviousAirportDepartureDateTime xsi:nil=\\\"true\\\"/><ActualPreviousAirportDepartureDateTime>2023-03-30T23:24:00</ActualPreviousAirportDepartureDateTime></PreviousAirportDepartureDateTime><LandingDateTime><ScheduledLandingDateTime>2023-03-31T00:35:00</ScheduledLandingDateTime><EstimatedLandingDateTime OldValue=\\\"2023-03-31T00:15:00\\\">2023-03-31T00:10:00</EstimatedLandingDateTime><ActualLandingDateTime xsi:nil=\\\"true\\\"/></LandingDateTime><OnBlockDateTime><ScheduledOnBlockDateTime>2023-03-31T00:40:00</ScheduledOnBlockDateTime><EstimatedOnBlockDateTime OldValue=\\\"2023-03-31T00:20:00\\\">2023-03-31T00:15:00</EstimatedOnBlockDateTime><ActualOnBlockDateTime xsi:nil=\\\"true\\\"/></OnBlockDateTime></OperationalDateTime><Status><OperationStatus/><FlightStatus>P</FlightStatus><DelayReason><DelayCode/><DelayFreeText/></DelayReason><DiversionAirport><AirportIATACode/><AirportICAOCode/></DiversionAirport><ChangeLandingAirport><AirportIATACode/><AirportICAOCode/></ChangeLandingAirport></Status><Airport><Terminal><FlightTerminalID>01</FlightTerminalID><AircraftTerminalID/></Terminal><Stand><StandID>25</StandID></Stand><GroundMovement><StandID>25</StandID><ScheduledStandStartDateTime OldValue=\\\"2023-03-31T00:20:00\\\">2023-03-31T00:15:00</ScheduledStandStartDateTime><ScheduledStandEndDateTime>2023-03-31T10:20:00</ScheduledStandEndDateTime><ActualStandStartDateTime xsi:nil=\\\"true\\\"/><ActualStandEndDateTime xsi:nil=\\\"true\\\"/></GroundMovement><Gate><GateID/><GateStatus xsi:nil=\\\"true\\\"/><ScheduledGateStartDateTime xsi:nil=\\\"true\\\"/><ScheduledGateEndDateTime xsi:nil=\\\"true\\\"/><ActualGateStartDateTime xsi:nil=\\\"true\\\"/><ActualGateEndDateTime xsi:nil=\\\"true\\\"/><BoardingStartDateTime xsi:nil=\\\"true\\\"/><LastCallDateTime xsi:nil=\\\"true\\\"/><BoardingEndDateTime xsi:nil=\\\"true\\\"/></Gate><BaggageReclaim><BaggageReclaimID>3</BaggageReclaimID><ScheduledReclaimStartDateTime OldValue=\\\"2023-03-31T00:30:00\\\">2023-03-31T00:25:00</ScheduledReclaimStartDateTime><ScheduledReclaimEndDateTime OldValue=\\\"2023-03-31T00:55:00\\\">2023-03-31T00:50:00</ScheduledReclaimEndDateTime><ActualReclaimStartDateTime xsi:nil=\\\"true\\\"/><ActualReclaimEndDateTime xsi:nil=\\\"true\\\"/><FirstBaggageDateTime xsi:nil=\\\"true\\\"/><LastBaggageDateTime xsi:nil=\\\"true\\\"/></BaggageReclaim></Airport></FlightData></Data></IMFRoot>\"\n"
            + "    ],\n"
            + "    \"dataType\": \"FlightData\",\n"
            + "    \"sendTime\": \"2023-03-31 00:03:05\"\n"
            + "}";

    String demo =
        "<IMFRoot xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\"><SysInfo><MessageSequenceID>109882</MessageSequenceID><MessageType>FlightData</MessageType><ServiceType>FSS1</ServiceType><OperationMode>MOD</OperationMode><SendDateTime>2023-03-31T00:03:05.818</SendDateTime><CreateDateTime>2023-03-31T00:03:05.795</CreateDateTime><OriginalDateTime>2023-03-31T00:03:05</OriginalDateTime><Receiver>WX</Receiver><Sender>IMF</Sender><Owner>AODB</Owner></SysInfo><Data><PrimaryKey><FlightKey><FlightScheduledDate>2023-03-31</FlightScheduledDate><FlightIdentity>JD5530</FlightIdentity><FlightDirection>A</FlightDirection><BaseAirport><AirportIATACode>HAK</AirportIATACode><AirportICAOCode>ZJHK</AirportICAOCode></BaseAirport><DetailedIdentity><AirlineIATACode>JD</AirlineIATACode><AirlineICAOCode>CBJ</AirlineICAOCode></DetailedIdentity><InternalID>1003782114</InternalID></FlightKey></PrimaryKey><FlightData><General><FlightScheduledDateTime>2023-03-31T00:35:00</FlightScheduledDateTime><Registration>B6723</Registration><CallSign>CBJ5530</CallSign><AircraftType><AircraftIATACode>320</AircraftIATACode><AircraftICAOCode>A320</AircraftICAOCode></AircraftType><FlightServiceType><FlightCAACServiceType>W/Z</FlightCAACServiceType><FlightIATAServiceType>J</FlightIATAServiceType></FlightServiceType><FlightRoute><IATARoute><IATAOriginAirport>NNG</IATAOriginAirport><IATAPreviousAirport>NNG</IATAPreviousAirport><IATAFullRoute><AirportIATACode LegNo=\\\"1\\\">NNG</AirportIATACode></IATAFullRoute></IATARoute><ICAORoute><ICAOOriginAirport>ZGNN</ICAOOriginAirport><ICAOPreviousAirport>ZGNN</ICAOPreviousAirport><ICAOFullRoute><AirportICAOCode LegNo=\\\"1\\\">ZGNN</AirportICAOCode></ICAOFullRoute></ICAORoute></FlightRoute><FlightCountryType>D</FlightCountryType><LinkFlight><FlightScheduledDate>2023-03-31</FlightScheduledDate><FlightIdentity>JD5891</FlightIdentity><FlightDirection>D</FlightDirection><BaseAirport><AirportIATACode>HAK</AirportIATACode><AirportICAOCode>ZJHK</AirportICAOCode></BaseAirport><DetailedIdentity><AirlineIATACode>JD</AirlineIATACode><AirlineICAOCode>CBJ</AirlineICAOCode></DetailedIdentity><InternalID>1003782389</InternalID></LinkFlight><SlaveFlight><FlightIdentity/></SlaveFlight><FreeTextComment><PublicTextComment xsi:nil=\\\"true\\\"/></FreeTextComment></General><OperationalDateTime><PreviousAirportDepartureDateTime><ScheduledPreviousAirportDepartureDateTime>2023-03-30T23:20:00</ScheduledPreviousAirportDepartureDateTime><EstimatedPreviousAirportDepartureDateTime xsi:nil=\\\"true\\\"/><ActualPreviousAirportDepartureDateTime>2023-03-30T23:24:00</ActualPreviousAirportDepartureDateTime></PreviousAirportDepartureDateTime><LandingDateTime><ScheduledLandingDateTime>2023-03-31T00:35:00</ScheduledLandingDateTime><EstimatedLandingDateTime OldValue=\\\"2023-03-31T00:15:00\\\">2023-03-31T00:10:00</EstimatedLandingDateTime><ActualLandingDateTime xsi:nil=\\\"true\\\"/></LandingDateTime><OnBlockDateTime><ScheduledOnBlockDateTime>2023-03-31T00:40:00</ScheduledOnBlockDateTime><EstimatedOnBlockDateTime OldValue=\\\"2023-03-31T00:20:00\\\">2023-03-31T00:15:00</EstimatedOnBlockDateTime><ActualOnBlockDateTime xsi:nil=\\\"true\\\"/></OnBlockDateTime></OperationalDateTime><Status><OperationStatus/><FlightStatus>P</FlightStatus><DelayReason><DelayCode/><DelayFreeText/></DelayReason><DiversionAirport><AirportIATACode/><AirportICAOCode/></DiversionAirport><ChangeLandingAirport><AirportIATACode/><AirportICAOCode/></ChangeLandingAirport></Status><Airport><Terminal><FlightTerminalID>01</FlightTerminalID><AircraftTerminalID/></Terminal><Stand><StandID>25</StandID></Stand><GroundMovement><StandID>25</StandID><ScheduledStandStartDateTime OldValue=\\\"2023-03-31T00:20:00\\\">2023-03-31T00:15:00</ScheduledStandStartDateTime><ScheduledStandEndDateTime>2023-03-31T10:20:00</ScheduledStandEndDateTime><ActualStandStartDateTime xsi:nil=\\\"true\\\"/><ActualStandEndDateTime xsi:nil=\\\"true\\\"/></GroundMovement><Gate><GateID/><GateStatus xsi:nil=\\\"true\\\"/><ScheduledGateStartDateTime xsi:nil=\\\"true\\\"/><ScheduledGateEndDateTime xsi:nil=\\\"true\\\"/><ActualGateStartDateTime xsi:nil=\\\"true\\\"/><ActualGateEndDateTime xsi:nil=\\\"true\\\"/><BoardingStartDateTime xsi:nil=\\\"true\\\"/><LastCallDateTime xsi:nil=\\\"true\\\"/><BoardingEndDateTime xsi:nil=\\\"true\\\"/></Gate><BaggageReclaim><BaggageReclaimID>3</BaggageReclaimID><ScheduledReclaimStartDateTime OldValue=\\\"2023-03-31T00:30:00\\\">2023-03-31T00:25:00</ScheduledReclaimStartDateTime><ScheduledReclaimEndDateTime OldValue=\\\"2023-03-31T00:55:00\\\">2023-03-31T00:50:00</ScheduledReclaimEndDateTime><ActualReclaimStartDateTime xsi:nil=\\\"true\\\"/><ActualReclaimEndDateTime xsi:nil=\\\"true\\\"/><FirstBaggageDateTime xsi:nil=\\\"true\\\"/><LastBaggageDateTime xsi:nil=\\\"true\\\"/></BaggageReclaim></Airport></FlightData></Data></IMFRoot>";
    String xml2 = StringEscapeUtils.unescapeJson(demo);
    System.out.println(xml2);
  }

  @Test
  public void test3() {

    String demo1 =
        "<IMFRoot xmlns:xsi=\\\"http://www.w3.org/2001/XMLSchema-instance\\\"><SysInfo><MessageSequenceID>109882</MessageSequenceID><TestUUId>";
    String demo2 =
        "</TestUUId><MessageType>FlightData</MessageType><ServiceType>FSS1</ServiceType><OperationMode>MOD</OperationMode><SendDateTime>2023-03-31T00:03:05.818</SendDateTime><CreateDateTime>2023-03-31T00:03:05.795</CreateDateTime><OriginalDateTime>2023-03-31T00:03:05</OriginalDateTime><Receiver>WX</Receiver><Sender>IMF</Sender><Owner>AODB</Owner></SysInfo><Data><PrimaryKey><FlightKey><FlightScheduledDate>2023-03-31</FlightScheduledDate><FlightIdentity>JD5530</FlightIdentity><FlightDirection>A</FlightDirection><BaseAirport><AirportIATACode>HAK</AirportIATACode><AirportICAOCode>ZJHK</AirportICAOCode></BaseAirport><DetailedIdentity><AirlineIATACode>JD</AirlineIATACode><AirlineICAOCode>CBJ</AirlineICAOCode></DetailedIdentity><InternalID>1003782114</InternalID></FlightKey></PrimaryKey><FlightData><General><FlightScheduledDateTime>2023-03-31T00:35:00</FlightScheduledDateTime><Registration>B6723</Registration><CallSign>CBJ5530</CallSign><AircraftType><AircraftIATACode>320</AircraftIATACode><AircraftICAOCode>A320</AircraftICAOCode></AircraftType><FlightServiceType><FlightCAACServiceType>W/Z</FlightCAACServiceType><FlightIATAServiceType>J</FlightIATAServiceType></FlightServiceType><FlightRoute><IATARoute><IATAOriginAirport>NNG</IATAOriginAirport><IATAPreviousAirport>NNG</IATAPreviousAirport><IATAFullRoute><AirportIATACode LegNo=\\\"1\\\">NNG</AirportIATACode></IATAFullRoute></IATARoute><ICAORoute><ICAOOriginAirport>ZGNN</ICAOOriginAirport><ICAOPreviousAirport>ZGNN</ICAOPreviousAirport><ICAOFullRoute><AirportICAOCode LegNo=\\\"1\\\">ZGNN</AirportICAOCode></ICAOFullRoute></ICAORoute></FlightRoute><FlightCountryType>D</FlightCountryType><LinkFlight><FlightScheduledDate>2023-03-31</FlightScheduledDate><FlightIdentity>JD5891</FlightIdentity><FlightDirection>D</FlightDirection><BaseAirport><AirportIATACode>HAK</AirportIATACode><AirportICAOCode>ZJHK</AirportICAOCode></BaseAirport><DetailedIdentity><AirlineIATACode>JD</AirlineIATACode><AirlineICAOCode>CBJ</AirlineICAOCode></DetailedIdentity><InternalID>1003782389</InternalID></LinkFlight><SlaveFlight><FlightIdentity/></SlaveFlight><FreeTextComment><PublicTextComment xsi:nil=\\\"true\\\"/></FreeTextComment></General><OperationalDateTime><PreviousAirportDepartureDateTime><ScheduledPreviousAirportDepartureDateTime>2023-03-30T23:20:00</ScheduledPreviousAirportDepartureDateTime><EstimatedPreviousAirportDepartureDateTime xsi:nil=\\\"true\\\"/><ActualPreviousAirportDepartureDateTime>2023-03-30T23:24:00</ActualPreviousAirportDepartureDateTime></PreviousAirportDepartureDateTime><LandingDateTime><ScheduledLandingDateTime>2023-03-31T00:35:00</ScheduledLandingDateTime><EstimatedLandingDateTime OldValue=\\\"2023-03-31T00:15:00\\\">2023-03-31T00:10:00</EstimatedLandingDateTime><ActualLandingDateTime xsi:nil=\\\"true\\\"/></LandingDateTime><OnBlockDateTime><ScheduledOnBlockDateTime>2023-03-31T00:40:00</ScheduledOnBlockDateTime><EstimatedOnBlockDateTime OldValue=\\\"2023-03-31T00:20:00\\\">2023-03-31T00:15:00</EstimatedOnBlockDateTime><ActualOnBlockDateTime xsi:nil=\\\"true\\\"/></OnBlockDateTime></OperationalDateTime><Status><OperationStatus/><FlightStatus>P</FlightStatus><DelayReason><DelayCode/><DelayFreeText/></DelayReason><DiversionAirport><AirportIATACode/><AirportICAOCode/></DiversionAirport><ChangeLandingAirport><AirportIATACode/><AirportICAOCode/></ChangeLandingAirport></Status><Airport><Terminal><FlightTerminalID>01</FlightTerminalID><AircraftTerminalID/></Terminal><Stand><StandID>25</StandID></Stand><GroundMovement><StandID>25</StandID><ScheduledStandStartDateTime OldValue=\\\"2023-03-31T00:20:00\\\">2023-03-31T00:15:00</ScheduledStandStartDateTime><ScheduledStandEndDateTime>2023-03-31T10:20:00</ScheduledStandEndDateTime><ActualStandStartDateTime xsi:nil=\\\"true\\\"/><ActualStandEndDateTime xsi:nil=\\\"true\\\"/></GroundMovement><Gate><GateID/><GateStatus xsi:nil=\\\"true\\\"/><ScheduledGateStartDateTime xsi:nil=\\\"true\\\"/><ScheduledGateEndDateTime xsi:nil=\\\"true\\\"/><ActualGateStartDateTime xsi:nil=\\\"true\\\"/><ActualGateEndDateTime xsi:nil=\\\"true\\\"/><BoardingStartDateTime xsi:nil=\\\"true\\\"/><LastCallDateTime xsi:nil=\\\"true\\\"/><BoardingEndDateTime xsi:nil=\\\"true\\\"/></Gate><BaggageReclaim><BaggageReclaimID>3</BaggageReclaimID><ScheduledReclaimStartDateTime OldValue=\\\"2023-03-31T00:30:00\\\">2023-03-31T00:25:00</ScheduledReclaimStartDateTime><ScheduledReclaimEndDateTime OldValue=\\\"2023-03-31T00:55:00\\\">2023-03-31T00:50:00</ScheduledReclaimEndDateTime><ActualReclaimStartDateTime xsi:nil=\\\"true\\\"/><ActualReclaimEndDateTime xsi:nil=\\\"true\\\"/><FirstBaggageDateTime xsi:nil=\\\"true\\\"/><LastBaggageDateTime xsi:nil=\\\"true\\\"/></BaggageReclaim></Airport></FlightData></Data></IMFRoot>";

    String uuid = IdGenerator.uuid();
    String demo3 = demo1 + uuid + demo2;

    String xml2 = StringEscapeUtils.unescapeJson(demo3);

    System.out.println(xml2);
  }

  @Test
  public void testBatchTest() {

    for (int i = 1; i <= 5; i++) {
      String jobName = "326wxg_500w_" + i;
      String jobMode = "STREAMING";
      String topic = "321wxg_500w_" + i;
      String subscrption = "326wxg_500w_" + i;
      String table = "ods_bag_aodb_flight_wxg326_500w_" + i;

      String json =
          "{\n"
              + "  \"env\": {\n"
              + "    \"flushInterval\": 5\n"
              + "  },\n"
              + "  \"jobName\": \""
              + jobName
              + "\",\n"
              + "  \"jobMode\": \""
              + jobMode
              + "\",\n"
              + "  \"sources\": [\n"
              + "    {\n"
              + "      \"type\": \"pulsar\",\n"
              + "      \"name\": \"pulsar\",\n"
              + "      \"output\": \"pulsar-output\",\n"
              + "      \"options\": {\n"
              + "        \"clientUrl\": \"pulsar://192.168.54.206:6650,192.168.54.207:6650,192.168.54.208:6650\",\n"
              + "        \"adminUrl\": \"http://192.168.54.206:8080,192.168.54.207:8080,192.168.54.208:8080\",\n"
              + "        \"token\": \"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.yY2MSTBSEk3WkL3yEfSUdyq-EbeO1Lyg5EG1KiuGO4M\",\n"
              + "        \"topic\": \""
              + topic
              + "\",\n"
              + "        \"subscription\": \""
              + subscrption
              + "\",\n"
              + "        \"subscriptionType\": \"Earliest\",\n"
              + "        \"commitSize\": 1000\n"
              + "      },\n"
              + "      \"columns\": [\n"
              + "        {\n"
              + "          \"name\": \"sendTime\",\n"
              + "          \"type\": \"varchar\",\n"
              + "          \"nullable\": false,\n"
              + "          \"length\": 100,\n"
              + "          \"comment\": \"主键\",\n"
              + "          \"defaultValue\": -1\n"
              + "        },\n"
              + "        {\n"
              + "          \"name\": \"eventTime\",\n"
              + "          \"type\": \"varchar\",\n"
              + "          \"nullable\": true,\n"
              + "          \"length\": 100,\n"
              + "          \"comment\": \"用户名\",\n"
              + "          \"defaultValue\": null\n"
              + "        },\n"
              + "        {\n"
              + "          \"name\": \"operate\",\n"
              + "          \"type\": \"varchar\",\n"
              + "          \"nullable\": true,\n"
              + "          \"length\": 100,\n"
              + "          \"comment\": \"用户名\",\n"
              + "          \"defaultValue\": null\n"
              + "        },\n"
              + "        {\n"
              + "          \"name\": \"data\",\n"
              + "          \"type\": \"varchar\",\n"
              + "          \"nullable\": true,\n"
              + "          \"length\": 100,\n"
              + "          \"comment\": \"用户名\",\n"
              + "          \"defaultValue\": null\n"
              + "        }\n"
              + "      ]\n"
              + "    }\n"
              + "  ],\n"
              + "  \"transforms\": [\n"
              + "    {\n"
              + "      \"type\": \"xml-parse\",\n"
              + "      \"name\": \"xml-parse\",\n"
              + "      \"inputs\": [\n"
              + "        \"pulsar-output\"\n"
              + "      ],\n"
              + "      \"output\": \"xml-output\",\n"
              + "      \"options\": {\n"
              + "        \"valueField\": \"data\",\n"
              + "        \"xmlParseFields\": [\n"
              + "          {\n"
              + "            \"destination\": \"MessageType\",\n"
              + "            \"sourcePath\": \"//SysInfo/MessageType\",\n"
              + "            \"type\": \"String\"\n"
              + "          },\n"
              + "          {\n"
              + "            \"destination\": \"FlightScheduledDate\",\n"
              + "            \"sourcePath\": \"//Data/PrimaryKey/FlightKey/FlightScheduledDate\",\n"
              + "            \"type\": \"String\"\n"
              + "          },\n"
              + "          {\n"
              + "            \"destination\": \"ServiceType\",\n"
              + "            \"sourcePath\": \"//SysInfo/ServiceType\",\n"
              + "            \"type\": \"String\"\n"
              + "          },\n"
              + "          {\n"
              + "            \"destination\": \"OperationMode\",\n"
              + "            \"sourcePath\": \"//SysInfo/OperationMode\",\n"
              + "            \"type\": \"String\"\n"
              + "          },\n"
              + "          {\n"
              + "            \"destination\": \"FlightIdentity\",\n"
              + "            \"sourcePath\": \"//Data/PrimaryKey/FlightKey/FlightIdentity\",\n"
              + "            \"type\": \"String\"\n"
              + "          },\n"
              + "          {\n"
              + "            \"destination\": \"FlightDirection\",\n"
              + "            \"sourcePath\": \"//Data/PrimaryKey/FlightKey/FlightDirection\",\n"
              + "            \"type\": \"String\"\n"
              + "          },\n"
              + "          {\n"
              + "            \"destination\": \"Registration\",\n"
              + "            \"sourcePath\": \"//Data/FlightData/General/Registration\",\n"
              + "            \"type\": \"String\"\n"
              + "          }\n"
              + "        ]\n"
              + "      }\n"
              + "    },\n"
              + "    {\n"
              + "      \"type\": \"generated-field\",\n"
              + "      \"name\": \"generated-field\",\n"
              + "      \"inputs\": [\n"
              + "        \"xml-output\"\n"
              + "      ],\n"
              + "      \"output\": \"generated-field-output\",\n"
              + "      \"options\": {\n"
              + "        \"fields\": [\n"
              + "          {\n"
              + "            \"generateType\": \"currentTime\",\n"
              + "            \"destName\": \"now\"\n"
              + "          },\n"
              + "          {\n"
              + "            \"generateType\": \"uuid\",\n"
              + "            \"destName\": \"uuid\"\n"
              + "          }\n"
              + "        ]\n"
              + "      }\n"
              + "    }\n"
              + "  ],\n"
              + "  \"sink\": {\n"
              + "    \"type\": \"jdbc\",\n"
              + "    \"name\": \"jdbc\",\n"
              + "    \"input\": \"generated-field-output\",\n"
              + "    \"options\": {\n"
              + "      \"url\": \"jdbc:mysql://192.168.54.206:3306/test?characterEncoding=utf-8&useSSL=false&allowPublicKeyRetrieval=true\",\n"
              + "      \"username\": \"root\",\n"
              + "      \"password\": \"bigdata321\",\n"
              + "      \"driver\": \"com.mysql.cj.jdbc.Driver\",\n"
              + "      \"database\": \"test\",\n"
              + "      \"table\": \""
              + table
              + "\",\n"
              + "      \"batchSize\": 1000,\n"
              + "      \"mode\": \"INSERT\",\n"
              + "      \"fields\": [\n"
              + "        \"FlightScheduledDate\",\n"
              + "        \"MessageType\",\n"
              + "        \"uuid\",\n"
              + "        \"now\",\n"
              + "        \"ServiceType\",\n"
              + "        \"OperationMode\",\n"
              + "        \"FlightIdentity\",\n"
              + "        \"FlightDirection\",\n"
              + "        \"Registration\"\n"
              + "      ]\n"
              + "    }\n"
              + "  }\n"
              + "}";
      // System.out.println(json);

      String s = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
      System.out.println(s);
    }
  }
}
