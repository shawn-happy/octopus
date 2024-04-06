package io.github.shawn.octopus.fluxus.engine.connector.transform.xml;

import com.google.common.io.Resources;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.connector.transform.xmlParse.XmlParseTransform;
import io.github.shawn.octopus.fluxus.engine.connector.transform.xmlParse.XmlParseTransformConfig;
import io.github.shawn.octopus.fluxus.engine.model.table.SourceRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
class XmlParseTransformTests {

  private static final String demo1 =
      "<IMFRoot xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><SysInfo><MessageSequenceID>109882</MessageSequenceID><MessageType>FlightData</MessageType><ServiceType>FSS1</ServiceType><OperationMode>MOD</OperationMode><SendDateTime>2023-03-31T00:03:05.818</SendDateTime><CreateDateTime>2023-03-31T00:03:05.795</CreateDateTime><OriginalDateTime>2023-03-31T00:03:05</OriginalDateTime><Receiver>WX</Receiver><Sender>IMF</Sender><Owner>AODB</Owner></SysInfo><Data><PrimaryKey><FlightKey><FlightScheduledDate>2023-03-31</FlightScheduledDate><FlightIdentity>JD5530</FlightIdentity><FlightDirection>A</FlightDirection><BaseAirport><AirportIATACode>HAK</AirportIATACode><AirportICAOCode>ZJHK</AirportICAOCode></BaseAirport><DetailedIdentity><AirlineIATACode>JD</AirlineIATACode><AirlineICAOCode>CBJ</AirlineICAOCode></DetailedIdentity><InternalID>1003782114</InternalID></FlightKey></PrimaryKey><FlightData><General><FlightScheduledDateTime>2023-03-31T00:35:00</FlightScheduledDateTime><Registration>B6723</Registration><CallSign>CBJ5530</CallSign><AircraftType><AircraftIATACode>320</AircraftIATACode><AircraftICAOCode>A320</AircraftICAOCode></AircraftType><FlightServiceType><FlightCAACServiceType>W/Z</FlightCAACServiceType><FlightIATAServiceType>J</FlightIATAServiceType></FlightServiceType><FlightRoute><IATARoute><IATAOriginAirport>NNG</IATAOriginAirport><IATAPreviousAirport>NNG</IATAPreviousAirport><IATAFullRoute><AirportIATACode LegNo=\"1\">NNG</AirportIATACode></IATAFullRoute></IATARoute><ICAORoute><ICAOOriginAirport>ZGNN</ICAOOriginAirport><ICAOPreviousAirport>ZGNN</ICAOPreviousAirport><ICAOFullRoute><AirportICAOCode LegNo=\"1\">ZGNN</AirportICAOCode></ICAOFullRoute></ICAORoute></FlightRoute><FlightCountryType>D</FlightCountryType><LinkFlight><FlightScheduledDate>2023-03-31</FlightScheduledDate><FlightIdentity>JD5891</FlightIdentity><FlightDirection>D</FlightDirection><BaseAirport><AirportIATACode>HAK</AirportIATACode><AirportICAOCode>ZJHK</AirportICAOCode></BaseAirport><DetailedIdentity><AirlineIATACode>JD</AirlineIATACode><AirlineICAOCode>CBJ</AirlineICAOCode></DetailedIdentity><InternalID>1003782389</InternalID></LinkFlight><SlaveFlight><FlightIdentity/></SlaveFlight><FreeTextComment><PublicTextComment xsi:nil=\"true\"/></FreeTextComment></General><OperationalDateTime><PreviousAirportDepartureDateTime><ScheduledPreviousAirportDepartureDateTime>2023-03-30T23:20:00</ScheduledPreviousAirportDepartureDateTime><EstimatedPreviousAirportDepartureDateTime xsi:nil=\"true\"/><ActualPreviousAirportDepartureDateTime>2023-03-30T23:24:00</ActualPreviousAirportDepartureDateTime></PreviousAirportDepartureDateTime><LandingDateTime><ScheduledLandingDateTime>2023-03-31T00:35:00</ScheduledLandingDateTime><EstimatedLandingDateTime OldValue=\"2023-03-31T00:15:00\">2023-03-31T00:10:00</EstimatedLandingDateTime><ActualLandingDateTime xsi:nil=\"true\"/></LandingDateTime><OnBlockDateTime><ScheduledOnBlockDateTime>2023-03-31T00:40:00</ScheduledOnBlockDateTime><EstimatedOnBlockDateTime OldValue=\"2023-03-31T00:20:00\">2023-03-31T00:15:00</EstimatedOnBlockDateTime><ActualOnBlockDateTime xsi:nil=\"true\"/></OnBlockDateTime></OperationalDateTime><Status><OperationStatus/><FlightStatus>P</FlightStatus><DelayReason><DelayCode/><DelayFreeText/></DelayReason><DiversionAirport><AirportIATACode/><AirportICAOCode/></DiversionAirport><ChangeLandingAirport><AirportIATACode/><AirportICAOCode/></ChangeLandingAirport></Status><Airport><Terminal><FlightTerminalID>01</FlightTerminalID><AircraftTerminalID/></Terminal><Stand><StandID>25</StandID></Stand><GroundMovement><StandID>25</StandID><ScheduledStandStartDateTime OldValue=\"2023-03-31T00:20:00\">2023-03-31T00:15:00</ScheduledStandStartDateTime><ScheduledStandEndDateTime>2023-03-31T10:20:00</ScheduledStandEndDateTime><ActualStandStartDateTime xsi:nil=\"true\"/><ActualStandEndDateTime xsi:nil=\"true\"/></GroundMovement><Gate><GateID/><GateStatus xsi:nil=\"true\"/><ScheduledGateStartDateTime xsi:nil=\"true\"/><ScheduledGateEndDateTime xsi:nil=\"true\"/><ActualGateStartDateTime xsi:nil=\"true\"/><ActualGateEndDateTime xsi:nil=\"true\"/><BoardingStartDateTime xsi:nil=\"true\"/><LastCallDateTime xsi:nil=\"true\"/><BoardingEndDateTime xsi:nil=\"true\"/></Gate><BaggageReclaim><BaggageReclaimID>3</BaggageReclaimID><ScheduledReclaimStartDateTime OldValue=\"2023-03-31T00:30:00\">2023-03-31T00:25:00</ScheduledReclaimStartDateTime><ScheduledReclaimEndDateTime OldValue=\"2023-03-31T00:55:00\">2023-03-31T00:50:00</ScheduledReclaimEndDateTime><ActualReclaimStartDateTime xsi:nil=\"true\"/><ActualReclaimEndDateTime xsi:nil=\"true\"/><FirstBaggageDateTime xsi:nil=\"true\"/><LastBaggageDateTime xsi:nil=\"true\"/></BaggageReclaim></Airport></FlightData></Data></IMFRoot>";

  @Test
  void xmlTest() {
    XmlParseTransformConfig config =
        XmlParseTransformConfig.builder()
            .options(
                XmlParseTransformConfig.XmlParseTransformOptions.builder()
                    .valueField("xml")
                    .xmlParseFields(
                        new XmlParseTransformConfig.XmlParseField[] {
                          XmlParseTransformConfig.XmlParseField.builder()
                              .destination("MessageType")
                              .sourcePath("//SysInfo/MessageType")
                              .type("String")
                              .build(),
                          XmlParseTransformConfig.XmlParseField.builder()
                              .destination("FlightScheduledDate")
                              .sourcePath("//Data/PrimaryKey/FlightKey/FlightScheduledDate")
                              .type("String")
                              .build(),
                          XmlParseTransformConfig.XmlParseField.builder()
                              .destination("ServiceType")
                              .sourcePath("//SysInfo/ServiceType")
                              .type("String")
                              .build(),
                          XmlParseTransformConfig.XmlParseField.builder()
                              .destination("OperationMode")
                              .sourcePath("//SysInfo/OperationMode")
                              .type("String")
                              .build(),
                          XmlParseTransformConfig.XmlParseField.builder()
                              .destination("FlightIdentity")
                              .sourcePath("//Data/PrimaryKey/FlightKey/FlightIdentity")
                              .type("String")
                              .build(),
                          XmlParseTransformConfig.XmlParseField.builder()
                              .destination("FlightDirection")
                              .sourcePath("//Data/PrimaryKey/FlightKey/FlightDirection")
                              .type("String")
                              .build(),
                          XmlParseTransformConfig.XmlParseField.builder()
                              .destination("Registration")
                              .sourcePath("//Data/FlightData/General/Registration")
                              .type("String")
                              .build()
                        })
                    .build())
            .build();
    SourceRowRecord record =
        SourceRowRecord.builder()
            .fieldNames(new String[] {"id", "name", "xml"})
            .fieldTypes(
                new DataWorkflowFieldType[] {
                  BasicFieldType.INT, BasicFieldType.STRING, BasicFieldType.STRING
                })
            .values(new Object[] {1, "xmlName", demo1})
            .build();
    long start = System.nanoTime();
    XmlParseTransform xmlParseTransform = new XmlParseTransform(config);
    xmlParseTransform.init();
    xmlParseTransform.begin();
    RowRecord transform = xmlParseTransform.transform(record);
    long l = System.nanoTime() - start;
    log.info("总耗时：{}毫秒", l / 1000000);
    log.info(
        "\nRowRecord field = {} \nRowRecord values = {}",
        transform.getFieldNames(),
        transform.getValues());
    Assertions.assertNotNull(transform);
  }

  @Test
  void xmlParseTest() throws IOException {
    XmlParseTransformConfig config =
        XmlParseTransformConfig.builder()
            .options(
                XmlParseTransformConfig.XmlParseTransformOptions.builder()
                    .valueField("xml")
                    .xmlParseFields(
                        new XmlParseTransformConfig.XmlParseField[] {
                          XmlParseTransformConfig.XmlParseField.builder()
                              .destination("data_id")
                              .sourcePath("/data/a")
                              .type("int")
                              .build(),
                          XmlParseTransformConfig.XmlParseField.builder()
                              .destination("data_name")
                              .sourcePath("/data/b")
                              .type("String")
                              .build()
                        })
                    .build())
            .build();

    SourceRowRecord record =
        SourceRowRecord.builder()
            .fieldNames(new String[] {"id", "name", "xml"})
            .fieldTypes(
                new DataWorkflowFieldType[] {
                  BasicFieldType.INT, BasicFieldType.STRING, BasicFieldType.STRING
                })
            .values(
                new Object[] {
                  1,
                  "xmlName",
                  Resources.toString(
                      Objects.requireNonNull(
                          Thread.currentThread()
                              .getContextClassLoader()
                              .getResource("example/transform/xml/data.xml")),
                      StandardCharsets.UTF_8)
                })
            .build();
    XmlParseTransform xmlParseTransform = new XmlParseTransform(config);
    xmlParseTransform.init();
    xmlParseTransform.begin();
    RowRecord transform = xmlParseTransform.transform(record);
    log.info(
        "\nRowRecord field = {} \nRowRecord values = {}",
        transform.getFieldNames(),
        transform.getValues());
    Assertions.assertNotNull(transform);
  }
}
