package io.github.shawn.octopus.fluxus.engine.connector.transform.xmlParse;

import io.github.shawn.octopus.fluxus.api.connector.Transform;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.engine.model.table.TransformRowRecord;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.commons.collections.CollectionUtils;
import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.XPath;

public class XmlParseTransform implements Transform<XmlParseTransformConfig> {

  private final XmlParseTransformConfig config;
  private final XmlParseTransformConfig.XmlParseTransformOptions options;
  private DocumentBuilder documentBuilder;

  public XmlParseTransform(XmlParseTransformConfig xmlParseTransformConfig) {
    this.config = xmlParseTransformConfig;
    this.options = xmlParseTransformConfig.getOptions();
  }

  @Override
  public void init() throws StepExecutionException {
    try {
      DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
      documentBuilderFactory.setFeature(
          "http://apache.org/xml/features/disallow-doctype-decl", true);
      documentBuilderFactory.setFeature(
          "http://xml.org/sax/features/external-general-entities", false);
      documentBuilderFactory.setFeature(
          "http://xml.org/sax/features/external-parameter-entities", false);
      documentBuilderFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
      documentBuilderFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
      documentBuilderFactory.setAttribute(XMLConstants.FEATURE_SECURE_PROCESSING, Boolean.FALSE);
      documentBuilderFactory.setNamespaceAware(false);
      documentBuilderFactory.setExpandEntityReferences(false);
      documentBuilder = documentBuilderFactory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new StepExecutionException(e);
    }
  }

  @Override
  public RowRecord transform(RowRecord source) throws StepExecutionException {
    int index = source.indexOf(options.getValueField());
    List<Object[]> results = null;
    Object[] values;
    while ((values = source.pollNext()) != null) {
      Object value = values[index];
      // 解析
      results = parse(String.valueOf(value));
    }
    TransformRowRecord rowRecord =
        new TransformRowRecord(options.getDestinations(), options.getDestinationTypes());
    rowRecord.addRecords(results);
    return rowRecord;
  }

  @Override
  public XmlParseTransformConfig getTransformConfig() {
    return config;
  }

  public List<Object[]> parse(String xmlData) throws StepExecutionException {
    List<List<Node>> results = evalCombinedResult(xmlData);
    return buildRowDataList(results);
  }

  private List<List<Node>> evalCombinedResult(String xmlData) {
    XmlParseTransformConfig.XmlParseField[] xmlParseFields = options.getXmlParseFields();
    List<List<Node>> results = new ArrayList<>(xmlParseFields.length);
    try (InputStream stream = new ByteArrayInputStream(xmlData.getBytes(StandardCharsets.UTF_8))) {
      org.dom4j.io.DOMReader xmlReader = new org.dom4j.io.DOMReader();
      Document document = xmlReader.read(documentBuilder.parse(stream));
      Map<String, String> map = new HashMap<>();
      String nsURI = document.getRootElement().getNamespaceURI();
      map.put("xmlns", nsURI);
      int lastSize = -1;
      for (XmlParseTransformConfig.XmlParseField xmlParseField : xmlParseFields) {
        XPath x = document.createXPath(xmlParseField.getSourcePath());
        x.setNamespaceURIs(map);
        List<Node> result = x.selectNodes(document);
        if (result.size() != lastSize && lastSize > 0 && !result.isEmpty()) {
          throw new StepExecutionException(
              String.format(
                  "The data structure is not the same inside the resource\\! We found %s values for json path [%s], which is different that the number returned for path [%s], We MUST have the same number of values for all paths.",
                  result.size(), xmlParseField.getSourcePath(), lastSize));
        }
        results.add(result);
        lastSize = result.size();
      }
    } catch (Exception e) {
      throw new StepExecutionException(e);
    }
    return results;
  }

  private List<Object[]> buildRowDataList(List<List<Node>> results) {
    List<Object[]> result = new LinkedList<>();
    Object[] rowData;
    // 获取记录条数
    int rowNum = getRowNum(results);
    for (int i = 0; i < rowNum; i++) {
      rowData = new Object[results.size()];
      result.add(rowData);
      for (int j = 0; j < results.size(); j++) {
        List<Node> temp = results.get(j);
        if (CollectionUtils.isNotEmpty(temp) && temp.size() > i) {
          rowData[j] = temp.get(i).getText();
        } else {
          rowData[j] = null;
        }
      }
    }
    return result;
  }

  private int getRowNum(List<List<Node>> results) {
    int num = 0;
    if (CollectionUtils.isEmpty(results)) {
      return num;
    }
    for (List<Node> arr : results) {
      if (CollectionUtils.isNotEmpty(arr) && num < arr.size()) {
        num = arr.size();
      }
    }
    return num;
  }
}
