package com.octopus.kettlex.core.statistics;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.Validate;

public class LocalCommunicationManager {

  private static Map<Integer, Communication> stepCommunicationMap = new ConcurrentHashMap<>();

  public void registerStepCommunication(int stepId, Communication communication) {
    stepCommunicationMap.put(stepId, communication);
  }

  public Communication getJobCommunication() {
    Communication communication = new Communication();
    communication.setState(ExecutionState.SUCCEEDED);

    for (Communication taskGroupCommunication : stepCommunicationMap.values()) {
      communication.mergeFrom(taskGroupCommunication);
    }

    return communication;
  }

  /**
   * 采用获取taskGroupId后再获取对应communication的方式， 防止map遍历时修改，同时也防止对map key-value对的修改
   *
   * @return
   */
  public Set<Integer> getStepIdSet() {
    return stepCommunicationMap.keySet();
  }

  public Communication getStepCommunication(int stepId) {
    Validate.isTrue(stepId >= 0, "stepId不能小于0");
    return stepCommunicationMap.get(stepId);
  }

  public void updateStepCommunication(final int taskGroupId, final Communication communication) {
    Validate.isTrue(
        stepCommunicationMap.containsKey(taskGroupId),
        String.format(
            "taskGroupCommunicationMap中没有注册taskGroupId[%d]的Communication，" + "无法更新该taskGroup的信息",
            taskGroupId));
    stepCommunicationMap.put(taskGroupId, communication);
  }

  public void clear() {
    stepCommunicationMap.clear();
  }

  public Map<Integer, Communication> getStepCommunicationMap() {
    return stepCommunicationMap;
  }
}
