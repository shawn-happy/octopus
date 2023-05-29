package com.octopus.kettlex.model;

import java.util.Map;

public interface Options {

  Map<String, String> getOptions();

  void verify();
}
