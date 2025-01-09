package io.github.octopus.sys.salus.service;

import io.github.octopus.sys.salus.model.bo.Dept;
import io.github.octopus.sys.salus.model.request.DeptForm;

public interface SysDeptService {

  Long createDept(DeptForm deptForm);

  Long updateDept(DeptForm deptForm);

  void updateChildrenDept(Long pid, String newAncestors, String oldAncestors);

  Dept getDeptByCode(String code);
}
