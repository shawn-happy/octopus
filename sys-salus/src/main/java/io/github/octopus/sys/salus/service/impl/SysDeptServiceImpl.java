package io.github.octopus.sys.salus.service.impl;

import io.github.octopus.sys.salus.convert.DeptConverter;
import io.github.octopus.sys.salus.dao.SysDeptDao;
import io.github.octopus.sys.salus.entity.SysDept;
import io.github.octopus.sys.salus.exception.DeptAlreadyExistsException;
import io.github.octopus.sys.salus.exception.DeptAlreadyForbiddenException;
import io.github.octopus.sys.salus.exception.DeptNotFoundException;
import io.github.octopus.sys.salus.model.bo.Dept;
import io.github.octopus.sys.salus.model.request.DeptForm;
import io.github.octopus.sys.salus.service.SysDeptService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class SysDeptServiceImpl implements SysDeptService {

  private final SysDeptDao sysDeptDao;

  @Override
  @Transactional
  public Long createDept(DeptForm deptForm) {
    Long pid = deptForm.getPid();
    SysDept parentSysDept = null;
    if (pid != null && pid > 0L) {
      parentSysDept = sysDeptDao.selectById(pid);
      if (parentSysDept == null) {
        throw new DeptNotFoundException(String.valueOf(pid));
      }
      boolean enabled = parentSysDept.isEnabled();
      if (!enabled) {
        throw new DeptAlreadyForbiddenException(parentSysDept.getCode());
      }
    }
    Dept dept = getDeptByCode(deptForm.getCode());
    if (dept != null) {
      throw new DeptAlreadyExistsException(deptForm.getCode());
    }

    SysDept entity = DeptConverter.toEntity(deptForm);
    entity.setEnabled(true);
    if (parentSysDept != null) {
      String ancestors = parentSysDept.getAncestors();
      entity.setAncestors(ancestors + ", " + deptForm.getPid());
    }
    sysDeptDao.insert(entity);
    return entity.getId();
  }

  @Override
  public Dept getDeptByCode(String code) {
    SysDept sysDept = sysDeptDao.selectByCode(code);
    if (sysDept == null) {
      return null;
    } else {
      Dept dept = Dept.builder().build();
      BeanUtils.copyProperties(sysDept, dept);
      return dept;
    }
  }
}
