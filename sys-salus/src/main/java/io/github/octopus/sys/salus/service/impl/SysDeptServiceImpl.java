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
import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
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
  @Transactional
  public Long updateDept(DeptForm deptForm) {
    SysDept sysDept = sysDeptDao.selectById(deptForm.getId());
    if (sysDept == null) {
      throw new DeptNotFoundException(String.valueOf(deptForm.getId()));
    }
    String oldCode = sysDept.getCode();
    if (!oldCode.equals(deptForm.getCode())) {
      Dept dept = getDeptByCode(deptForm.getCode());
      if (dept != null) {
        throw new DeptAlreadyExistsException(deptForm.getCode());
      }
    }
    Long oldPid = sysDept.getPid();
    String oldAncestors = sysDept.getAncestors();
    String newAncestors = oldAncestors;
    if (!Objects.equals(oldPid, deptForm.getPid())) {
      SysDept parentDept = sysDeptDao.selectById(deptForm.getPid());
      if (parentDept == null) {
        throw new DeptNotFoundException(String.valueOf(deptForm.getPid()));
      }
      newAncestors = parentDept.getAncestors() + "," + parentDept.getId();
    }
    sysDept.setName(deptForm.getName());
    sysDept.setDescription(deptForm.getRemark());
    sysDept.setLeaderName(deptForm.getLeader());
    sysDept.setLeaderPhone(deptForm.getPhone());
    sysDept.setLeaderEmail(deptForm.getEmail());
    sysDept.setOrderNum(deptForm.getOrder());
    sysDept.setAncestors(newAncestors);
    sysDeptDao.updateById(sysDept);
    if (!oldAncestors.equals(newAncestors)) {
      updateChildrenDept(sysDept.getId(), newAncestors, oldAncestors);
    }
    return sysDept.getId();
  }

  @Override
  @Transactional
  public void updateChildrenDept(Long pid, String newAncestors, String oldAncestors) {
    List<SysDept> sysDeptList = sysDeptDao.selectByPid(pid);
    for (SysDept child : sysDeptList) {
      child.setAncestors(child.getAncestors().replaceFirst(oldAncestors, newAncestors));
    }
    if (CollectionUtils.isNotEmpty(sysDeptList)) {
      sysDeptDao.updateDeptChildren(sysDeptList);
    }
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
