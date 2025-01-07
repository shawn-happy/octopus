package io.github.octopus.sys.salus.convert;

import io.github.octopus.sys.salus.entity.SysDept;
import io.github.octopus.sys.salus.model.request.DeptForm;
import java.util.Optional;

public class DeptConverter {

  public static SysDept toEntity(DeptForm deptForm) {
    return Optional.ofNullable(deptForm)
        .map(
            form -> {
              SysDept sysDept = new SysDept();
              sysDept.setPid(deptForm.getPid());
              sysDept.setCode(deptForm.getCode());
              sysDept.setName(deptForm.getName());
              sysDept.setLeaderName(deptForm.getLeader());
              sysDept.setLeaderPhone(deptForm.getPhone());
              sysDept.setLeaderEmail(deptForm.getEmail());
              sysDept.setOrderNum(deptForm.getOrder());
              sysDept.setDescription(deptForm.getRemark());
              return sysDept;
            })
        .orElse(null);
  }
}
