package io.github.octopus.sys.salus.service.impl;

import io.github.octopus.sys.salus.dao.SysUserDao;
import io.github.octopus.sys.salus.service.SysUserService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class SysUserServiceImpl implements SysUserService {

  private final SysUserDao sysUserDao;
}
