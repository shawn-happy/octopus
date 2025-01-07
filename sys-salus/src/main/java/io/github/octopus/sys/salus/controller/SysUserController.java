package io.github.octopus.sys.salus.controller;

import io.github.octopus.sys.salus.service.SysUserService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v1/sys/user")
public class SysUserController {

  private final SysUserService sysUserService;
}
