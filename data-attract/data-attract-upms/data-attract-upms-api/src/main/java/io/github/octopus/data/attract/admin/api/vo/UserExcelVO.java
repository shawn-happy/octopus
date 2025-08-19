package io.github.octopus.data.attract.admin.api.vo;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
public class UserExcelVO implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * 导入时候回显行号
	 */
	private Long lineNum;

	/**
	 * 主键ID
	 */
	private Long userId;

	/**
	 * 用户名
	 */
	@NotBlank(message = "用户名不能为空")
	private String username;

	/**
	 * 手机号
	 */
	@NotBlank(message = "手机号不能为空")
	private String phone;

	/**
	 * 手机号
	 */
	@NotBlank(message = "昵称不能为空")
	private String nickname;

	/**
	 * 手机号
	 */
	@NotBlank(message = "姓名不能为空")
	private String name;

	/**
	 * 手机号
	 */
	@NotBlank(message = "邮箱不能为空")
	private String email;

	/**
	 * 部门名称
	 */
	@NotBlank(message = "部门名称不能为空")
	private String deptName;

	/**
	 * 角色列表
	 */
	@NotBlank(message = "角色不能为空")
	private String roleNameList;

	/**
	 * 角色列表
	 */
	@NotBlank(message = "岗位不能为空")
	private String postNameList;

	/**
	 * 锁定标记
	 */
	private String lockFlag;

	/**
	 * 创建时间
	 */
	private LocalDateTime createTime;

}
