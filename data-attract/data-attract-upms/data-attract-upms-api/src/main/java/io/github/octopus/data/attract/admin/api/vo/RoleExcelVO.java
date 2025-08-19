package io.github.octopus.data.attract.admin.api.vo;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
public class RoleExcelVO implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * 导入时候回显行号
	 */
	private Long lineNum;

	/**
	 * 主键ID
	 */
	private Long roleId;

	/**
	 * 角色名称
	 */
	@NotBlank(message = "角色名称不能为空")
	private String roleName;

	/**
	 * 角色标识
	 */
	@NotBlank(message = "角色标识不能为空")
	private String roleCode;

	/**
	 * 角色描述
	 */
	@NotBlank(message = "角色描述不能为空")
	private String roleDesc;

	/**
	 * 创建时间
	 */
	private LocalDateTime createTime;

}
