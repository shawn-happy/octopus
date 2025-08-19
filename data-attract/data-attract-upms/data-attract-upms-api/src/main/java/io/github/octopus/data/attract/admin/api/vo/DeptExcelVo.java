package io.github.octopus.data.attract.admin.api.vo;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

import java.io.Serializable;

/**
 * 部门导入导出
 */
@Data
public class DeptExcelVo implements Serializable {

	/**
	 * 导入时候回显行号
	 */
	private Long lineNum;

	/**
	 * 上级部门
	 */
	@NotBlank(message = "上级部门不能为空")
	private String parentName;

	/**
	 * 部门名称
	 */
	@NotBlank(message = "部门名称不能为空")
	private String name;

	/**
	 * 排序
	 */
	private Integer sortOrder;

}
