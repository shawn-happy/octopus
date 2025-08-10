package io.github.octopus.data.attract.common.log.event;

import io.github.octopus.data.attract.admin.api.entity.SysLog;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 系统日志事件源类，继承自SysLog
 *
 * @author lengleng
 * @date 2025/05/31
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class SysLogEventSource extends SysLog {

	/**
	 * 参数重写成object
	 */
	private Object body;

}
