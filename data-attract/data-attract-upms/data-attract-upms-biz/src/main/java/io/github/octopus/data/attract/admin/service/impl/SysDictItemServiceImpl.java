/*
 *    Copyright (c) 2018-2025, lengleng All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * Neither the name of the pig4cloud.com developer nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * Author: lengleng (wangiegie@gmail.com)
 */
package io.github.octopus.data.attract.admin.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.github.octopus.data.attract.admin.api.entity.SysDict;
import io.github.octopus.data.attract.admin.api.entity.SysDictItem;
import io.github.octopus.data.attract.admin.mapper.SysDictItemMapper;
import io.github.octopus.data.attract.admin.service.SysDictItemService;
import io.github.octopus.data.attract.admin.service.SysDictService;
import io.github.octopus.data.attract.common.core.constant.CacheConstants;
import io.github.octopus.data.attract.common.core.constant.enums.DictTypeEnum;
import io.github.octopus.data.attract.common.core.exception.ErrorCodes;
import io.github.octopus.data.attract.common.core.util.MsgUtils;
import io.github.octopus.data.attract.common.core.util.R;
import lombok.AllArgsConstructor;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Service;

/**
 * 字典项服务实现类
 *
 * @author lengleng
 * @date 2025/05/30
 */
@Service
@AllArgsConstructor
public class SysDictItemServiceImpl extends ServiceImpl<SysDictItemMapper, SysDictItem> implements SysDictItemService {

	private final SysDictService dictService;

	/**
	 * 删除字典项
	 * @param id 字典项ID
	 * @return 操作结果
	 * @see R
	 */
	@Override
	@CacheEvict(value = CacheConstants.DICT_DETAILS, allEntries = true)
	public R removeDictItem(Long id) {
		// 根据ID查询字典ID
		SysDictItem dictItem = this.getById(id);
		SysDict dict = dictService.getById(dictItem.getDictId());
		// 系统内置
		if (DictTypeEnum.SYSTEM.getType().equals(dict.getSystemFlag())) {
			return R.failed(MsgUtils.getMessage(ErrorCodes.SYS_DICT_DELETE_SYSTEM));
		}
		return R.ok(this.removeById(id));
	}

	/**
	 * 更新字典项
	 * @param item 需要更新的字典项
	 * @return 操作结果，包含成功或失败信息
	 * @see R
	 */
	@Override
	@CacheEvict(value = CacheConstants.DICT_DETAILS, key = "#item.dictType")
	public R updateDictItem(SysDictItem item) {
		// 查询字典
		SysDict dict = dictService.getById(item.getDictId());
		// 系统内置
		if (DictTypeEnum.SYSTEM.getType().equals(dict.getSystemFlag())) {
			return R.failed(MsgUtils.getMessage(ErrorCodes.SYS_DICT_UPDATE_SYSTEM));
		}
		return R.ok(this.updateById(item));
	}

}
