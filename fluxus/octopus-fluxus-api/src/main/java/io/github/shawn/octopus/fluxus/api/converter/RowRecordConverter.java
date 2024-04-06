package io.github.shawn.octopus.fluxus.api.converter;

import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;

public interface RowRecordConverter<S> {

  RowRecord convert(S s);
}
