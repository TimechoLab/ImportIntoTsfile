package com.timecho.dataloader;

import com.timecho.LoadCommandOptions;
import org.apache.commons.math3.util.Pair;

import java.util.List;
import java.util.Map;

public interface DataLoaderInterface {
    List<Pair<Long, Map<String, Object>>> loadTimeSeries(LoadCommandOptions options);
}
