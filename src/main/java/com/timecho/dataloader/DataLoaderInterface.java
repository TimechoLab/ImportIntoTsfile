package com.timecho.dataloader;

import org.apache.commons.math3.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface DataLoaderInterface {
    List<Pair<Long, Map<String, Object>>> loadTimeSeries(Properties properties);
}
