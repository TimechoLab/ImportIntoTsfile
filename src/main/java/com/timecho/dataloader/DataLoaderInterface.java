package com.timecho.dataloader;

import org.apache.commons.math3.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface DataLoaderInterface {
    default List<Pair<Long, Map>> loadTimeSeries(Properties properties) { return null; }
}
