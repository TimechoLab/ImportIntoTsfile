package com.timecho.test;

import com.timecho.LoadData;

public class Main {
    public static void main(String[] args) throws Exception {
        String[] params = {
                "-l", "com.timecho.test.SqliteDataLoader",
                "-s", "../data/1002_CMS_016_20211216230132_X.sqlite",
                "-d", "root.raw.`1062`.`001`.cms"
        };
        LoadData.main(params);
    }
}
