package com.atguigu.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class zkClient {

    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
//                System.out.println("--------------------");
//                List<String> children = null;
//                try {
//                    children = zkClient.getChildren("/", true);
//                    for (String child : children) {
//                        System.out.println(child);
//                    }
//
//                    System.out.println("--------------------");
//                } catch (KeeperException e) {
//                    throw new RuntimeException(e);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }

            }
        });

    }

    @Test
    public void create() throws InterruptedException, KeeperException {
        String nodeCreated = zkClient.create("/atguigu", "ss".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void getChildren() throws InterruptedException, KeeperException {
        List<String> children = zkClient.getChildren("/", true);

        for (String child : children) {
            System.out.println(child);
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void exsit() throws InterruptedException, KeeperException {
        Stat stat = zkClient.exists("/atguigu", false);
        System.out.println(stat == null ? "not exsit" : "exsit");
    }
}
