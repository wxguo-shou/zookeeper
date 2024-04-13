package com.atguigu.case2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import javax.lang.model.element.VariableElement;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedLock {

    private final String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private final int sessionTimeout = 2000;
    private final ZooKeeper zk;

    private CountDownLatch connectLatch = new CountDownLatch(1);
    private CountDownLatch waitLatch = new CountDownLatch(1);
    private String withPath;
    private String currentMode;

    public DistributedLock() throws IOException, InterruptedException, KeeperException {
        // 获取连接
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // connectLatch  如果连接上zk  可以释放
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    connectLatch.countDown();
                }

                // waitLatch 需要释放
                if (watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getPath().equals(withPath)){
                    waitLatch.countDown();
                }

            }
        });

        // 等待zk正常连接后， 往下走程序
        connectLatch.await();

        // 判断根节点是否存在
        Stat stat = zk.exists("/locks", false);

        if (stat == null){
            // 创建根节点
            zk.create("/locks", "locks".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

    }

    // 对zk 加锁
    public void zklock(){
        // 创建对应的 临时带序号 节点
        try {
            currentMode = zk.create("/locks/" + "seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            // 判断创建的节点是否为最小的节点， 如果是，获取到锁， 如果不是，监听上一个节点
            List<String> children = zk.getChildren("/locks", false);

            // 如果children只有一个值，直接获取。 如果有多个节点，需要判断，谁最小
            if (children.size() == 1){
                return;
            }else {
                Collections.sort(children);

                // 获取节点名称seq-00000000
                String thisNode = currentMode.substring("/locks/".length());
                // 通过seq-00000000获取该节点在children集合的位置
                int index = children.indexOf(thisNode);

                // 判断
                if(index == -1){
                    System.out.println("数据异常");
                }else if (index == 0){
                    // 就一个节点， 可以获取锁了
                    return;
                }else {
                    // 需要监听， 前一个节点的变化
                    withPath = "/locks/" + children.get(index - 1);
                    zk.getData(withPath, true, null);

                    // 等待监听
                    waitLatch.await();
                    return;
                }
            }

        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    // 解锁
    public void unZklock(){
        // 删除节点
        try {
            zk.delete(currentMode, -1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
