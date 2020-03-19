package com.example.zookeeper_lock.zookeeper_api;

import com.example.zookeeper_lock.lock.ReentrantLockZk;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @ClassName ZookeeperApiTest
 * @Deacription 对Zookeeper的java客户端Api的使用了解
 * @Author dinggang
 * @Date 2020/3/13
 * @Version 1.0
 * @Modefied what？
 **/
public class ZookeeperApiTest {
    //zookeeper集群的ip地址和端口
    static String zkNodes = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
    /**
     * @Author dinggang
     * @Description //创建zookeeper客户端，与服务端建立连接
     * @Date 2020/3/13
     * @Param []
     * @return
     * @throws
     **/
    public static void createZookeeperClient() throws IOException, InterruptedException {
        /*
        由于Zookeeper客户端对象的建立和连接是异步执行，所以很有可能会因为Zookeeper对象尚未建立连接，
        主线程就继续执行导致的执行报错，这一点可以通过一些线程同步类辅助,保证Zookeeper连接完全建立成功后在进行后续操作
         */
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ZooKeeper zooKeeper = new ZooKeeper(zkNodes, 5000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //已建立连接
                if (watchedEvent.getState().equals(Event.KeeperState.SyncConnected)) {
                    countDownLatch.countDown();
                }
            }
        });
        countDownLatch.await();
        //do something...
    }
    /**
     * @Author dinggang
     * @Description //创建节点
     * @Date 2020/3/13
     * @Param []
     * @return void
     * @throws
     **/
    public static void createNode() throws IOException, KeeperException, InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ZooKeeper zooKeeper = new ZooKeeper(zkNodes, 5000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //已建立连接
                if (watchedEvent.getState().equals(Event.KeeperState.SyncConnected)) {
                    countDownLatch.countDown();
                }
            }
        });
        countDownLatch.await();
        //节点权限设置
        ACL acl = new ACL(ZooDefs.Perms.ALL,ZooDefs.Ids.ANYONE_ID_UNSAFE);
        List<ACL> acls = new ArrayList<ACL>();
        acls.add(acl);
        //创建节点
        zooKeeper.create("/demo", "helloworld".getBytes(), acls, CreateMode.PERSISTENT);

        System.out.println("over");

    }

    public static void setNode() {

    }

    public static void deleteNode() {

    }

    public static void addNodeWatcher() {

    }

    public static void getNode() {

    }

}
