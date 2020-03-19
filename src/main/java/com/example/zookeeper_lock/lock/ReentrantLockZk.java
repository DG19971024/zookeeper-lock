package com.example.zookeeper_lock.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @ClassName ReentrantLockZk
 * @Deacription zookeeper实现分布式可重入锁
 * @Author dinggang
 * @Date 2020/3/12
 * @Version 1.0
 * @Modefied what？
 **/
public class ReentrantLockZk {

    private static String zkNodes = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
    private ZooKeeper zooKeeper = null;
    private String lockPath = null;
    private String parentPath = "/lock";
    private int version;
    public ReentrantLockZk() throws IOException, InterruptedException, KeeperException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        zooKeeper = new ZooKeeper(zkNodes , 50000, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState().equals(Event.KeeperState.SyncConnected)) {
                    countDownLatch.countDown();
                }
            }
        });
        countDownLatch.await();
        ACL acl = new ACL(ZooDefs.Perms.ALL,ZooDefs.Ids.ANYONE_ID_UNSAFE);
        List<ACL> acls = new ArrayList<ACL>();
        acls.add(acl);
        Stat stat = zooKeeper.exists(parentPath, false);
        if (stat == null) {
            parentPath = zooKeeper.create(parentPath, "locak".getBytes(), acls, CreateMode.PERSISTENT);
        }

    }

    public void createNode() throws IOException, KeeperException, InterruptedException {
        final CountDownLatch countDownLatch=new CountDownLatch(1);
        ACL acl = new ACL(ZooDefs.Perms.ALL,ZooDefs.Ids.ANYONE_ID_UNSAFE);
        List<ACL> acls = new ArrayList<ACL>();
        acls.add(acl);
        //创建节点
        lockPath = zooKeeper.create(parentPath + "/demo", "helloworld".getBytes(), acls, CreateMode.EPHEMERAL_SEQUENTIAL);
        Stat stat = new Stat();
        zooKeeper.getData(lockPath,true, stat);
        version = stat.getVersion();
        //获取子节点列表，并设置回调方法实现所获取的判断逻辑
        zooKeeper.getChildren(parentPath, false, new LockCallBack(), countDownLatch);
        //主线程阻塞
        countDownLatch.await();
    }

    private class LockCallBack implements AsyncCallback.Children2Callback {
        @Override
        public void processResult(int i, String s, Object o, List<String> list, Stat stat) {
            CountDownLatch countDownLatch = (CountDownLatch)o;
            //遍历查询出的节点集合，这个经过实际测试发现并不是有序的，并不是按照序号大小有序返回的
            for (int j=0 ; j < list.size(); j++) {
                //遍历到当前节点
                if (lockPath.equals(parentPath+"/"+list.get(j))) {
                    try {
                        //如果当前节点是第一个，那就直接获取锁，解除主线程阻塞
                        if (j == 0) {
                            countDownLatch.countDown();
                            return;
                        } else {
                            //否则监控前一个节点的事件状态，这里使用同步方法进行获取，实际上会有一些问题，应该采用异步方法
                            stat = zooKeeper.exists(parentPath+"/"+list.get(j-1), new Watcher() {
                                @Override
                                public void process(WatchedEvent watchedEvent) {
                                    //如果事件类型为节点删除，那么就解除主线程阻塞，获取锁
                                    if (watchedEvent.getType().equals(Event.EventType.NodeDeleted)) {
                                        countDownLatch.countDown();
                                    }
                                }
                            });
                            //该情况表示前一个节点已经被删除了，直接解除主线程阻塞，表示获取到锁
                            if (stat == null) {
                                countDownLatch.countDown();
                            }

                        }

                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }
            }
        }
    }
    public void deleteNode() throws IOException, KeeperException, InterruptedException {
        zooKeeper.delete(lockPath, version);
    }

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        //测试加锁和释放锁
//        ReentrantLockZk lockZk = new ReentrantLockZk();
//        lockZk.createNode();
//        lockZk.deleteNode();

        //简单模拟秒杀场景
        SecondKill secondKill = new SecondKill();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                ReentrantLockZk lockZk = null;
                try {
                    //创建锁对象
                    lockZk = new ReentrantLockZk();
                    //阻塞直到获取锁，
                    lockZk.createNode();
                    //数量减一
                    secondKill.decrease();
                    //这行代码主要是查看加锁效果，是否会造成其他线程的阻塞等待，一定要注意不要设置时间太长，
                    //否则一旦超出创建客户端连接对象设置的50秒的过期时间，就会报错异常。
                    Thread.sleep(10000);
                    //释放锁
                    lockZk.deleteNode();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        };
        //启动101个线程进行测试，最后的输出结果应该为9899
        for (int i = 0; i <= 100; i++) {
            Thread t = new Thread(runnable);
            t.start();
        }
    }

}
