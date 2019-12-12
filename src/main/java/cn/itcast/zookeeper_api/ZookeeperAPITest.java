package cn.itcast.zookeeper_api;

import com.sun.security.ntlm.Client;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

public class ZookeeperAPITest {
//监听节点
    @Test
    public void watchZnode() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        String connectStr = "192.168.174.100:2181,192.168.174.110:2181,192.168.174.120:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectStr, 8000, 8000, retryPolicy);
        client.start();
//        创建一个TreeCache对象，指定要监控的节点路径
         TreeCache treeCache = new TreeCache(client, "/hello3");
//         自定义一个监听器
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                final ChildData data = treeCacheEvent.getData();
                if(data != null){
                    switch (treeCacheEvent.getType()){
                        case NODE_ADDED:
                            System.out.println("监控到有新增节点");
                            break;
                        case NODE_REMOVED:
                            System.out.println("监控到有节点被移除");
                            break;
                        case NODE_UPDATED:
                            System.out.println("节点被更新");
                            break;
                        default:
                            break;

                    }
                }

            }
        });

//        开始监听
        treeCache.start();
        Thread.sleep(10000000);
    }
//    获取节点数据
    @Test
    public void getZnodeData() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        String connectStr = "192.168.174.100:2181,192.168.174.110:2181,192.168.174.120:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectStr, 8000, 8000, retryPolicy);
        client.start();
//        获取节点数据
        final byte[] bytes = client.getData().forPath("/hello");
        System.out.println(new String(bytes));
        client.close();
    }

//   设置节点数据
    @Test
    public void setZnodeData() throws Exception {
         RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        String connectStr = "192.168.174.100:2181,192.168.174.110:2181,192.168.174.120:2181";
         CuratorFramework client = CuratorFrameworkFactory.newClient(connectStr, 8000, 8000, retryPolicy);
         client.start();
         client.setData().forPath("/hello","zookeeper".getBytes());
         client.close();

    }
    @Test
    public void createZnode() throws Exception {
//        定制一个重试策略
        /**
         * param1:重试的间隔时间
         * param2：重试的最大次数
         */
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,1);
//        获取客户端对象
        /**
         *param1: 要连接的Zookeeper服务器列表
         * param2:会话的超时时间
         * param3:链接超时时间
         * param4:重试策略
         */
        String connectStr = "192.168.174.100:2181,192.168.174.110:2181,192.168.174.120:2181";
        final CuratorFramework client = CuratorFrameworkFactory.newClient(connectStr, 8000, 8000, retryPolicy);
//        开启客户端
        client.start();
//        创建节点
        client.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hello2","world".getBytes());
//                关闭客户端
        client.close();
    }
//    创建临时节点
    @Test
    public void createTempZnode() throws Exception {
//        定制一个重试策略
        /**
         * param1:重试的间隔时间
         * param2：重试的最大次数
         */
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,1);
//        获取客户端对象
        /**
         *param1: 要连接的Zookeeper服务器列表
         * param2:会话的超时时间
         * param3:链接超时时间
         * param4:重试策略
         */
        String connectStr = "192.168.174.100:2181,192.168.174.110:2181,192.168.174.120:2181";
        final CuratorFramework client = CuratorFrameworkFactory.newClient(connectStr, 8000, 8000, retryPolicy);
//        开启客户端
        client.start();
//        创建节点
        client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/hello3","world".getBytes());
        Thread.sleep(5000);
//                关闭客户端
        client.close();
    }
}
