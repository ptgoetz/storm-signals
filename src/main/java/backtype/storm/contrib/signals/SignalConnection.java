package backtype.storm.contrib.signals;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

public class SignalConnection implements Watcher {
	private static final Logger LOG = LoggerFactory.getLogger(SignalConnection.class);
	private static final String namespace = "storm-signals";
    private String name;
    private CuratorFramework client;
    private SignalListener listener;
    
    public SignalConnection(String name, SignalListener listener){
    	this.name = name;
    	this.listener = listener;
    }
    
    
    @SuppressWarnings("rawtypes")
    public void init(Map conf) throws Exception {
        String connectString = zkHosts(conf);
        int retryCount = ((Integer) conf.get("storm.zookeeper.retry.times"));
        int retryInterval = ((Integer) conf.get("storm.zookeeper.retry.interval"));

        this.client = CuratorFrameworkFactory.builder().namespace(namespace).connectString(connectString)
                .retryPolicy(new RetryNTimes(retryCount, retryInterval)).build();
        this.client.start();

        // create base path if necessary
        Stat stat = this.client.checkExists().usingWatcher(this).forPath(this.name);
        if (stat == null) {
            String path = this.client.create().creatingParentsIfNeeded().forPath(this.name);
            LOG.info("Created: " + path);
        }
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static String zkHosts(Map conf) {
        int zkPort = ((Integer) conf.get("storm.zookeeper.port"));
        List<String> zkServers = (List<String>) conf.get("storm.zookeeper.servers");

        Iterator<String> it = zkServers.iterator();
        StringBuffer sb = new StringBuffer();
        while (it.hasNext()) {
            sb.append(it.next());
            sb.append(":");
            sb.append(zkPort);
            if (it.hasNext()) {
                sb.append(",");
            }
        }
        return sb.toString();
    }
	
	
    @Override
    public void process(WatchedEvent we) {
        try {
            this.client.checkExists().usingWatcher(this).forPath(this.name);
            LOG.debug("Renewed watch for path {}", this.name);
        } catch (Exception ex) {
            LOG.error("Error renewing watch.", ex);
        }

        switch (we.getType()) {
        case NodeCreated:
            LOG.debug("Node created.");
            break;
        case NodeDataChanged:
            LOG.debug("Received signal.");
            try {
                this.listener.onSignal(this.client.getData().forPath(we.getPath()));
            } catch (Exception e) {
                LOG.warn("Unable to process signal.", e);
            }
            break;
        case NodeDeleted:
            LOG.debug("NodeDeleted");
            break;
        }
    }
    
    public void close(){
    	this.client.close();
    }
}
