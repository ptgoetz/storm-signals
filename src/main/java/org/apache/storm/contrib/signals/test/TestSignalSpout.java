// Copyright (c) P. Taylor Goetz (ptgoetz@gmail.com)

package org.apache.storm.contrib.signals.test;

import java.util.Map;

import org.apache.storm.contrib.signals.spout.BaseSignalSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;

@SuppressWarnings("serial")
public class TestSignalSpout extends BaseSignalSpout {

    private static final Logger LOG = LoggerFactory.getLogger(TestSignalSpout.class);

    public TestSignalSpout(String name) {
        super(name);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // TODO Auto-generated method stub
        super.open(conf, context, collector);
        LOG.info("Collector class: " + collector.getClass().getName());
    }

    @Override
    public void onSignal(byte[] data) {
        LOG.info("Received signal: " + new String(data));

    }

    @Override
    public void nextTuple() {
        // TODO Auto-generated method stub

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub

    }

}
