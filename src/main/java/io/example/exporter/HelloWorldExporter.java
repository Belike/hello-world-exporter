package io.example.exporter;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HelloWorldExporter implements Exporter {

    private Logger LOG;

    private final HelloWorldExporterConfiguration config = new HelloWorldExporterConfiguration();

    private Controller controller;
    private int partitionId = -1;
    private long countSinceAck = 0;

    @Override
    public void configure(final Context context) {
        LOG = context.getLogger();
        partitionId = context.getPartitionId();
        final HelloWorldExporterConfiguration loaded = context.getConfiguration().instantiate(HelloWorldExporterConfiguration.class);

        config.setCompactLog(loaded.isCompactLog());
        config.setAckEveryN(loaded.getAckEveryN());

        LOG.info("configure: partitionId={} compactLog={} ackEveryN={}",
                partitionId, config.isCompactLog(), config.getAckEveryN());
    }

    @Override
    public void open(final Controller controller) {
        this.controller = controller;
        LOG.info("open: partitionId={}", partitionId);
    }

    @Override
    public void export(final Record<?> record) {
        if (config.isCompactLog()) {
            LOG.info("export p={} pos={} type={} valueType={} intent={}",
                    partitionId,
                    record.getPosition(),
                    record.getRecordType(),
                    record.getValueType(),
                    record.getIntent());
        } else {
            LOG.info("export p={} pos={} key={} type={} valueType={} intent={} ts={}",
                    partitionId,
                    record.getPosition(),
                    record.getKey(),
                    record.getRecordType(),
                    record.getValueType(),
                    record.getIntent(),
                    record.getTimestamp());
        }
        countSinceAck++;
        if (countSinceAck >= config.getAckEveryN()) {
            controller.updateLastExportedRecordPosition(record.getPosition());
            countSinceAck = 0;
        }
    }

    @Override
    public void close() {
        LOG.info("close: partitionId={}", partitionId);
    }

    @Override
    public void purge() {
        LOG.info("purge: partitionId={}", partitionId);
    }
}
