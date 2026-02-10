package io.example.exporter;

public final class HelloWorldExporterConfiguration {

    private boolean compactLog = true;

    /**
     * If > 0, exporter will only advance the exported position every N records.
     * For "hello world", 1 is fine. Larger values reduce controller calls.
     */
    private int ackEveryN = 1;

    public boolean isCompactLog() {
        return compactLog;
    }

    public void setCompactLog(boolean compactLog) {
        this.compactLog = compactLog;
    }

    public int getAckEveryN() {
        return ackEveryN;
    }

    public void setAckEveryN(int ackEveryN) {
        this.ackEveryN = ackEveryN;
    }
}
