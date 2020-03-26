package org.opencloudb.net;

import org.opencloudb.MycatServer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class NioProcessorPool implements AutoCloseable {

	private final NioProcessor[] processors;
	private final AtomicInteger nextProcessor;

	public NioProcessorPool(String name, int poolSize)
			throws IOException {
		this.processors = new NioProcessor[poolSize];
		boolean failed = true;
		try {
			for (int i = 0; i < poolSize; i++) {
				NioProcessor processor = new NioProcessor(name + "-" + i);
				this.processors[i] = processor;
				processor.startup();
			}
			this.nextProcessor = new AtomicInteger();
			failed = false;
		} finally {
			if (failed) {
				for (NioProcessor p: this.processors) {
					if (p != null) {
						p.shutdown();
						p.join();
					}
				}
			}
		}
	}

	public NioProcessor getNextProcessor() {
        int i = this.nextProcessor.incrementAndGet();
        if (i >= processors.length) {
			this.nextProcessor.set(i = 0);
        }

        return this.processors[i];
	}

	@Override
	public void close() {
		for (NioProcessor p: this.processors) {
			if (p != null) p.shutdown();
		}
	}

    public void join() {
		for (NioProcessor p: this.processors) {
			if (p != null) p.join();
		}
    }

}
