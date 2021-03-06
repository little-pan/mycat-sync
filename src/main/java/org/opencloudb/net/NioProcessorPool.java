package org.opencloudb.net;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public class NioProcessorPool implements AutoCloseable {

	protected final String name;
	private final NioProcessor[] processors;
	private final AtomicInteger nextProcessor;

	public NioProcessorPool(String name, int poolSize) throws IOException {
		this.name = name;
		this.processors = new NioProcessor[poolSize];
		boolean failed = true;
		try {
			for (int i = 0; i < poolSize; i++) {
				NioProcessor processor = new NioProcessor(name + "-p" + i);
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

	public String getName() {
		return this.name;
	}

	public int getPoolSize() {
		return this.processors.length;
	}

	public int getActiveCount() {
		int n = 0;
		for (NioProcessor p: this.processors) {
			if (p.isActive()) ++n;
		}
		return n;
	}

	public int getQueueSize() {
		int n = 0;
		for (NioProcessor p: this.processors) {
			n += p.getQueueSize();
		}
		return n;
	}

	public long getTaskCount() {
		int n = 0;
		for (NioProcessor p: this.processors) {
			n += p.getTaskCount();
		}
		return n;
	}

	public long getCompletedTaskCount() {
		int n = 0;
		for (NioProcessor p: this.processors) {
			n += p.getCompletedTaskCount();
		}
		return n;
	}

	/** Acquire next an available processor.
	 *
	 * @return Available processor
	 * @throws IllegalStateException No processor available
	 */
	public NioProcessor getNextProcessor() throws IllegalStateException {
		int i = this.nextProcessor.getAndIncrement();

		if (i >= this.processors.length) {
			this.nextProcessor.set(i = 0);
		}
        return getNextProcessor(i, 0);
	}

	private NioProcessor getNextProcessor(final int i, final int n) throws IllegalStateException {
		if (n >= this.processors.length) {
			throw new IllegalStateException("No processor available");
		}

		NioProcessor p = this.processors[i % this.processors.length];
		if (p.isOpen()) {
			return p;
		} else {
			return getNextProcessor(i + 1, n + 1);
		}
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

    public Collection<NioProcessor> getProcessors() {
		return Arrays.asList(this.processors);
    }

}
