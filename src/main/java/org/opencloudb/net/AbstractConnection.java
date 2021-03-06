/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opencloudb.mysql.CharsetUtil;
import static org.opencloudb.util.CompressUtil.*;

import org.opencloudb.util.Callback;
import org.opencloudb.util.IoUtil;
import org.opencloudb.util.TimeUtil;
import org.slf4j.*;

/**
 * @author mycat
 */
public abstract class AbstractConnection implements ClosableConnection {

    static final Logger log = LoggerFactory.getLogger(AbstractConnection.class);

	private static final int OP_NOT_READ = ~SelectionKey.OP_READ;
	private static final int OP_NOT_WRITE = ~SelectionKey.OP_WRITE;

	private final AtomicBoolean isClosed;
	protected long id;

	protected String host;
	protected int localPort;
	protected int port;
	protected String schema;
	protected String charset;
	protected int charsetIndex;

	protected final SocketChannel channel;
	protected SelectionKey processKey;
	protected Handler handler;

	protected int packetHeaderSize;
	protected int maxPacketSize;
	protected ByteBuffer readBuffer;
	protected ByteBuffer writeBuffer;

	// Note: it's thread-safe for processor bind mode
	private final Queue<ByteBuffer> writeQueue = new LinkedList<>();
	protected int writeQueueSize;
	protected Callback<Boolean> writeComplete;

	protected int readBufferOffset;
	protected long startupTime;
	protected long lastReadTime;
	protected long lastWriteTime;
	protected long netInBytes;
	protected long netOutBytes;
	protected int writeAttempts;
	protected boolean isSupportCompress = false;
	protected final Queue<byte[]> decompressUnfinishedDataQueue = new LinkedList<>();
	protected final Queue<byte[]> compressUnfinishedDataQueue = new LinkedList<>();

	private long idleTimeout;
	protected ConnectionManager manager;
	protected volatile NioProcessor processor;

	public AbstractConnection(SocketChannel channel) {
		this.channel = channel;
		this.isClosed = new AtomicBoolean(false);
		this.startupTime = TimeUtil.currentTimeMillis();
		this.lastReadTime = startupTime;
		this.lastWriteTime = startupTime;
	}

	public void onRegister(Selector selector, boolean autoRead) {
		NioProcessor currentProcessor = NioProcessor.ensureRunInProcessor();
		if (this.processor != null && this.processor != currentProcessor) {
			throw new IllegalStateException("Connection has been assigned to another processor");
		}
		setProcessor(currentProcessor);

		// recall here for running in current processor's selector
		// when reuse this connection from connection pool after it's released.
		try {
			if (this.processKey != null) {
				throw new IllegalStateException("Connection-"+this.id + " has registered");
			}

			int read = SelectionKey.OP_READ;
			this.processKey = this.channel.register(selector, read, this);
			onRegister(autoRead);
		} catch (IOException cause) {
			wrapIoException(cause);
		}
	}

	public void onRegister(boolean autoRead) {
		if (autoRead) onRead();
	}

	public void onRead() {
		ByteBuffer buf = this.readBuffer;
		if (buf == null) {
			buf = allocate();
			this.readBuffer = buf;
		}
		try {
			int got = this.channel.read(buf);
			onReadData(got);
		} catch (IOException cause) {
			wrapIoException(cause);
		}
	}

	public void onWrite() {
		boolean completed = flush();
		if (completed && this.writeQueue.isEmpty()) {
			try {
				if ((this.processKey.interestOps() & SelectionKey.OP_WRITE) != 0) {
					disableWrite();
				}
			} catch (Error | RuntimeException cause) {
				onWriteError(cause);
				throw cause;
			}
			onWriteSuccess();
		} else {
			if ((this.processKey.interestOps() & SelectionKey.OP_WRITE) == 0) {
				enableWrite(false);
			}
		}
	}

	private void disableWrite() {
		SelectionKey key = this.processKey;
		key.interestOps(key.interestOps() & OP_NOT_WRITE);
	}

	private void enableWrite(boolean wakeup) {
		SelectionKey key = this.processKey;
		key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
		if (wakeup) {
			this.processKey.selector().wakeup();
		}
	}

	public void disableRead() {
		SelectionKey key = this.processKey;
		key.interestOps(key.interestOps() & OP_NOT_READ);
	}

	public void enableRead() {
		SelectionKey key = this.processKey;
		key.interestOps(key.interestOps() | SelectionKey.OP_READ);
		this.processKey.selector().wakeup();
	}

	protected abstract void wrapIoException(IOException cause);

	@Override
	public String getCharset() {
		return this.charset;
	}

	public boolean setCharset(String charset) {
		// 修复PHP字符集设置错误, 如： set names 'utf8'
		if ( charset != null ) {			
			charset = charset.replace("'", "");
		}
		
		int ci = CharsetUtil.getIndex(charset);
		if (ci > 0) {
			assert charset != null;
			this.charset = charset.equalsIgnoreCase("utf8mb4")?"utf8":charset;
			this.charsetIndex = ci;
			return true;
		} else {
			return false;
		}
	}

	public void handle(byte[] data) {
        if(isSupportCompress()) {
            List<byte[]> packs = decompressMysqlPacket(data, this.decompressUnfinishedDataQueue);
            for (byte[] pack : packs) {
				if(pack.length != 0) {
                    this.handler.handle(pack);
                }
            }
        } else {
            this.handler.handle(data);
        }
	}

	public void onReadData(int got) {
		if (isClosed()) {
			return;
		}

		ByteBuffer buffer = this.readBuffer;
        this.lastReadTime = TimeUtil.currentTimeMillis();
		if (got < 0) {
			close("Stream closed");
            return;
		}
		if (got == 0 && !this.channel.isOpen()) {
			close("Channel closed");
			return;
		}

        this.netInBytes += got;
		this.processor.addNetInBytes(got);
		// Handle MySQL protocol packet
		int offset = this.readBufferOffset, length, position = buffer.position();
		for (;;) {
			length = getPacketLength(buffer, offset);
			if (length == -1) {
				// Partial packet header has been read, or buffer full
				arrangeReadBuffer(buffer, offset, position);
				break;
			}
			if (position >= offset + length) {
				buffer.position(offset);
				byte[] data = new byte[length];
				buffer.get(data, 0, length);
				// Note: it's possible that another thread enters this method when this connection released into pool
				// in handle() method and handle() complete or not. We must set buffer state first, then handle data!
				// It's safe for this connection to call the handle() method last.
				boolean noMore;
				offset += length;
				if (noMore = (position == offset)) {
					this.readBufferOffset = 0;
					buffer.clear();
				} else {
					this.readBufferOffset = offset;
					buffer.position(position);
				}
				handle(data);
				if (noMore) {
					break;
				}
				// next packet
			} else {
				arrangeReadBuffer(buffer, offset, position);
				break;
			}
		}
	}

	private ByteBuffer arrangeReadBuffer(ByteBuffer buffer, int offset, int limit) {
		if (buffer.hasRemaining()) {
			return buffer;
		}

		// Handle full buffer: buffer.limit() == limit
		if (offset == 0) {
			if (limit >= this.maxPacketSize) {
				String s = "Packet size over the max limit " + this.maxPacketSize;
				wrapIoException(new IOException(s));
			}
			int size = buffer.capacity() << 1;
			size = Math.min(size, this.maxPacketSize);
			final ByteBuffer newBuffer = allocate(size);
			buffer.position(offset);
			newBuffer.put(buffer);
			this.readBuffer = newBuffer;
			recycle(buffer);
			return newBuffer;
		} else {
			buffer.position(offset);
			buffer.compact();
			this.readBufferOffset = 0;
			return buffer;
		}
	}

    public void write(final ByteBuffer buffer) {
		if(isSupportCompress()) {
			AbstractConnection con = this;
			ByteBuffer newBuf = compressMysqlPacket(buffer, con, con.compressUnfinishedDataQueue);
			this.writeQueue.offer(newBuf);
		} else {
			this.writeQueue.offer(buffer);
		}
		this.writeQueueSize++;

		onWrite();
	}

    private boolean flush() {
		try {
			ByteBuffer buffer = this.writeBuffer;

			if (buffer != null) {
				while (buffer.hasRemaining()) {
					int n = this.channel.write(buffer);
					if (n > 0) {
						this.netOutBytes += n;
						this.processor.addNetOutBytes(n);
						this.lastWriteTime = TimeUtil.currentTimeMillis();
					} else {
						break;
					}
				}

				if (buffer.hasRemaining()) {
					this.writeAttempts++;
					return false;
				} else {
					this.writeBuffer = null;
					recycle(buffer);
				}
			}
			while ((buffer = this.writeQueue.poll()) != null) {
				this.writeQueueSize--;
				if (buffer.limit() == 0) {
					recycle(buffer);
					close("'quit' sent");
					return true;
				}

				buffer.flip();
				while (buffer.hasRemaining()) {
					int n = this.channel.write(buffer);
					if (n > 0) {
						this.lastWriteTime = TimeUtil.currentTimeMillis();
						this.netOutBytes += n;
						this.processor.addNetOutBytes(n);
						this.lastWriteTime = TimeUtil.currentTimeMillis();
					} else {
						break;
					}
				}
				if (buffer.hasRemaining()) {
					this.writeBuffer = buffer;
					this.writeAttempts++;
					return false;
				} else {
					recycle(buffer);
				}
			}
		} catch (IOException cause) {
			try {
				onWriteError(cause);
			} finally {
				wrapIoException(cause);
			}
		} catch (Error | RuntimeException cause) {
			onWriteError(cause);
			throw cause;
		}

		return true;
    }

    protected void onWriteSuccess() {
		if (this.writeComplete != null) {
			try {
				this.writeComplete.call(true, null);
			} finally {
				this.writeComplete = null;
			}
		}
	}

	protected void onWriteError(Throwable cause) {
		if (this.writeComplete != null) {
			try {
				this.writeComplete.call(null, cause);
			} finally {
				this.writeComplete = null;
			}
		}
	}

    public ByteBuffer checkWriteBuffer(ByteBuffer buffer, int capacity, boolean writeSocketIfFull) {
        if (capacity > buffer.remaining()) {
            if (writeSocketIfFull) {
                writeNotSend(buffer);
                return allocate(capacity);
            } else { // Relocate a larger buffer
                buffer.flip();
                ByteBuffer newBuf = allocate(capacity + buffer.limit() + 1);
                newBuf.put(buffer);
                recycle(buffer);
                return newBuf;
            }
        } else {
            return buffer;
        }
    }

    public ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
        int offset = 0;
        int length = src.length;
        int remaining = buffer.remaining();
        while (length > 0) {
            if (remaining >= length) {
                buffer.put(src, offset, length);
                break;
            }
            buffer.put(src, offset, remaining);
            writeNotSend(buffer);
            buffer = allocate();
            offset += remaining;
            length -= remaining;
            remaining = buffer.remaining();
        }

        return buffer;
    }

    public void write(byte[] data) {
        ByteBuffer buffer = allocate();
        buffer = writeToBuffer(data, buffer);
        write(buffer);
    }

    private void writeNotSend(ByteBuffer buffer) {
        if(isSupportCompress()) {
            ByteBuffer newBuffer = compressMysqlPacket(buffer,this, this.compressUnfinishedDataQueue);
            this.writeQueue.offer(newBuffer);
        } else {
            this.writeQueue.offer(buffer);
        }
		this.writeQueueSize++;
    }

	@Override
	public final void close(String reason) {
		final NioProcessor processor = this.processor;

		this.closeReason = reason;
		if (processor == null || !processor.isOpen()) {
			this.closeTask.run();
		} else {
			processor.execute(this.closeTask);
		}
	}

	private volatile String closeReason;
	private final Runnable closeTask = new Runnable() {
		@Override
		public void run() {
			doClose(closeReason);
		}
	};

	protected void doClose(String reason) {
		final NioProcessor processor = this.processor;
		if (processor != null && processor != NioProcessor.ensureRunInProcessor()) {
			throw new IllegalStateException("Fatal: close the connection in another processor or thread");
		}

		if (this.isClosed.compareAndSet(false, true)) {
			try {
				closeSocket();
				if (this.manager != null) {
					this.manager.removeConnection(this);
				}
			} finally {
				cleanup();
			}
			this.isSupportCompress = false;
			log.info("Close connection for reason '{}': {}", reason, this);
		}
	}

	public boolean isClosed() {
		return this.isClosed.get();
	}

    @Override
	public void idleCheck() {
		if (isIdleTimeout()) {
			log.info("Idle timeout: conn {}", this);
			close("Idle timeout");
		}
	}

	/**
	 * 清理资源
	 */
	protected void cleanup() {
		if (this.readBuffer != null) {
			recycle(this.readBuffer);
			this.readBuffer = null;
			this.readBufferOffset = 0;
		}
		if (this.writeBuffer != null) {
			recycle(this.writeBuffer);
			this.writeBuffer = null;
		}
        if(!this.decompressUnfinishedDataQueue.isEmpty()) {
			this.decompressUnfinishedDataQueue.clear();
        }
        if(!this.compressUnfinishedDataQueue.isEmpty()) {
			this.compressUnfinishedDataQueue.clear();
        }
		ByteBuffer buffer;
		while ((buffer = this.writeQueue.poll()) != null) {
			this.writeQueueSize--;
			recycle(buffer);
		}
	}

	protected final int getPacketLength(ByteBuffer buffer, int offset) {
        int headerSize = getPacketHeaderSize();
        if(isSupportCompress()) {
            headerSize = 7;
        }
        if (buffer.position() < offset + headerSize) {
            return -1;
        } else {
            int length = buffer.get(offset) & 0xff;
            length |= (buffer.get(++offset) & 0xff) << 8;
            length |= (buffer.get(++offset) & 0xff) << 16;
            return length + headerSize;
        }
	}

	@Override
	public NioProcessor getProcessor() {
		return this.processor;
	}

	@Override
	public void setProcessor(NioProcessor processor) {
		this.processor = processor;
	}

	private Queue<ByteBuffer> getWriteQueue() {
		return this.writeQueue;
	}

	private void closeSocket() {
        IoUtil.close(this.channel);
	}

	public ConnectionManager getManager () {
		return this.manager;
	}

    public void setManager(ConnectionManager manager) {
	    this.manager = manager;
		this.readBuffer = allocate();
    }

	public boolean isSupportCompress() {
		return isSupportCompress;
	}

	public void setSupportCompress(boolean isSupportCompress) {
		this.isSupportCompress = isSupportCompress;
	}
	public int getCharsetIndex() {
		return charsetIndex;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public int getLocalPort() {
		return localPort;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public void setLocalPort(int localPort) {
		this.localPort = localPort;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public boolean isIdleTimeout() {
		long lastIoTime = Math.max(this.lastWriteTime, this.lastReadTime);
		return TimeUtil.currentTimeMillis() > lastIoTime + this.idleTimeout;
	}

	public SocketChannel getChannel() {
		return channel;
	}

	public int getPacketHeaderSize() {
		return packetHeaderSize;
	}

	public void setPacketHeaderSize(int packetHeaderSize) {
		this.packetHeaderSize = packetHeaderSize;
	}

	public int getMaxPacketSize() {
		return maxPacketSize;
	}

	public void setMaxPacketSize(int maxPacketSize) {
		this.maxPacketSize = maxPacketSize;
	}

	public long getStartupTime() {
		return startupTime;
	}

	public long getLastReadTime() {
		return lastReadTime;
	}

	public long getLastWriteTime() {
		return lastWriteTime;
	}

	public long getNetInBytes() {
		return netInBytes;
	}

	public long getNetOutBytes() {
		return netOutBytes;
	}

	public int getWriteAttempts() {
		return writeAttempts;
	}

	public int getWriteQueueSize() {
		return this.writeQueueSize;
	}

	public ByteBuffer getReadBuffer() {
		return readBuffer;
	}

	public ByteBuffer allocate() {
		return this.manager.getBufferPool().allocate();
	}

	public ByteBuffer allocate(int size) {
		return this.manager.getBufferPool().allocate(size);
	}

	public final void recycle(ByteBuffer buffer) {
		this.manager.getBufferPool().recycle(buffer);
	}

	public void setHandler(Handler handler) {
		this.handler = handler;
	}

	public void writeCompleteCallback(Callback<Boolean> writeComplete) {
		this.writeComplete = writeComplete;
	}

}
