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

import com.google.common.base.Strings;
import org.opencloudb.mysql.CharsetUtil;
import static org.opencloudb.util.CompressUtil.*;
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
	protected final Queue<ByteBuffer> writeQueue = new LinkedList<>();
	protected int readBufferOffset;
	protected final AtomicBoolean isClosed;
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

	public AbstractConnection(SocketChannel channel) {
		this.channel = channel;
		this.isClosed = new AtomicBoolean(false);
		this.startupTime = TimeUtil.currentTimeMillis();
		this.lastReadTime = startupTime;
		this.lastWriteTime = startupTime;
	}

	public void onRegister(Selector selector, boolean autoRead) {
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
			this.onReadData(got);
		} catch (IOException cause) {
			wrapIoException(cause);
		}
	}

	public void onWrite() {
		boolean completed = flush();
		if (completed && this.writeQueue.isEmpty()) {
			if ((this.processKey.interestOps() & SelectionKey.OP_WRITE) != 0) {
				disableWrite();
			}
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
		this.manager.addNetInBytes(got);
		// Handle MySQL protocol packet
		int offset = this.readBufferOffset, length, position = buffer.position();
		for (;;) {
			length = getPacketLength(buffer, offset);
			if (length == -1) {
				if (!buffer.hasRemaining()) {
					checkReadBuffer(buffer, offset, position);
				}
				break;
			}
			if (position >= offset + length) {
				buffer.position(offset);
				byte[] data = new byte[length];
				buffer.get(data, 0, length);
				handle(data);

				offset += length;
				if (position == offset) {
					if (this.readBufferOffset != 0) {
                        this.readBufferOffset = 0;
					}
					buffer.clear();
					break;
				} else {
                    this.readBufferOffset = offset;
					buffer.position(position);
					continue;
				}
			} else {
				if (!buffer.hasRemaining()) {
					 checkReadBuffer(buffer, offset, position);
				}
				break;
			}
		}
	}

	private ByteBuffer checkReadBuffer(ByteBuffer buffer, int offset, int position) {
		if (offset == 0) {
			if (buffer.capacity() >= maxPacketSize) {
				throw new IllegalArgumentException("Packet size over the limit.");
			}
			int size = buffer.capacity() << 1;
			size = Math.min(size, maxPacketSize);
			ByteBuffer newBuffer = allocate(size);
			buffer.position(offset);
			newBuffer.put(buffer);
			this.readBuffer = newBuffer;
			recycle(buffer);
			return newBuffer;
		} else {
			buffer.position(offset);
			buffer.compact();
			readBufferOffset = 0;
			return buffer;
		}
	}

    public void write(final ByteBuffer buffer) {
		if(isSupportCompress()) {
			final ByteBuffer newBuffer = compressMysqlPacket(buffer,
						this, this.compressUnfinishedDataQueue);
			this.writeQueue.offer(newBuffer);
		} else {
			this.writeQueue.offer(buffer);
		}

		// if async write finish event got lock before me ,then writing
		// flag is set false but not start a write request
		// so we check again
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
						this.manager.addNetOutBytes(n);
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
					this.recycle(buffer);
				}
			}
			while ((buffer = this.writeQueue.poll()) != null) {
				if (buffer.limit() == 0) {
					this.recycle(buffer);
					this.close("quit send");
					return true;
				}

				buffer.flip();
				while (buffer.hasRemaining()) {
					int n = this.channel.write(buffer);
					if (n > 0) {
						this.lastWriteTime = TimeUtil.currentTimeMillis();
						this.netOutBytes += n;
						this.manager.addNetOutBytes(n);
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
			wrapIoException(cause);
		}

		return true;
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
                this.recycle(buffer);
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
            ByteBuffer newBuffer = compressMysqlPacket(buffer,this, compressUnfinishedDataQueue);
            writeQueue.offer(newBuffer);
        } else {
            writeQueue.offer(buffer);
        }
    }

	@Override
	public void close(String reason) {
		if (!isClosed()) {
			closeSocket();
			this.isClosed.set(true);
			if (this.manager != null) {
                this.manager.removeConnection(this);
			}
			this.cleanup();
			this.isSupportCompress = false;

			// Ignore null information
			if (Strings.isNullOrEmpty(reason)) {
				return;
			}
			log.info("Close connection, reason: {}, conn: {}", reason, this);
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
		while ((buffer = writeQueue.poll()) != null) {
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

	public Queue<ByteBuffer> getWriteQueue() {
		return writeQueue;
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

}
