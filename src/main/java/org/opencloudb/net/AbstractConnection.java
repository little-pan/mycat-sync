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
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Strings;
import org.opencloudb.mysql.CharsetUtil;
import org.opencloudb.util.CompressUtil;
import org.opencloudb.util.IoUtil;
import org.opencloudb.util.TimeUtil;
import org.slf4j.*;

/**
 * @author mycat
 */
public abstract class AbstractConnection implements NIOConnection, ClosableConnection {

    static final Logger log = LoggerFactory.getLogger(AbstractConnection.class);

	protected String host;
	protected int localPort;
	protected int port;
	protected long id;
	protected volatile String charset;
	protected volatile int charsetIndex;

	protected final NetworkChannel channel;
	protected NIOHandler handler;

	protected int packetHeaderSize;
	protected int maxPacketSize;
	protected volatile ByteBuffer readBuffer;
	protected volatile ByteBuffer writeBuffer;
	protected final ConcurrentLinkedQueue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<ByteBuffer>();
	protected volatile int readBufferOffset;
	protected final AtomicBoolean isClosed;
	protected boolean isSocketClosed;
	protected long startupTime;
	protected long lastReadTime;
	protected long lastWriteTime;
	protected long netInBytes;
	protected long netOutBytes;
	protected int writeAttempts;
	protected volatile boolean isSupportCompress=false;
    protected final ConcurrentLinkedQueue<byte[]> decompressUnfinishedDataQueue = new ConcurrentLinkedQueue<>();
    protected final ConcurrentLinkedQueue<byte[]> compressUnfinishedDataQueue = new ConcurrentLinkedQueue<>();

	private long idleTimeout;
	protected ConnectionManager manager;

	public AbstractConnection(NetworkChannel channel) {
		this.channel = channel;
		this.isClosed = new AtomicBoolean(false);
		this.startupTime = TimeUtil.currentTimeMillis();
		this.lastReadTime = startupTime;
		this.lastWriteTime = startupTime;
	}

	@Override
	public String getCharset() {
		return charset;
	}

	public boolean setCharset(String charset) {
		// 修复PHP字符集设置错误, 如： set names 'utf8'
		if ( charset != null ) {			
			charset = charset.replace("'", "");
		}
		
		int ci = CharsetUtil.getIndex(charset);
		if (ci > 0) {
			this.charset = charset.equalsIgnoreCase("utf8mb4")?"utf8":charset;
			this.charsetIndex = ci;
			return true;
		} else {
			return false;
		}
	}

    public boolean isSupportCompress()
	{
		return isSupportCompress;
	}

	public void setSupportCompress(boolean isSupportCompress)
	{
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
		return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + idleTimeout;
	}

	public NetworkChannel getChannel() {
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
		ByteBuffer buffer = this.manager.getBufferPool().allocate();
		return buffer;
	}

	public final void recycle(ByteBuffer buffer) {
		this.manager.getBufferPool().recycle(buffer);
	}

	public void setHandler(NIOHandler handler) {
		this.handler = handler;
	}

	@Override
	public void handle(byte[] data) {
        if(isSupportCompress()) {
            List<byte[]> packs= CompressUtil.decompressMysqlPacket(data,decompressUnfinishedDataQueue);
            for (byte[] pack : packs) {
				if(pack.length != 0) {
                    this.handler.handle(pack);
                }
            }
        } else {
            this.handler.handle(data);
        }
	}

	public void onReadData(int got) throws IOException {
		if (isClosed()) {
			return;
		}

		ByteBuffer buffer = this.readBuffer;
        this.lastReadTime = TimeUtil.currentTimeMillis();
		if (got < 0) {
			close("stream closed");
            return;
		} else if (got == 0) {
			if (!this.channel.isOpen()) {
				close("socket closed");
				return;
			}
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
			size = (size > maxPacketSize) ? maxPacketSize : size;
			ByteBuffer newBuffer = this.manager.getBufferPool().allocate(size);
			buffer.position(offset);
			newBuffer.put(buffer);
			readBuffer = newBuffer;
			recycle(buffer);
			return newBuffer;
		} else {
			buffer.position(offset);
			buffer.compact();
			readBufferOffset = 0;
			return buffer;
		}
	}

	public void write(byte[] data) {
		ByteBuffer buffer = allocate();
		buffer = writeToBuffer(data, buffer);
		write(buffer);

	}

	private final void writeNotSend(ByteBuffer buffer) {
        if(isSupportCompress())
        {
            ByteBuffer newBuffer = CompressUtil.compressMysqlPacket(buffer,this,compressUnfinishedDataQueue);
            writeQueue.offer(newBuffer);
        }   else
        {
            writeQueue.offer(buffer);
        }
	}


    @Override
	public final void write(ByteBuffer buffer) {
        if(isSupportCompress()) {
            ByteBuffer newBuffer = CompressUtil.compressMysqlPacket(buffer,this,compressUnfinishedDataQueue);
            this.writeQueue.offer(newBuffer);
        } else {
            this.writeQueue.offer(buffer);
        }
		try {
            int n = doWrite();
            log.debug("written {} bytes in conn {}", n, this);
		} catch (Exception e) {
            log.warn("Write error", e);
			this.close("write error: " + e);
		}
	}

	private int doWrite () throws IOException {
        SocketChannel channel = (SocketChannel)this.channel;
        ByteBuffer buf = this.writeBuffer;
        int written = 0;

        if (buf != null) {
            while (buf.hasRemaining()) {
                int n = channel.write(buf);
                written += n;
                this.lastWriteTime = TimeUtil.currentTimeMillis();
                this.netOutBytes += n;
                this.manager.addNetOutBytes(n);
            }
            this.writeBuffer = null;
            this.recycle(buf);
        }

        while ((buf = this.writeQueue.poll()) != null) {
            if (buf.limit() == 0) {
                recycle(buf);
                close("quit send");
                return written;
            }

            buf.flip();
            while (buf.hasRemaining()) {
                int n = channel.write(buf);
                written += n;
                this.lastWriteTime = TimeUtil.currentTimeMillis();
                this.netOutBytes += n;
                this.manager.addNetOutBytes(n);
            }
            recycle(buf);
        }

        return written;
    }

	public ByteBuffer checkWriteBuffer(ByteBuffer buffer, int capacity,
			boolean writeSocketIfFull) {
		if (capacity > buffer.remaining()) {
			if (writeSocketIfFull) {
				writeNotSend(buffer);
				return this.manager.getBufferPool().allocate(capacity);
			} else { // Relocate a larger buffer
				buffer.flip();
				ByteBuffer newBuf = this.manager.getBufferPool().allocate(capacity + buffer.limit() + 1);
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
			} else {
				buffer.put(src, offset, remaining);
				writeNotSend(buffer);
				buffer = allocate();
				offset += remaining;
				length -= remaining;
				remaining = buffer.remaining();
				continue;
			}
		}

		return buffer;
	}

	@Override
	public void close(String reason) {
		if (!isClosed.get()) {
			closeSocket();
			isClosed.set(true);
			if (this.manager != null) {
                this.manager.removeConnection(this);
			}
			this.cleanup();
			isSupportCompress=false;

			//ignore null information
			if (Strings.isNullOrEmpty(reason)) {
				return;
			}
			log.info("Close connection, reason: {}, conn: {}", reason, this);
			if (reason.contains("connection,reason:java.net.ConnectException")) {
				throw new RuntimeException("Connection error");
			}
		}
	}

	public boolean isClosed() {
		return isClosed.get();
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
		if (readBuffer != null) {
			recycle(readBuffer);
			this.readBuffer = null;
			this.readBufferOffset = 0;
		}
		if (writeBuffer != null) {
			recycle(writeBuffer);
			this.writeBuffer = null;
		}
        if(!decompressUnfinishedDataQueue.isEmpty())
        {
            decompressUnfinishedDataQueue.clear();
        }
        if(!compressUnfinishedDataQueue.isEmpty())
        {
            compressUnfinishedDataQueue.clear();
        }
		ByteBuffer buffer = null;
		while ((buffer = writeQueue.poll()) != null) {
			recycle(buffer);
		}
	}

	protected final int getPacketLength(ByteBuffer buffer, int offset) {
        int headerSize = getPacketHeaderSize();
        if(isSupportCompress()) {
            headerSize=7;
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

	public ConcurrentLinkedQueue<ByteBuffer> getWriteQueue() {
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
		this.readBuffer = manager.getBufferPool().allocate();
    }

}
