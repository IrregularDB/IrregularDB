package compression.utility.BitBuffer;

import compression.utility.BitStream.BitStreamNew;
import jdk.jshell.spi.ExecutionControl;
import compression.utility.BitStream.BitStream;
import compression.utility.BitStream.BitStreamOld;

import java.nio.ByteBuffer;

public abstract class BitBuffer {

    // Public abstract methods:
    abstract public void writeFalseBit();

    abstract public void writeTrueBit();

    abstract public void writeIntUsingNBits(int i, int n);

    abstract public int bitsLeftInCurrentByte();

    // Public non-abstract methods:
    public final ByteBuffer getFinishedByteBuffer() {
        if (bitsLeftInCurrentByte() != 0) { // We have an unfinished byte
            handledUnfinishedByte();
        }
        if (this.getByteBuffer().hasRemaining()) { // We have allocated more bytes than needed
            shortenBufferToSizeN(this.getByteBuffer().position());
        }
        return this.getByteBuffer();
    }

    public final BitStream getBitStream(){
        return new BitStreamNew(getFinishedByteBuffer());
    }

    // Protected methods
    abstract protected ByteBuffer getByteBuffer();

    abstract protected void setByteBuffer(ByteBuffer byteBuffer);

    abstract protected void handledUnfinishedByte();

    protected final void extendBufferWithNMoreBytes(int n) {
        ByteBuffer extendedBuffer = ByteBuffer.allocate(this.getByteBuffer().capacity() + n);
        this.getByteBuffer().flip();
        extendedBuffer.put(this.getByteBuffer());
        this.setByteBuffer(extendedBuffer);
    }

    // Private methods
    private void shortenBufferToSizeN(int n) {
        ByteBuffer shortenedBuffer = ByteBuffer.allocate(n);
        this.getByteBuffer().flip();
        shortenedBuffer.put(this.getByteBuffer());
        this.setByteBuffer(shortenedBuffer);
    }}
