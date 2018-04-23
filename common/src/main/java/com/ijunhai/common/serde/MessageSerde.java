package com.ijunhai.common.serde;

import com.ijunhai.common.serde.util.ByteUtils;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.rocketmq.common.message.Message;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static com.ijunhai.common.serde.util.ByteUtils.INT_SIZE;

/**
 * this class can deserialize and serialize rocketmq message.
 * <p/>
 * one message struct: data + compress type[4 byte]
 * + messageCount[4 byte]
 * + preCompressLength[4 byte]
 * + data source
 * + compressedLength[4 byte]
 * + mark code [1 byte]
 * <p>
 * <p/>
 * data: the data from file or mysql, here maybe compressed if it's large enough.
 * <p/>
 * compress type: @com.ijunhai.common.serde.CompressType, it equals idx.
 * <p/>
 * preCompressLength: the data's length before compress, we need it when decompress data.
 * <p/>
 * data source: mark that which system the data come from.
 * <p/>
 * compressedLength: the data's length after compress, mark the position where the data end
 */
public class MessageSerde {

    public final static int COMPRESS_SIZE_THRESHOLD = 1024 * 1024;
    public final static int MAX_DATA_SYSTEM_SIZE = 128;
    public final static int MAX_FILE_PATH_SIZE = 4096;
    private final static byte MARK_CODE = -1;


    private static LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    private static LZ4FastDecompressor deCompressor = LZ4Factory.fastestInstance().fastDecompressor();

    public static Message serialize(String topic, String ip, String filePath, ByteBuffer messageByteBuffer, int messageCount, String dataSystem) {
        int length = messageByteBuffer.position();
        int preCompressLength, compressedLength;
        ByteBuffer byteBuffer;
        preCompressLength = messageByteBuffer.position();
        if (length >= COMPRESS_SIZE_THRESHOLD) {
            int maxCompressedLength = compressor.maxCompressedLength(length);
            byteBuffer = ByteBuffer.allocate(maxCompressedLength + 17 + MAX_DATA_SYSTEM_SIZE + MAX_FILE_PATH_SIZE);
            messageByteBuffer.flip();
            compressor.compress(messageByteBuffer, byteBuffer);
            compressedLength = byteBuffer.position();
            byteBuffer.put(ByteUtils.writeInt(CompressType.LZ4.getIdx()));
        } else {
            byteBuffer = ByteBuffer.allocate(preCompressLength + 17 + MAX_DATA_SYSTEM_SIZE + MAX_FILE_PATH_SIZE);
            messageByteBuffer.flip();
            byteBuffer.put(messageByteBuffer);
            compressedLength = byteBuffer.position();
            byteBuffer.put(ByteUtils.writeInt(CompressType.NONE.getIdx()));
        }
        byteBuffer.put(ByteUtils.writeInt(messageCount));
        byteBuffer.put(ByteUtils.writeInt(preCompressLength));

        byte[] dataSystemBytesAndFilepathBytes = new StringBuffer().append(dataSystem)
                .append("\001")
                .append(ip)
                .append("\001")
                .append(filePath == null ? "" : filePath)
                .toString().getBytes(Charset.forName("UTF-8"));
        byteBuffer.put(dataSystemBytesAndFilepathBytes);
        byteBuffer.put(ByteUtils.writeInt(compressedLength));
        byteBuffer.put(MARK_CODE);

        return new Message(topic, ByteUtils.readBytes(byteBuffer.array(), 0, byteBuffer.position()));
    }

    public static WrapMessage deserialize(Message message) {
        int offset = 0;
        byte[] messageBody = message.getBody();
        byte mark = messageBody[messageBody.length -1];
        if (mark == MARK_CODE) {
            int dataLength = ByteUtils.readInt(messageBody, messageBody.length - INT_SIZE - 1);

            byte[] data = ByteUtils.readBytes(messageBody, 0, dataLength);
            offset += dataLength;

            int compressTypeIdx = ByteUtils.readInt(messageBody, offset);
            CompressType compressType = CompressType.valueOf(compressTypeIdx);
            offset += INT_SIZE;

            int lineCount = ByteUtils.readInt(messageBody, offset);
            offset += INT_SIZE;

            int preCompressLength = ByteUtils.readInt(messageBody, offset);
            offset += INT_SIZE;

            byte[] dataSystemBytes = ByteUtils.readBytes(messageBody, offset, messageBody.length - INT_SIZE - offset - 1);
            String dataSystemAndFilePath = new String(dataSystemBytes, Charset.forName("UTF-8"));
            String[] split = dataSystemAndFilePath.split("\001");

            if (CompressType.NONE.equals(compressType)) {
                return new WrapMessage(split[0],
                        split.length > 1 ? split[1] : "",
                        split.length > 2 ? split[2] : "",
                        lineCount, data);
            } else {
                byte[] deCompressData = new byte[preCompressLength];
                deCompressor.decompress(data, deCompressData);
                return new WrapMessage(split[0],
                        split.length > 1 ? split[1] : "",
                        split.length > 2 ? split[2] : "",
                        lineCount,
                        deCompressData);
            }
        } else {
            return new WrapMessage("", "", "",1, messageBody);
        }
    }


}


