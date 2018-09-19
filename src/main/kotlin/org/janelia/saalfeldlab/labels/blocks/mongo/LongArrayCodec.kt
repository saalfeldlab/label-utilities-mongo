package org.janelia.saalfeldlab.labels.blocks.mongo

import net.imglib2.FinalInterval
import net.imglib2.Interval
import org.bson.BsonReader
import org.bson.BsonWriter
import org.bson.codecs.Codec
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import java.util.stream.LongStream

class LongArrayCodec : Codec<LongArray> {
    override fun getEncoderClass(): Class<LongArray> {
        return LongArray::class.java
    }

    override fun encode(writer: BsonWriter, data: LongArray, context: EncoderContext) {
        writer.writeInt32(data.size)
        data.forEach { writer.writeInt64(it) }
    }

    override fun decode(reader: BsonReader, context: DecoderContext): LongArray {
        val numElements = reader.readInt32()
        return LongStream
                .generate {reader.readInt64()}
                .limit(numElements.toLong())
                .toArray()
    }
}