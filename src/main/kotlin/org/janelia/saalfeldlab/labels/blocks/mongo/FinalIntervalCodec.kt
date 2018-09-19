package org.janelia.saalfeldlab.labels.blocks.mongo

import net.imglib2.FinalInterval
import net.imglib2.Interval
import org.bson.BsonReader
import org.bson.BsonWriter
import org.bson.codecs.Codec
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext

class FinalIntervalCodec : Codec<FinalInterval> {
    override fun getEncoderClass(): Class<FinalInterval> {
        return FinalInterval::class.java;
    }

    override fun encode(writer: BsonWriter, interval: FinalInterval, context: EncoderContext) {
        interval.run {
            writer.writeInt64(min(0))
            writer.writeInt64(min(1))
            writer.writeInt64(min(2))
            writer.writeInt64(max(0))
            writer.writeInt64(max(1))
            writer.writeInt64(max(2))
        }
    }

    override fun decode(reader: BsonReader, context: DecoderContext): FinalInterval {
        return reader.run { FinalInterval.createMinMax(readInt64(), readInt64(), readInt64(), readInt64(), readInt64(), readInt64())}
    }
}