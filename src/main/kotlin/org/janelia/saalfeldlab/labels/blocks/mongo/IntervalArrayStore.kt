package org.janelia.saalfeldlab.labels.blocks.mongo

import net.imglib2.Interval

data class IntervalArrayStore(val intervals: Array<out Interval>)