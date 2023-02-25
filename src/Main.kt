import jdk.jfr.consumer.*
import java.io.File
import java.nio.file.Path
import kotlin.math.roundToInt

const val printEventStats = false
const val TOP_N = 10

private const val INDENT = "  "
private const val INDENT_NEXT = "| "
private const val CUTOFF = 0.0001

class Log(fileName: String) {
    private val out = File(fileName).printWriter()
    fun log(s: String, fileOnly: Boolean = false) {
        if (!fileOnly) println(s)
        out.println(s)
    }
    fun close() = out.close()
}

abstract class Stats {
    abstract fun add(event: RecordedEvent)

    context(Log)
    abstract fun dump()
}

class EventStats : Stats() {
    private val eventCount = HashMap<String, Int>()

    override fun add(event: RecordedEvent) {
        val name = event.eventType.name
        eventCount[name] = (eventCount[name] ?: 0) + 1
    }

    context(Log)
    override fun dump() {
        log("--- Event stats ---")
        for ((name, count) in eventCount.entries.sortedBy { it.key }) {
            log("  $name : $count events")
        }
    }
}

class LocationSize {
    var size = 0L
    val map = HashMap<String, LocationSize>()
    operator fun get(location: String): LocationSize = map.getOrPut(location) { LocationSize() }
}

context(Log)
fun LocationSize.dump(totalSize: Long, indent: String = INDENT, hasNext: Boolean = false) {
    val limit = totalSize * CUTOFF
    val a = map.entries.sortedByDescending { it.value.size }.take(TOP_N).takeWhile { it.value.size >= limit }
    for (i in a.indices) {
        val (location, ls) = a[i]
        log("$indent$location : ${fmtPercent(ls.size, totalSize)} by size", fileOnly = indent != INDENT)
        ls.dump(totalSize, indent + (if (hasNext) INDENT_NEXT else INDENT), i < a.lastIndex)
    }
}

class AllocationStats : Stats() {
    class SizeCount {
        var size = 0L
        var count = 0L
    }

    private var totalEvents = 0
    private var threadAllocations = HashMap<Long, Long>()
    private val sampledCnameSize = HashMap<String, Long>()
    private val tracedCnameSizeCount = HashMap<String, SizeCount>()
    private val locationSize = LocationSize()

    private val sampleEventTypes = setOf(
        "jdk.ObjectAllocationInNewTLAB",
        "jdk.ObjectAllocationOutsideTLAB"
    )

    override fun add(event: RecordedEvent) {
        val eventName = event.eventType.name
        if (eventName == "jdk.ThreadAllocationStatistics") {
            val thread: RecordedThread = event.getThread("thread")
            threadAllocations[thread.id] = event.getLong("allocated")
            return
        }
        if (eventName == "jdk.ObjectCount") {
            val cname: String = event.getValue<RecordedObject>("objectClass").getValue("name")
            val size = event.getLong("totalSize")
            val count = event.getLong("count")
            tracedCnameSizeCount.getOrPut(cname) { SizeCount() }.let {
                it.size += size
                it.count += count
            }
            return
        }
        if (eventName !in sampleEventTypes) return
        totalEvents++
        val size = event.getLong("allocationSize")
        val cname: String = event.getValue<RecordedObject>("objectClass").getValue("name")
        sampledCnameSize[cname] = (sampledCnameSize[cname] ?: 0L) + size
        val locations = event.stackTrace?.toLocations() ?: return
        var p = locationSize
        for (location in locations) {
            p.size += size
            p = p[location]
        }
        p.size += size
    }

    context(Log)
    override fun dump() {
        val threadAllocatedSize = threadAllocations.values.sum()
        val tracedAllocatedSize = tracedCnameSizeCount.values.sumOf { it.size }
        val tracedAllocatedCount = tracedCnameSizeCount.values.sumOf { it.count }
        val totalSampledSize = sampledCnameSize.values.sum()
        log("--- Allocation stats ---")
        val threadAllocatedStr = fmtSize(threadAllocatedSize)
        val tracedAllocatedStr = fmtSize(tracedAllocatedSize)
        val totalSampledStr = fmtSize(totalSampledSize)
        val totalLen = maxOf(threadAllocatedStr.length, tracedAllocatedStr.length, totalSampledStr.length)
        val sampledFraction = totalSampledSize.toDouble() / threadAllocatedSize
        log("  Total allocations by treads: ${threadAllocatedStr.padStart(totalLen)} bytes")
        log("  Total allocations traced   : ${tracedAllocatedStr.padStart(totalLen)} bytes in $tracedAllocatedCount allocations")
        log("  Total allocations sampled  : ${totalSampledStr.padStart(totalLen)} bytes (${(sampledFraction * 100_000).roundToInt() / 1000.0}% of all) in $totalEvents events")
        // -------------------------------
        val topTraced = tracedCnameSizeCount.entries.sortedByDescending { it.value.size }.take(TOP_N)
        val topSampled = sampledCnameSize.entries.sortedByDescending { it.value }.take(TOP_N)
        var nameLen = 0
        var sizeLen = 0
        for ((cname, sizeCount) in topTraced) {
            val sizeStr = fmtSize(sizeCount.size)
            nameLen = maxOf(nameLen, fmtCname(cname).length)
            sizeLen = maxOf(sizeLen, sizeStr.length)
        }
        for ((cname, size) in topSampled) {
            val sizeStr = fmtSize((size / sampledFraction).toLong())
            nameLen = maxOf(nameLen, fmtCname(cname).length)
            sizeLen = maxOf(sizeLen, sizeStr.length)
        }
        log("--- Top $TOP_N traced classes allocated ---")
        for ((cname, sizeCount) in topTraced) {
            val sizeStr = fmtSize(sizeCount.size)
            log("  ${fmtCname(cname).padEnd(nameLen)} : ${sizeStr.padStart(sizeLen)} bytes, ${sizeCount.count} objects")
        }
        log("--- Top $TOP_N sampled classes allocated ---")
        for ((cname, size) in topSampled) {
            val sizeStr = fmtSize((size / sampledFraction).toLong())
            log("  ${fmtCname(cname).padEnd(nameLen)} : ${sizeStr.padStart(sizeLen)} est bytes, ${fmtPercent(size, totalSampledSize).padStart(6)} by size")
        }
        log("--- Top $TOP_N sampled allocation locations ---")
        locationSize.dump(totalSampledSize)
    }
}

private fun RecordedStackTrace.toLocations() : List<String> =
    frames?.map { it.toLocation() }?.dropWhile {
        it == null || it.startsWith("java.") || it.startsWith("kotlin.")
    }?.filterNotNull()?.takeWhile {
        !it.startsWith("junit.framework.")
    } ?: emptyList()

private fun RecordedFrame.toLocation(): String? {
    val method = method ?: return null
    val typeName = method.type?.name ?: return null
    return "$typeName.${method.name}"
}

fun main(args: Array<String>) {
    val log = Log("result.txt")
    with(log) {
        analyze(args[0])
    }
    log.close()
}

context(Log)
private fun analyze(file: String) {
    log("Parsing $file")
    val rf = RecordingFile(Path.of(file))
    val stats = listOfNotNull(
        if (printEventStats) EventStats() else null,
        AllocationStats()
    )
    try {
        while (rf.hasMoreEvents()) {
            val event = rf.readEvent()
            stats.forEach { it.add(event) }
        }
    } catch (e: Throwable) {
        log("Stopped because of exception")
        e.printStackTrace(System.out)
    } finally {
        log("Parsed fully")
        rf.close()
    }
    stats.forEach { it.dump() }
}

fun fmtSize(size: Long): String = buildString {
    val s = size.toString()
    val sc = (s.length + 2) / 3
    for (k in sc - 1 downTo 0) {
        val i = (s.length - (k + 1) * 3).coerceAtLeast(0)
        val j = s.length - k * 3
        append(s.substring(i, j))
        if (k != 0) append('\'')
    }
}

fun fmtPercent(size: Long, totalSize: Long): String {
    val s = (size * 10000 / totalSize).toString().padStart(3, '0')
    val i = s.length - 2
    return "${s.substring(0, i)}.${s.substring(i)}%"
}

fun fmtCname(cname: String): String {
    var s = cname
    var dim = 0
    while (s.startsWith("[")) {
        s = s.substring(1)
        dim++
    }
    if (s.endsWith(";")) {
        check(s.startsWith("L"))
        s = s.substring(1, s.length - 1)
    }
    when (s) {
        "B" ->  s = "byte"
        "C" ->  s = "char"
        "I" ->  s = "int"
        "J" ->  s = "long"
    }
    s = s.replace('/', '.')
    while (dim > 0) {
        s += "[]"
        dim--
    }
    return s
}