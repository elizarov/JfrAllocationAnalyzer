import jdk.jfr.consumer.*
import java.io.File
import java.nio.file.Path

const val printEventStats = true
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
    private var totalEvents = 0
    private val cnameSize = HashMap<String, Long>()
    private val locationSize = LocationSize()

    private val eventTypes = setOf(
        "jdk.ObjectAllocationInNewTLAB",
        "jdk.ObjectAllocationOutsideTLAB"
    )

    override fun add(event: RecordedEvent) {
        if (event.eventType.name !in eventTypes) return
        totalEvents++
        val size: Int = event.getValue("allocationSize")
        val cname: String = event.getValue<RecordedObject>("objectClass").getValue("name")
        cnameSize[cname] = (cnameSize[cname] ?: 0L) + size
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
        val totalSize = cnameSize.values.sum()
        log("--- Allocation stats ---")
        log("  Total allocations traced: ${fmtSize(totalSize)} bytes in $totalEvents events")
        log("--- Top $TOP_N objects allocated ---")
        for ((cname, size) in cnameSize.entries.sortedByDescending { it.value }.take(TOP_N)) {
            log("  ${fmtCname(cname)} : ${fmtPercent(size, totalSize)} by size")
        }
        log("--- Top $TOP_N allocation locations ---")
        locationSize.dump(totalSize)
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
        log("Stopped because of $e")
    } finally {
        log("Parsed fully")
        rf.close()
    }
    stats.forEach { it.dump() }
}

fun fmtSize(size: Long): String = buildString {
    val s = size.toString()
    val sc = (s.length - 1) / 3
    for (k in sc - 1 downTo 0) {
        val i = s.length - (k + 1) * 3
        val j = s.length - k * 3
        append(s.substring(i, j))
        if (k != 0) append(' ')
    }
}

fun fmtPercent(size: Long, totalSize: Long): String =
    "${size * 10000 / totalSize / 100.0}%"

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