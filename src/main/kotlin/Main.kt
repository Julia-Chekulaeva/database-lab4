import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import java.io.File

const val selectQueriesCount = 7000
const val allQueriesCount = 10000
val sqlCreation = File("src\\main\\resources\\task1.sql").readText()
val sqlInitialisation = File("src\\main\\resources\\task2_generated.sql").readText()
val timesWithCashe = MutableList(allQueriesCount * 2) { 0.toLong() }
val timesWithoutCashe = MutableList(allQueriesCount * 2) { 0.toLong() }
@Volatile
var totalQueries = 0

fun main(args: Array<String>) {
    execute(true)
    execute(false)
}

private fun execute(useCashe: Boolean) {
    totalQueries = 0
    println("----Executing queries ${if (useCashe) "with" else "without"} cashing----")
    val proxy = Proxy()
    for (i in 0 until selectQueriesCount step 2) {
        GlobalScope.launch {
            executeStatement(selectFromVisitors(), useCashe, proxy, i)
            executeStatement(selectFromWaiters(), useCashe, proxy, i + 1)
            totalQueries += 2
        }
    }
    for (i in selectQueriesCount until allQueriesCount step 2) {
        GlobalScope.launch {
            val surname = if (i % 4 == 0) "Воронцова" else "Михайлова"
            val addressId = 8 + i % 4
            executeStatement(updateVisitors(surname), useCashe, proxy, i)
            executeStatement(updateWaiters(addressId), useCashe, proxy, i + 1)
            totalQueries += 2
        }
    }
    while (totalQueries < allQueriesCount) {}
    println("Average time response for $selectQueriesCount selections and ${
        allQueriesCount - selectQueriesCount
    } updates: ${(if (useCashe) timesWithCashe else timesWithoutCashe).fold(0.toLong()) {
            it, prev -> prev + it
    } / totalQueries} ns\n")
}

private fun executeStatement(sql: String, useCash: Boolean, proxy: Proxy, coroutineNum: Int): String {
    val start = System.nanoTime()
    val res = proxy.executeSQL(sql, useCash)
    if (useCash)
        timesWithCashe[coroutineNum] = System.nanoTime() - start
    else
        timesWithoutCashe[coroutineNum] = System.nanoTime() - start
    return res
}

private fun selectFromVisitors() = "SELECT * FROM visitors WHERE name = 'Анна'"

private fun selectFromWaiters() = "SELECT * FROM waiters where address_id = 9"

private fun updateVisitors(surname: String) = "UPDATE visitors SET surname = '$surname' WHERE name = 'Анна'"

private fun updateWaiters(addressId: Int) = "UPDATE waiters SET address_id = $addressId WHERE NAME = 'Руслан'"