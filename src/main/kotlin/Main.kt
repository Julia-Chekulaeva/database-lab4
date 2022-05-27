import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import java.io.File

val sqlCreation = File("src\\main\\resources\\task1.sql").readText()
val sqlInitialisation = File("src\\main\\resources\\task2_generated.sql").readText()
val timesWithCashe = mutableListOf<Long>()
val timesWithoutCashe = mutableListOf<Long>()
const val coroutinesForSelections = 7000
const val coroutinesAll = 10000
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
    for (i in 0 until coroutinesForSelections) {
        GlobalScope.launch {
            executeStatement(selectFromVisitors(), useCashe, proxy)
            executeStatement(selectFromWaiters(), useCashe, proxy)
            totalQueries += 2
        }
    }
    for (i in coroutinesForSelections until coroutinesAll) {
        GlobalScope.launch {
            val surname = if (i % 2 == 0) "Воронцова" else "Михайлова"
            executeStatement(updateVisitors(surname), useCashe, proxy)
            executeStatement(updateWaiters(8 + i % 2), useCashe, proxy)
            totalQueries += 2
        }
    }
    while (totalQueries < coroutinesAll) {}
    println("Average time response for ${coroutinesForSelections * 2} selections and ${
        (coroutinesAll - coroutinesForSelections) * 2
    } updates: ${(if (useCashe) timesWithCashe else timesWithoutCashe).fold(0.toLong()) {
            it, prev -> prev + it
    } / coroutinesAll} ns\n")
}

private fun executeStatement(sql: String, useCash: Boolean, proxy: Proxy): String {
    val start = System.nanoTime()
    val res = proxy.executeSQL(sql, useCash)
    if (useCash)
        timesWithCashe.add(System.nanoTime() - start)
    else
        timesWithoutCashe.add(System.nanoTime() - start)
    return res
}

private fun selectFromVisitors() = "SELECT * FROM visitors WHERE name = 'Анна'"

private fun selectFromWaiters() = "SELECT * FROM waiters where address_id = 9"

private fun updateVisitors(surname: String) = "UPDATE visitors SET surname = '$surname' WHERE name = 'Анна'"

private fun updateWaiters(addressId: Int) = "UPDATE waiters SET address_id = $addressId WHERE NAME = 'Руслан'"