import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

class Proxy {

    private val connection = connect()

    init {
        init(connection)
    }

    private val cashe = mutableMapOf<String, String>()

    @Synchronized private fun changeCashe(key: String, value: String, addNotRemove: Boolean) {
        if (addNotRemove)
            cashe[key] = value
        else
            cashe.remove(key)
    }

    fun executeSQL(sql: String, useCashe: Boolean): String {
        val statement = connection.prepareStatement(sql)
        if (useCashe) {
            if (sql.startsWith("SELECT")) {
                return if (cashe[sql] != null) {
                    cashe[sql]!!
                } else {
                    val res = getRowsFromSQL(statement.executeQuery())
                    changeCashe(sql, res, true)
                    res
                }
            } else {
                val table = getTableFromSQL(sql, "UPDATE")
                val cashedQueries = cashe.keys.toList()
                for (query in cashedQueries) {
                    if (getTableFromSQL(query, "FROM") == table) {
                        changeCashe(query, "", false)
                    }
                }
                return statement.executeUpdate().toString()
            }
        } else {
            return if (sql.startsWith("SELECT")) {
                getRowsFromSQL(statement.executeQuery())
            } else {
                statement.executeUpdate().toString()
            }
        }
    }

    private fun getRowsFromSQL(rs: ResultSet): String {
        val columns = rs.metaData.columnCount
        val res = StringBuilder()
        while (rs.next()) {
            for (i in 1..columns) {
                res.append(String.format("%15s", rs.getString(i) + "\t"))
            }
            res.append("\n")
        }
        return res.toString()
    }

    private fun getTableFromSQL(sql: String, wordBefore: String): String {
        val split = sql.split(' ')
        return split[split.indexOf(wordBefore) + 1]
    }

    private fun connect(): Connection {
        try {
            Class.forName("org.postgresql.Driver")
            val connection = DriverManager.getConnection(
                "jdbc:postgresql://localhost/postgres",
                "postgres", "10102001"
            )
            println("Подключение успешно выполнено")
            return connection
        } catch (e: Exception) {
            println("Не удалось подключиться к базе данных")
            throw e
        }
    }

    private fun init(connection: Connection) {
        println("Creating tables")
        val query1 = connection.prepareStatement(sqlCreation)
        query1.execute()
        println("Inserting data")
        val query2 = connection.prepareStatement(sqlInitialisation)
        query2.execute()
        println("Sending queries")
    }
}