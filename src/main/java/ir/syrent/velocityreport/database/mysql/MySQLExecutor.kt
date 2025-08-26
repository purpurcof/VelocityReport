package ir.syrent.velocityreport.database.mysql

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import ir.syrent.velocityreport.database.Database
import ir.syrent.velocityreport.database.Priority
import ir.syrent.velocityreport.database.Query
import ir.syrent.velocityreport.database.Query.StatusCode
import ir.syrent.velocityreport.spigot.Ruom
import ir.syrent.velocityreport.utils.ServerVersion
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.util.*
import java.util.concurrent.*

typealias DatabaseStorage = ir.syrent.velocityreport.spigot.storage.Database

abstract class MySQLExecutor(
    private val credentials: MySQLCredentials,
    @JvmField
    protected val poolingSize: Int,
    threadFactory: ThreadFactory?
) : Database() {
    @JvmField
    protected val threadPool: ExecutorService = Executors.newCachedThreadPool(threadFactory)
    @JvmField
    protected var hikari: HikariDataSource? = null
    @JvmField
    protected var poolingUsed = 0

    protected fun connect(driverClassName: String) {
        Ruom.log("Registering MySQL database connection using $driverClassName driver path.")
        if (!ServerVersion.supports(13)) {
            val exception = runCatching { Class.forName("com.mysql.jdbc.Driver") }.exceptionOrNull()
            if (exception != null) {
                Ruom.error("Couldn't load MySQL Driver correctly, that may because you're using an outdated version of Minecraft or Java. Please open an issue on plugin's Github page if you think it's plugin's problem.")
                Ruom.error("Error message: ${exception.message}")
                Ruom.error("Full error message:")
                exception.printStackTrace()
            }
        }

        /*type: SQLite
mysql:
    address: localhost
    port: 3306
    database: velocityreport
    username: root
    password: ""
    ssl: false
    pooling_size: 5
    max_lifetime: 1800000
    idle_timeout: 600000
    connection_timeout: 30000
    validation_timeout: 5000
    initialization_fail_timeout: 30000
    minimum_idle: 3
    auto_commit: true
    is_register_mbeans: false
    leak_detection_threshold: 60000*/

        val hikariConfig = HikariConfig()
        hikariConfig.jdbcUrl = credentials.url
        hikariConfig.driverClassName = driverClassName
        hikariConfig.username = credentials.username
        hikariConfig.password = credentials.password
        hikariConfig.minimumIdle = DatabaseStorage.storage.config.getInt("mysql.minimum_idle", 3)
        hikariConfig.maximumPoolSize = poolingSize.coerceAtLeast(3)
        hikariConfig.poolName = "${Ruom.getPlugin().name.lowercase()}-hikari-pool"
        hikariConfig.initializationFailTimeout = DatabaseStorage.storage.config.getLong("mysql.initialization_fail_timeout", 30000)

        hikariConfig.connectionTestQuery = "SELECT 1"
        hikariConfig.validationTimeout = DatabaseStorage.storage.config.getLong("mysql.validation_timeout", 5000)
        hikariConfig.connectionTimeout = DatabaseStorage.storage.config.getLong("mysql.connection_timeout", 30000)
        hikariConfig.idleTimeout = DatabaseStorage.storage.config.getLong("mysql.idle_timeout", 600000)
        hikariConfig.maxLifetime = DatabaseStorage.storage.config.getLong("mysql.max_lifetime", 1800000)

        hikariConfig.isAutoCommit = DatabaseStorage.storage.config.getBoolean("mysql.auto_commit", true)
        hikariConfig.isRegisterMbeans = DatabaseStorage.storage.config.getBoolean("mysql.is_register_mbeans", false)

        hikariConfig.leakDetectionThreshold = DatabaseStorage.storage.config.getLong("mysql.leak_detection_threshold", 60000)

        hikariConfig.addDataSourceProperty("socketTimeout", TimeUnit.SECONDS.toMillis(30).toString());
        hikariConfig.addDataSourceProperty("tcpKeepAlive", "true")

        hikariConfig.addDataSourceProperty("socketTimeout", TimeUnit.SECONDS.toMillis(30).toString());

        hikariConfig.addDataSourceProperty("characterEncoding", "utf8")
        hikariConfig.addDataSourceProperty("encoding", "UTF-8")
        hikariConfig.addDataSourceProperty("useUnicode", "true");

        hikariConfig.addDataSourceProperty("rewriteBatchedStatements", "true");
        hikariConfig.addDataSourceProperty("jdbcCompliantTruncation", "false");

        hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "275");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        hikari = HikariDataSource(hikariConfig)

        Ruom.runSync({
            for (priority in Priority.entries) {
                Ruom.debug("Query statements for ${priority.name}: ${queue[priority]?.toMutableList()!!.map { "${it.statement}:${it.statusCode}" }}")
            }
        }, 20, 20)
    }

    protected fun tick() {
        for (priority in Priority.entries) {
            val queries = queue[priority] ?: continue
            if (queries.isEmpty()) continue
            val removedQueries = HashSet<Query>()
            for (query in queries) {
                if (query.statusCode == StatusCode.FINISHED.code || query.statusCode == StatusCode.FAILED.code) removedQueries.add(query)
            }
            queries.removeAll(removedQueries)
            for (query in queries.toList()) {
                if (query.hasDoneRequirements() && query.statusCode != StatusCode.RUNNING.code) {
                    query.statusCode = StatusCode.RUNNING.code
                    executeQuery(query).whenComplete { statusCode, _ ->
                        query.statusCode = statusCode
                        poolingUsed--
                    }
                    poolingUsed++
                    if (poolingUsed >= poolingSize) break
                }
            }
            if (poolingUsed >= poolingSize) break
            if (queries.isNotEmpty()) break
        }
    }

    private fun executeQuery(query: Query): CompletableFuture<Int> {
        val completableFuture = CompletableFuture<Int>()
        val runnable = Runnable {
            var connection: Connection? = null
            try {
                connection = createConnection()
                if (connection == null) {
                    completableFuture.complete(StatusCode.FAILED.code)
                    return@Runnable
                }

                val preparedStatement = query.createPreparedStatement(connection)
                var resultSet: ResultSet? = null
                if (query.statement.startsWith("INSERT") ||
                    query.statement.startsWith("UPDATE") ||
                    query.statement.startsWith("DELETE") ||
                    query.statement.startsWith("ALTER") ||
                    query.statement.startsWith("CREATE")
                ) preparedStatement.executeUpdate() else resultSet = preparedStatement.executeQuery()

                query.completableFuture.complete(resultSet)
                completableFuture.complete(StatusCode.FINISHED.code)
            } catch (e: SQLException) {
                onQueryFail(query)
                Ruom.error("Failed to execute query: ${query.statement}")
                e.printStackTrace()
                query.increaseFailedAttempts()
                if (query.failedAttempts > failAttemptRemoval) {
                    completableFuture.complete(StatusCode.FINISHED.code)
                    onQueryRemoveDueToFail(query)
                } else {
                    completableFuture.complete(StatusCode.FAILED.code)
                }
            } finally {
                closeConnection(connection)
            }
        }
        threadPool.submit(runnable)
        return completableFuture
    }

    private fun createConnection(): Connection? {
        return try {
            hikari!!.connection
        } catch (e: SQLException) {
            e.printStackTrace()
            null
        }
    }

    private fun closeConnection(connection: Connection?) {
        try {
            connection!!.close()
        } catch (e: SQLException) {
            e.printStackTrace()
        }
    }

    protected abstract fun onQueryFail(query: Query)
    protected abstract fun onQueryRemoveDueToFail(query: Query)
}