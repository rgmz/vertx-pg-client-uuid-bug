package com.example.vertx.vertx_pg_client.uuid_reproducer

import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.pgclient.pgConnectOptionsOf
import io.vertx.kotlin.sqlclient.poolOptionsOf
import io.vertx.pgclient.PgPool
import io.vertx.sqlclient.Tuple
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withTimeout
import java.util.*
import kotlin.time.ExperimentalTime

data class Person(val id: UUID, val name: String)

@ExperimentalTime
class MainVerticle : CoroutineVerticle() {
  private lateinit var pool: PgPool

  override suspend fun start() {
    // Create the pooled client
    val connectOptions = pgConnectOptionsOf()
    val poolOptions = poolOptionsOf(maxSize = 5)
    pool = PgPool.pool(vertx, connectOptions, poolOptions)

    // Create the table.
    pool.query("""
      CREATE TABLE IF NOT EXISTS person (
        id UUID PRIMARY KEY,
        name TEXT
      );
    """.trimIndent())
      .execute()
      .await()

    // These work.
    println("Creating people...")
    insertPerson(uuid(), "Tom")
    insertPerson(uuid().toString(), "Dick")
    pool.query("INSERT INTO person (id, name) VALUES ('${dashlessUuid()}', 'Harry');")
      .execute()
      .await()

    // This doesn't.
    insertPerson(dashlessUuid(), "Sally")

    println(">>>>>>>>> PEOPLE <<<<<<<<<")
    pool.query("SELECT * FROM person;")
      .execute()
      .await()
      .forEach {
        val id = it.getValue("id")
        val name = it.getValue("name")
        println(" > id=$id, name=$name")
      }
  }

  private fun uuid() = UUID.randomUUID()
  private fun dashlessUuid() = UUID.randomUUID().toString().replace("-", "")

  private suspend fun insertPerson(id: Any, name: String) = coroutineScope {
    try {
      withTimeout(5_000L) {
        pool.preparedQuery("""INSERT INTO person (id, name) VALUES ($1, $2);""")
          .execute(Tuple.of(id, name))
          .await()
      }
    } catch (t: TimeoutCancellationException) {
      println("Failed to insert person: timed out after 10s")
    } catch (t: Throwable) {
      t.printStackTrace()
    }
  }
}
