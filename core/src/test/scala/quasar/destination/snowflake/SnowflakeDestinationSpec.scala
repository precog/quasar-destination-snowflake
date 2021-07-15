/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.destination.snowflake

import slamdata.Predef._

import argonaut._, Argonaut._, ArgonautScalaz._

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.implicits.legacy.localdate._
import doobie.util.Read

import fs2.{Pull, Stream}

import java.time._

import org.specs2.matcher.MatchResult
import org.specs2.execute.AsResult
import org.specs2.specification.core.{Fragment => SFragment}

import qdata.time._

import quasar.EffectfulQSpec
import quasar.api.{Column, ColumnType}
import quasar.api.destination._
import quasar.api.push.OffsetKey
import quasar.api.resource._
import quasar.contrib.scalaz.MonadError_
import quasar.connector._
import quasar.connector.render.RenderConfig
import quasar.connector.destination.{WriteMode => QWriteMode, _}
import quasar.lib.jdbc.destination.WriteMode

import scala.concurrent.ExecutionContext.Implicits.global
import scala.sys
import scala.util.Random

import scalaz.-\/
import scalaz.syntax.show._

import shapeless._
import shapeless.ops.hlist.{Mapper, ToList}
import shapeless.ops.record.{Keys, Values}
import shapeless.record._
import shapeless.syntax.singleton._

import shims.applicativeToScalaz

object SnowflakeDestinationSpec extends EffectfulQSpec[IO] with CsvSupport {

  type Table = String

  val DM = SnowflakeDestinationModule

  def Account = sys.env.get("SNOWFLAKE_ACCOUNT") getOrElse ""
  def Password = sys.env.get("SNOWFLAKE_PASSWORD") getOrElse ""
  def User = sys.env.get("SNOWFLAKE_USER") getOrElse ""
  def Database = sys.env.get("SNOWFLAKE_DATABASE") getOrElse ""
  def Schema = sys.env.get("SNOWFLAKE_SCHEMA") getOrElse ""
  def Warehouse = sys.env.get("SNOWFLAKE_WAREHOUSE") getOrElse ""

  def shouldSkip =
    Account.isEmpty || Password.isEmpty || User.isEmpty || Database.isEmpty || Schema.isEmpty || Warehouse.isEmpty

  skipAllIf(shouldSkip)

  "initialization" should {
    "fail with malformed config when not decodable" >>* {
      val cfg = Json("malformed" := true)

      dest(cfg)(r => IO.pure {
        r match {
          case Left(DestinationError.MalformedConfiguration(_, c, _)) =>
            c must_=== jEmptyObject

          case _ => ko("Expected a malformed configuration")
      }})
    }

    "fail when unable to connect to database" >>* {
      val cfg = config.copy(accountName = "incorrect").asJson

      dest(cfg)(r => IO.pure {
        r match {
          case Left(_) =>
            ok
          case _ => ko("Expected a connection failed or access denied")
      }})
    }
  }

  "seek sinks (upsert and append)" should {
    "write after commit" >> appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(
            ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
            ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit1"))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(Column("x", ColumnType.String)), QWriteMode.Replace, events)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "baz" :: "qux" :: HNil)
        }
    }

    "write two chunks with a single commit" >> appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(
            ("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Create(List(
            ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit1"))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(Column("x", ColumnType.String)), QWriteMode.Replace, events)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "baz" :: "qux" :: HNil)

          offsets must_== List(OffsetKey.Actual.string("commit1"))
        }
    }

    "not write without a commit" >> appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Create(List(("x" ->> "quz") :: ("y" ->> "corge") :: HNil)),
          UpsertEvent.Commit("commit1"),
          UpsertEvent.Create(List(("x" ->> "baz") :: ("y" ->> "qux") :: HNil)))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(Column("x", ColumnType.String)), QWriteMode.Replace, events)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "quz" :: "corge" :: HNil)
          offsets must_== List(OffsetKey.Actual.string("commit1"))
        }
    }

    "commit twice in a row" >> appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Commit("commit1"),
          UpsertEvent.Commit("commit2"))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(Column("x", ColumnType.String)), QWriteMode.Replace, events)
        } yield {
          values must_== List("foo" :: "bar" :: HNil)
          offsets must_== List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2"))
        }
    }

    "upsert updates rows with string typed primary key on append" >>* {
      Consumer.upsert[String :: String :: HNil]().use { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
                ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"))

        val append =
          Stream(
            UpsertEvent.Delete(Ids.StringIds(List("foo"))),
            UpsertEvent.Create(
              List(
                ("x" ->> "foo") :: ("y" ->> "check0") :: HNil,
                ("x" ->> "bar") :: ("y" ->> "check1") :: HNil)),
            UpsertEvent.Commit("commit2"))

        for {
          tbl <- freshTableName
          (values1, offsets1) <- consumer(tbl, Some(Column("x", ColumnType.String)), QWriteMode.Replace, events)
          (values2, offsets2) <- consumer(tbl, Some(Column("x", ColumnType.String)), QWriteMode.Append, append)
        } yield {
          offsets1 must_== List(OffsetKey.Actual.string("commit1"))
          offsets2 must_== List(OffsetKey.Actual.string("commit2"))
          Set(values1:_*) must_== Set("foo" :: "bar" :: HNil, "baz" :: "qux" :: HNil)
          Set(values2:_*) must_== Set("baz" :: "qux" :: HNil, "foo" :: "check0" :: HNil, "bar" :: "check1" :: HNil)
        }
      }
    }

    "upsert empty deletes without failing" >>* {
      Consumer.upsert[String :: String :: HNil]().use { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
                ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"),
            UpsertEvent.Delete(Ids.StringIds(List())),
            UpsertEvent.Commit("commit2"))

        for {
          tbl <- freshTableName
          (values, offsets) <- consumer(tbl, Some(Column("x", ColumnType.String)), QWriteMode.Replace, events)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "baz" :: "qux" :: HNil)

          offsets must_== List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2"))
        }
      }
    }

    "creates table and then appends" >> appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events1 =
        Stream(
          UpsertEvent.Create(List(("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Commit("commit1"))

      val events2 =
        Stream(
          UpsertEvent.Create(List(("x" ->> "bar") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit2"))

      for {
        tbl <- freshTableName

        _ <- consumer(tbl, toOpt(Column("x", ColumnType.String)), QWriteMode.Replace, events1)

        (values, _) <- consumer(tbl, Some(Column("x", ColumnType.String)), QWriteMode.Append, events2)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "bar" :: "qux" :: HNil)
        }
    }
  }

  "csv sink" should {
    "reject empty paths with NotAResource" >>* {
      csv(config) { sink =>
        val p = ResourcePath.root()
        val (_, pipe) = sink.consume(p, NonEmptyList.one(Column("a", ColumnType.Boolean)))
        val r = pipe(Stream.empty).compile.drain

        MRE.attempt(r).map(_ must beLike {
          case -\/(ResourceError.NotAResource(p2)) => p2 must_=== p
        })
      }
    }

    "reject paths with > 1 segments with NotAResource" >>* {
      csv(config) { sink =>
        val p = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")
        val (_, pipe) = sink.consume(p, NonEmptyList.one(Column("a", ColumnType.Boolean)))
        val r = pipe(Stream.empty).compile.drain

        MRE.attempt(r).map(_ must beLike {
          case -\/(ResourceError.NotAResource(p2)) => p2 must_=== p
        })
      }
    }

    "reject OffsetDate columns" >>* {
      val MinOffsetDate = OffsetDate(LocalDate.MIN, ZoneOffset.MIN)
      val MaxOffsetDate = OffsetDate(LocalDate.MAX, ZoneOffset.MAX)

      freshTableName flatMap { table =>
        val rs = Stream(("min" ->> MinOffsetDate) :: ("max" ->> MaxOffsetDate) :: HNil)

        csv(config)(drainAndSelectAs[IO, String :: String :: HNil](config.jdbcUri, table, _, rs))
          .attempt
          .map(_ must beLeft.like {
            case ColumnTypesNotSupported(ts) => ts.toList must contain(ColumnType.OffsetDate: ColumnType)
          })
      }
    }

    "quote table names to prevent injection" >>* {
      csv(config) { sink =>
        val recs = List(("x" ->> "a") :: ("y" ->> "b") :: HNil)

        drainAndSelect(
          config.jdbcUri,
          "foobar; drop table really_important; create table haha",
          sink,
          Stream.emits(recs)
        ).map(_ must_=== recs)
      }
    }

    "support table names containing double quote" >>* {
      csv(config) { sink =>
        val recs = List(("x" ->> "934.23") :: ("y" ->> "1234424.1239847") :: HNil)

        drainAndSelect(
          config.jdbcUri,
          """the "table" name""",
          sink,
          Stream.emits(recs)
        ).map(_ must_=== recs)
      }
    }

    "quote column names to prevent injection" >>* {
      mustRoundtrip(("from nowhere; drop table super_mission_critical; select *" ->> "42") :: HNil)
    }

    "support columns names containing a double quote" >>* {
      mustRoundtrip(("""the "column" name""" ->> "76") :: HNil)
    }

    "create an empty table when input is empty" >>* {
      csv(config) { sink =>
        type R = Record.`"a" -> String, "b" -> Int, "c" -> LocalDate`.T

        for {
          tbl <- freshTableName
          r <- drainAndSelect(config.jdbcUri, tbl, sink, Stream.empty.covaryAll[IO, R])
        } yield r must beEmpty
      }
    }

    "create a row for each line of input" >>* mustRoundtrip(
      ("foo" ->> 1.0) :: ("bar" ->> "baz1") :: ("quux" ->> 34.342) :: HNil,
      ("foo" ->> 2.0) :: ("bar" ->> "baz2") :: ("quux" ->> 35.342) :: HNil,
      ("foo" ->> 3.0) :: ("bar" ->> "baz3") :: ("quux" ->> 36.342) :: HNil)

    "overwrite any existing table with the same name with WriteMode.Replace" >>* {
      csv(config.copy(writeMode = WriteMode.Replace.some)) { sink =>
        val r1 = ("x" ->> 1.0) :: ("y" ->> "two") :: ("z" ->> 3.0) :: HNil
        val r2 = ("a" ->> "b") :: ("c" ->> "d") :: HNil

        for {
          tbl <- freshTableName
          res1 <- drainAndSelect(config.jdbcUri, tbl, sink, Stream(r1))
          res2 <- drainAndSelect(config.jdbcUri, tbl, sink, Stream(r2))
        } yield (res1 must_=== List(r1)) and (res2 must_=== List(r2))
      }
    }

    "overwrite any existing table with the same name with WriteMode.Truncate" >>* {
      csv(config.copy(writeMode = WriteMode.Truncate.some)) { sink =>
        val r1 = ("x" ->> 1.0) :: ("y" ->> "two") :: ("z" ->> 3.0) :: HNil
        val r2 = ("x" ->> 2.0) :: ("y" ->> "skldfj") :: ("z" ->> 3.0) :: HNil

        for {
          tbl <- freshTableName
          res1 <- drainAndSelect(config.jdbcUri, tbl, sink, Stream(r1))
          res2 <- drainAndSelect(config.jdbcUri, tbl, sink, Stream(r2))
        } yield (res1 must_=== List(r1)) and (res2 must_=== List(r2))
      }
    }

    "append to existing table with WriteMode.Append" >>* {
      csv(config.copy(writeMode = WriteMode.Append.some)) { sink =>
        val r1 = ("x" ->> 1.0) :: ("y" ->> "two") :: ("z" ->> 3.0) :: HNil
        val r2 = ("x" ->> 2.0) :: ("y" ->> "skldfj") :: ("z" ->> 3.0) :: HNil

        for {
          tbl <- freshTableName
          res1 <- drainAndSelect(config.jdbcUri, tbl, sink, Stream(r1))
          res2 <- drainAndSelect(config.jdbcUri, tbl, sink, Stream(r2))
        } yield (res1 must_=== List(r1)) and (res2 must_=== List(r1) ++ List(r2))
      }
    }

    "create new table with WriteMode.Create" >>* {
      csv(config.copy(writeMode = WriteMode.Create.some)) { sink =>
        val r1 = ("x" ->> 1.0) :: ("y" ->> "two") :: ("z" ->> 3.0) :: HNil

        for {
          tbl <- freshTableName
          res1 <- drainAndSelect(config.jdbcUri, tbl, sink, Stream(r1))
        } yield (res1 must_=== List(r1))
      }
    }

    "create new table in user-defined schema with WriteMode.Create" >>* {
      val schema = "myschema"

      csv(config.copy(schema = schema.some, writeMode = WriteMode.Create.some)) { sink =>
        val r1 = ("x" ->> 1.0) :: ("y" ->> "two") :: ("z" ->> 3.0) :: HNil

        for {
          tbl <- freshTableName
          res1 <- drainAndSelect(config.jdbcUri, tbl, sink, Stream(r1), schema)
        } yield (res1 must_=== List(r1))
      }
    }

    "error when table already exists in public schema with WriteMode.Create" >>* {
      val action = csv(config.copy(writeMode = WriteMode.Create.some)) { sink =>
        val r1 = ("x" ->> 1.0) :: ("y" ->> "two") :: ("z" ->> 3.0) :: HNil
        val r2 = ("a" ->> "b") :: ("c" ->> "d") :: HNil

        for {
          tbl <- freshTableName
          _ <- drainAndSelect(config.jdbcUri, tbl, sink, Stream(r1))
          res2 <- drainAndSelect(config.jdbcUri, tbl, sink, Stream(r2))
        } yield ()
      }

      action
        .attempt
        .map(_ must beLeft)
    }

    "error when table already exists in user-provided schema with WriteMode.Create" >>* {
      val schema = "myschema"

      val action = csv(config.copy(schema = schema.some, writeMode = WriteMode.Create.some)) { sink =>
        val r1 = ("x" ->> 1.0) :: ("y" ->> "two") :: ("z" ->> 3.0) :: HNil
        val r2 = ("a" ->> "b") :: ("c" ->> "d") :: HNil

        for {
          tbl <- freshTableName
          _ <- drainAndSelect(config.jdbcUri, tbl, sink, Stream(r1), schema)
          res2 <- drainAndSelect(config.jdbcUri, tbl, sink, Stream(r2), schema)
        } yield ()
      }

      action
        .attempt
        .map(_ must beLeft)
    }

  }


  def config: SnowflakeConfig = SnowflakeConfig(
      writeMode = None,
      accountName = Account,
      user = User,
      password = Password,
      databaseName = Database,
      schema = Schema.some,
      warehouse = Warehouse,
      sanitizeIdentifiers = Some(true),
      retryTransactionTimeoutMs = Some(0),
      maxTransactionReattempts = Some(0))

  val hygienicIdent: String => String = inp => QueryGen.sanitizeIdentifier(inp, true)

  implicit val CS: ContextShift[IO] = IO.contextShift(global)

  implicit val TM: Timer[IO] = IO.timer(global)

  implicit val MRE: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  object renderRow extends renderForCsv(RenderConfig.Csv())

  val randomAlphaNum: IO[String] =
    IO(Random.alphanumeric.take(6).mkString)

  val freshTableName: IO[String] =
    randomAlphaNum map { s => s"precog_test_$s" }

  def runDb[F[_]: Async: ContextShift, A](fa: ConnectionIO[A], uri: String = config.jdbcUri): F[A] =
    fa.transact(Transactor.fromDriverManager[F]("net.snowflake.client.jdbc.SnowflakeDriver", s"$uri&USER=$User&PASSWORD=$Password"))

  def csv[A](cfg: SnowflakeConfig)(f: ResultSink.CreateSink[IO, ColumnType.Scalar, Byte] => IO[A]): IO[A] =
    dest(cfg.asJson) {
      case Left(err) =>
        IO.raiseError(new RuntimeException(err.shows))

      case Right(dst) =>
        dst.sinks.toList
          .collectFirst { case c @ ResultSink.CreateSink(_) => c }
          .map(s => f(s.asInstanceOf[ResultSink.CreateSink[IO, ColumnType.Scalar, Byte]]))
          .getOrElse(IO.raiseError(new RuntimeException("No CSV sink found!")))
    }

  def upsertCsv[A](cfg: Json)(f: ResultSink.UpsertSink[IO, ColumnType.Scalar, Byte] => IO[A]): IO[A] =
    dest(cfg) {
      case Left(err) =>
        IO.raiseError(new RuntimeException(err.shows))

      case Right(dst) =>
        dst.sinks.toList
          .collectFirst { case c @ ResultSink.UpsertSink(_) => c }
          .map(s => f(s.asInstanceOf[ResultSink.UpsertSink[IO, ColumnType.Scalar, Byte]]))
          .getOrElse(IO.raiseError(new RuntimeException("No upsert CSV sink found!")))
    }

  def appendSink[A](cfg: Json)(f: ResultSink.AppendSink[IO, ColumnType.Scalar] => IO[A]): IO[A] =
    dest(cfg) {
      case Left(err) =>
        IO.raiseError(new RuntimeException(err.shows))

      case Right(dst) =>
        dst.sinks.toList
          .collectFirst { case c @ ResultSink.AppendSink(_) => c }
          .map(s => f(s.asInstanceOf[ResultSink.AppendSink[IO, ColumnType.Scalar]]))
          .getOrElse(IO.raiseError(new RuntimeException("No append CSV sink found!")))
    }

  def dest[A](cfg: Json)(f: Either[DM.InitErr, Destination[IO]] => IO[A]): IO[A] =
    DM.destination[IO](cfg, _ => _ => Stream.empty, _ => IO.pure(None)).use(f)

  trait Consumer[A]{
    def apply[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
        table: Table,
        idColumn: Option[Column[ColumnType.Scalar]],
        writeMode: QWriteMode,
        records: Stream[IO, UpsertEvent[R]])(
        implicit
        read: Read[A],
        keys: Keys.Aux[R, K],
        values: Values.Aux[R, V],
        getTypes: Mapper.Aux[asColumnType.type, V, T],
        rrow: Mapper.Aux[renderRow.type, V, S],
        ktl: ToList[K, String],
        vtl: ToList[S, String],
        ttl: ToList[T, ColumnType.Scalar])
        : IO[(List[A], List[OffsetKey.Actual[String]])]
  }

  object Consumer {
    def upsert[A](cfg: SnowflakeConfig = config): Resource[IO, Consumer[A]] = {
      val rsink: Resource[IO, ResultSink.UpsertSink[IO, ColumnType.Scalar, Byte]] =
        DM.destination[IO](cfg.asJson, _ => _ => Stream.empty, _ => IO.pure(None)) evalMap {
          case Left(err) =>
            IO.raiseError(new RuntimeException(err.shows))
          case Right(dst) =>
            val optSink = dst.sinks.toList.collectFirst { case c @ ResultSink.UpsertSink(_) => c }
            optSink match {
              case Some(s) => s.asInstanceOf[ResultSink.UpsertSink[IO, ColumnType.Scalar, Byte]].pure[IO]
              case None => IO.raiseError(new RuntimeException("No upsert sink found"))
            }
        }
      rsink map { sink => new Consumer[A] {
        def apply[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
            table: Table,
            idColumn: Option[Column[ColumnType.Scalar]],
            writeMode: QWriteMode,
            records: Stream[IO, UpsertEvent[R]])(
            implicit
            read: Read[A],
            keys: Keys.Aux[R, K],
            values: Values.Aux[R, V],
            getTypes: Mapper.Aux[asColumnType.type, V, T],
            rrow: Mapper.Aux[renderRow.type, V, S],
            ktl: ToList[K, String],
            vtl: ToList[S, String],
            ttl: ToList[T, ColumnType.Scalar])
            : IO[(List[A], List[OffsetKey.Actual[String]])] =
        for {
          tableColumns <- columnsOf(records, renderRow, idColumn).compile.lastOrError

          colList = (idColumn.get :: tableColumns).map(k =>
            Fragment.const(hygienicIdent(k.name))).intercalate(fr",")

          dst = ResourcePath.root() / ResourceName(table)

          offsets <- toUpsertCsvSink(
            dst, sink, idColumn.get, writeMode, renderRow, records).compile.toList

          q = fr"SELECT" ++ colList ++ fr"FROM" ++ Fragment.const(hygienicIdent(table))

          rows <- runDb[IO, List[A]](q.query[A].to[List], cfg.jdbcUri)

        } yield (rows, offsets)
      }}
    }

    def append[A](cfg: SnowflakeConfig = config): Resource[IO, Consumer[A]] = {
      val rsink: Resource[IO, ResultSink.AppendSink[IO, ColumnType.Scalar]] =
        DM.destination[IO](cfg.asJson, _ => _ => Stream.empty, _ => IO.pure(None)) evalMap {
          case Left(err) =>
            IO.raiseError(new RuntimeException(err.shows))
          case Right(dst) =>
            val optSink = dst.sinks.toList.collectFirst { case c @ ResultSink.AppendSink(_) => c }
            optSink match {
              case Some(s) => s.asInstanceOf[ResultSink.AppendSink[IO, ColumnType.Scalar]].pure[IO]
              case None => IO.raiseError(new RuntimeException("No append sink found"))
            }
        }
      rsink map { sink => new Consumer[A] {
        def apply[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
            table: Table,
            idColumn: Option[Column[ColumnType.Scalar]],
            writeMode: QWriteMode,
            records: Stream[IO, UpsertEvent[R]])(
            implicit
            read: Read[A],
            keys: Keys.Aux[R, K],
            values: Values.Aux[R, V],
            getTypes: Mapper.Aux[asColumnType.type, V, T],
            rrow: Mapper.Aux[renderRow.type, V, S],
            ktl: ToList[K, String],
            vtl: ToList[S, String],
            ttl: ToList[T, ColumnType.Scalar])
            : IO[(List[A], List[OffsetKey.Actual[String]])] =
        for {
          tableColumns <- columnsOf(records, renderRow, idColumn).compile.lastOrError

          colList = (idColumn.toList ++ tableColumns).map(k =>
            Fragment.const(hygienicIdent(k.name))).intercalate(fr",")

          dst = ResourcePath.root() / ResourceName(table)

          offsets <- toAppendCsvSink(
            dst, sink, idColumn, writeMode, renderRow, records).compile.toList

          q = fr"SELECT" ++ colList ++ fr"FROM" ++ Fragment.const(hygienicIdent(table))

          rows <- runDb[IO, List[A]](q.query[A].to[List], cfg.jdbcUri)

        } yield (rows, offsets)
      }
    }}
  }

  object appendAndUpsert {
    def apply[A]: PartiallyApplied[A] = new PartiallyApplied[A]

    final class PartiallyApplied[A] {
      type MkOption = Column[ColumnType.Scalar] => Option[Column[ColumnType.Scalar]]
      def apply[R: AsResult](
          f: (MkOption, Consumer[A]) => IO[R])
          : SFragment = {
        "upsert" >>* Consumer.upsert[A]().use(f(Some(_), _))
        "append" >>* Consumer.append[A]().use(f(Some(_), _))
        "no-id" >>* Consumer.append[A]().use(f(x => None, _))
      }
    }
  }

  object idOnly {
    def apply[A]: PartiallyApplied[A] = new PartiallyApplied[A]

    final class PartiallyApplied[A] {
      type MkOption = Column[ColumnType.Scalar] => Option[Column[ColumnType.Scalar]]
      def apply[R: AsResult](
          f: (MkOption, Consumer[A]) => IO[R])
          : SFragment = {
        "upsert" >>* Consumer.upsert[A]().use(f(Some(_), _))
        "append" >>* Consumer.append[A]().use(f(Some(_), _))
      }
    }
  }

  def drainAndSelect[F[_]: Async: ContextShift, R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      connectionUri: String,
      table: Table,
      sink: ResultSink.CreateSink[F, ColumnType.Scalar, Byte],
      records: Stream[F, R],
      schema: String = "public")(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      read: Read[V],
      getTypes: Mapper.Aux[asColumnType.type, V, T],
      rrow: Mapper.Aux[renderRow.type, V, S],
      ktl: ToList[K, String],
      vtl: ToList[S, String],
      ttl: ToList[T, ColumnType.Scalar])
      : F[List[V]] =
    drainAndSelectAs[F, V](connectionUri, table, sink, records, schema)

  object drainAndSelectAs {
    def apply[F[_], A]: PartiallyApplied[F, A] =
      new PartiallyApplied[F, A]

    final class PartiallyApplied[F[_], A]() {
      def apply[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
          connectionUri: String,
          table: Table,
          sink: ResultSink.CreateSink[F, ColumnType.Scalar, Byte],
          records: Stream[F, R],
          schema: String = "public")(
          implicit
          async: Async[F],
          cshift: ContextShift[F],
          read: Read[A],
          keys: Keys.Aux[R, K],
          values: Values.Aux[R, V],
          getTypes: Mapper.Aux[asColumnType.type, V, T],
          rrow: Mapper.Aux[renderRow.type, V, S],
          ktl: ToList[K, String],
          vtl: ToList[S, String],
          ttl: ToList[T, ColumnType.Scalar])
          : F[List[A]] = {

        val dst = ResourcePath.root() / ResourceName(table)

        val go = records.pull.peek1 flatMap {
          case Some((r, rs)) =>
            val colList =
              r.keys.toList
                .map(k => Fragment.const(hygienicIdent(k)))
                .intercalate(fr",")

            val createSchema = fr"CREATE SCHEMA IF NOT EXISTS" ++
              Fragment.const(hygienicIdent(schema))

            val q = fr"SELECT" ++
              colList ++
              fr"FROM" ++
              Fragment.const(hygienicIdent(schema)) ++
              fr0"." ++
              Fragment.const(hygienicIdent(table))

            val run = for {
              _ <- runDb[F, Int](createSchema.update.run, connectionUri)
              _ <- toCsvSink(dst, sink, renderRow, rs).compile.drain
              rows <- runDb[F, List[A]](q.query[A].to[List], connectionUri)
            } yield rows

            Pull.eval(run).flatMap(Pull.output1)

          case None =>
            Pull.done
        }

        go.stream.compile.last.map(_ getOrElse Nil)
      }
    }
  }

  def loadAndRetrieve[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      record: R,
      records: R*)(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      read: Read[V],
      getTypes: Mapper.Aux[asColumnType.type, V, T],
      rrow: Mapper.Aux[renderRow.type, V, S],
      ktl: ToList[K, String],
      vtl: ToList[S, String],
      ttl: ToList[T, ColumnType.Scalar])
      : IO[List[V]] =
    randomAlphaNum flatMap { tableSuffix =>
      val table = s"swdest_test_${tableSuffix}"
      val rs = record +: records

      csv(config)(drainAndSelect(config.jdbcUri, table, _, Stream.emits(rs)))
    }

  def mustRoundtrip[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      record: R,
      records: R*)(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      read: Read[V],
      getTypes: Mapper.Aux[asColumnType.type, V, T],
      rrow: Mapper.Aux[renderRow.type, V, S],
      ktl: ToList[K, String],
      vtl: ToList[S, String],
      ttl: ToList[T, ColumnType.Scalar])
      : IO[MatchResult[List[V]]] =
    loadAndRetrieve(record, records: _*)
      .map(_ must containTheSameElementsAs(record +: records))
}
