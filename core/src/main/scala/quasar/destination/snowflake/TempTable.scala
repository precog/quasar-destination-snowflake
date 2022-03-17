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

import quasar.api.{Column, ColumnType}
import quasar.api.resource._
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.connector.destination.{WriteMode => QWriteMode}
import quasar.lib.jdbc.Slf4sLogHandler
import quasar.lib.jdbc.destination.WriteMode

import cats.~>
import cats.data.ValidatedNel
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.free.connection.commit

import fs2.Pipe

import org.slf4s.Logger

sealed trait TempTable[F[_]] {
  def ingest[A]: Pipe[F, Region[F, A], A]
}

object TempTable {
  sealed trait Builder[F[_]] {
    def build(blocker: Blocker): Resource[F, TempTable[F]]
  }

  def builder[F[_]: ConcurrentEffect: ContextShift: Timer: MonadResourceErr](
      writeMode: WriteMode,
      schema: String,
      hygienicIdent: String => String,
      retry: ConnectionIO ~> ConnectionIO,
      args: Flow.Args,
      stagingParams: StageFile.Params,
      rxa: Resource[F, Transactor[F]],
      logger: Logger)
      : F[Builder[F]] = {
    val log = Slf4sLogHandler(logger)

    def checkWriteMode(tableName: String): F[Unit] = {
      val fragment =
        fr0"SELECT count(*) as exists_flag from information_schema.tables where table_schema = '" ++
        Fragment.const0(tableName) ++ fr0"' AND table_schema = '" ++
        Fragment.const0(schema) ++ fr0"'"

      val existing =
        fragment.queryWithLogHandler[Int](log).option.map(_.exists(_ === 1))

      writeMode match {
        case WriteMode.Create => rxa use { xa => existing.transact(xa) flatMap { exists =>
          MonadResourceErr[F].raiseError(
            ResourceError.accessDenied(
              args.path,
              "Create mode is set but table exists already".some,
              none)).whenA(exists)
        }}
        case _ =>
          ().pure[F]
      }
    }

    for {
      tbl <- args.path match {
        case file /: ResourcePath.Root => file.pure[F]
        case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(args.path))
      }
      createColumnFragment <- args.columns.traverse(mkColumn(hygienicIdent, _)).fold(
        errs => Sync[F].raiseError(ColumnTypesNotSupported(errs)),
        xs => Fragments.parentheses(xs.intercalate(fr",")).pure[F])
      _ <- checkWriteMode(tbl)
      refMode <- Ref.of[F, QWriteMode](args.writeMode)
    } yield new Builder[F] {

      val tmpName = s"precog_tmp_$tbl"
      val bakName = s"precog_bak_$tbl"

      val tmpFragment =
        Fragment.const0(hygienicIdent(schema)) ++
        fr0"." ++
        Fragment.const0(hygienicIdent(tmpName))

      val bakFragment =
        Fragment.const0(hygienicIdent(schema)) ++
        fr0"." ++
        Fragment.const0(hygienicIdent(bakName))

      val tgtFragment =
        Fragment.const0(hygienicIdent(schema)) ++
        fr0"." ++
        Fragment.const0(hygienicIdent(tbl))

      def ingestFragment(sf: StageFile): Fragment =
        fr"COPY INTO" ++ tmpFragment ++ fr0" FROM " ++ sf.fragment ++
        fr""" file_format = (type = csv, skip_header = 0, field_optionally_enclosed_by = '"', escape = none, escape_unenclosed_field = none)"""

      def runFragment: Fragment => ConnectionIO[Unit] = { fr =>
        retry(fr.updateWithLogHandler(log).run.void)
      }


      def build(blocker: Blocker): Resource[F, TempTable[F]] = rxa flatMap { xa =>
        val createFragment = {
          val prefix =
            if (writeMode === WriteMode.Replace)
              fr"CREATE OR REPLACE TABLE"
            else
              fr"CREATE OR REPLACE TEMP TABLE"

          prefix ++
          tmpFragment ++
          createColumnFragment
       }

        def execFragment: Fragment => F[Unit] =
          runFragment andThen (_.transact(xa))

        val dropFragment =
          fr"DROP TABLE IF EXISTS" ++ tmpFragment

        val tempTable =
          new TempTable[F] {

            def ingest[A]: Pipe[F, Region[F, A], A] = _.flatMap { (region: Region[F, A]) =>
              region.data
                .through(StageFile.files(blocker, rxa, logger, stagingParams))
                .evalMap(sf => execFragment(ingestFragment(sf)))
                .onFinalize({
                  region.commitCount flatMap { size => persist.whenA(size > 0) }
                })
                .drain ++ region.commits
            }

            def persist = refMode.get flatMap {
              case QWriteMode.Replace =>
                persist0 >> refMode.set(QWriteMode.Append)
              case QWriteMode.Append =>
                append.transact(xa)
            }

            def persist0: F[Unit] = xa.trans.apply {
              writeMode match {
                case WriteMode.Create =>
                  createTgt >>
                  append
                case WriteMode.Replace =>
                  backupTgtIfExists >>
                  rename >>
                  dropBackupIfExists >>
                  recreate
                case WriteMode.Truncate =>
                  createTgtIfNotExists >>
                  commit >>
                  truncateTgt >>
                  append
                case WriteMode.Append =>
                  createTgtIfNotExists >>
                  commit >>
                  append
              }
            }

            def recreate: ConnectionIO[Unit] = runFragment {
              fr"CREATE TEMP TABLE" ++ tmpFragment ++ createColumnFragment
            }

            def append: ConnectionIO[Unit] = args.idColumn match {
              case None =>
                insert
              case Some(col) =>
                delete(col.name) >>
                insert
            }

            def prefixedColumns(prefix: Fragment) = {
              val colFragments = args.columns.map { (col: Column[_]) =>
                prefix ++ Fragment.const0(hygienicIdent(col.name))
              }
              colFragments.intercalate(fr",")
            }

            val allColumns =
              prefixedColumns(fr0"")

            def insert: ConnectionIO[Unit] = runFragment {
              fr"INSERT INTO" ++ tgtFragment ++ fr0" " ++ Fragments.parentheses(allColumns) ++ fr0" " ++
              fr"SELECT" ++ allColumns ++ fr" FROM" ++ tmpFragment
            }

            def delete(filterColumn: String): ConnectionIO[Unit] = runFragment {
              val whereClause =
                fr0"tgt." ++ Fragment.const0(hygienicIdent(filterColumn)) ++
                fr0" = " ++
                fr0"tmp." ++ Fragment.const0(hygienicIdent(filterColumn)) ++ fr0" "

              fr"DELETE FROM" ++ tgtFragment ++ fr" tgt" ++
              fr"USING" ++ tmpFragment ++ fr" tmp" ++
              fr"WHERE" ++ whereClause
            }

            def createTgt = runFragment {
              fr"CREATE TABLE" ++ tgtFragment ++ createColumnFragment
            }

            def backupTgtIfExists = runFragment {
              fr"ALTER TABLE" ++ tgtFragment ++ fr" RENAME TO" ++ bakFragment
            }

            def dropBackupIfExists = runFragment {
              fr"DROP TABLE IF EXISTS" ++ bakFragment
            }

            def truncateTgt = runFragment {
              fr"TRUNCATE" ++ tgtFragment
            }

            def createTgtIfNotExists = runFragment {
              fr"CREATE TABLE IF NOT EXISTS" ++ tgtFragment ++ createColumnFragment
            }

            def rename = runFragment {
              fr"ALTER TABLE" ++ tmpFragment ++ fr" RENAME TO" ++ tgtFragment
            }
          }

          val release: TempTable[F] => F[Unit] = _ =>
            Sync[F].defer(execFragment(dropFragment))

          Resource.make(tempTable.pure[F])(release) evalMap { t =>
            execFragment(createFragment) as t
          }
        }
    }
  }

  private def mkColumn(hygienicIdent: String => String, c: Column[ColumnType.Scalar])
      : ValidatedNel[ColumnType.Scalar, Fragment] =
    columnTypeToSnowflake(c.tpe)
      .map(Fragment.const(hygienicIdent(c.name)) ++ _)

  private def columnTypeToSnowflake(ct: ColumnType.Scalar)
      : ValidatedNel[ColumnType.Scalar, Fragment] =
    ct match {
      case ColumnType.Null => fr0"BYTEINT".validNel
      case ColumnType.Boolean => fr0"BOOLEAN".validNel
      case ColumnType.LocalTime => fr0"TIME".validNel
      case ot @ ColumnType.OffsetTime => ot.invalidNel
      case ColumnType.LocalDate => fr0"DATE".validNel
      case od @ ColumnType.OffsetDate => od.invalidNel
      case ColumnType.LocalDateTime => fr0"TIMESTAMP_NTZ".validNel
      case ColumnType.OffsetDateTime => fr0"TIMESTAMP_TZ".validNel
      case i @ ColumnType.Interval => i.invalidNel
      // this is an arbitrary precision and scale
      case ColumnType.Number => fr0"NUMBER(33, 3)".validNel
      case ColumnType.String => fr0"STRING".validNel
    }
}
