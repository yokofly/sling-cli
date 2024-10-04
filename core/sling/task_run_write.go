package sling

import (
	"bufio"
	"context"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// WriteToFile writes to a target file
func (t *TaskExecution) WriteToFile(cfg *Config, df *iop.Dataflow) (cnt uint64, err error) {
	var bw int64
	defer t.PBar.Finish()
	setStage("5 - load-into-final")

	if uri := cfg.TgtConn.URL(); uri != "" {
		dateMap := iop.GetISO8601DateMap(time.Now())
		cfg.TgtConn.Set(g.M("url", g.Rm(uri, dateMap)))

		if len(df.Buffer) == 0 && !cast.ToBool(os.Getenv("SLING_ALLOW_EMPTY")) {
			g.Warn("No data or records found in stream. Nothing to do. To allow Sling to create empty files, set SLING_ALLOW_EMPTY=TRUE")
			return
		}

		// construct props by merging with options
		options := g.M()
		g.Unmarshal(g.Marshal(cfg.Target.Options), &options)
		props := append(
			g.MapToKVArr(cfg.TgtConn.DataS()),
			g.MapToKVArr(g.ToMapString(options))...,
		)

		fs, err := filesys.NewFileSysClientFromURLContext(t.Context.Ctx, uri, props...)
		if err != nil {
			err = g.Error(err, "Could not obtain client for: %s", cfg.TgtConn.Type)
			return cnt, err
		}

		// apply column casing
		applyColumnCasingToDf(df, fs.FsType(), t.Config.Target.Options.ColumnCasing)

		bw, err = filesys.WriteDataflow(fs, df, uri)
		if err != nil {
			err = g.Error(err, "Could not FileSysWriteDataflow")
			return cnt, err
		}
		cnt = df.Count()
	} else if cfg.Options.StdOut {
		// apply column casing
		applyColumnCasingToDf(df, dbio.TypeFileLocal, t.Config.Target.Options.ColumnCasing)

		limit := cast.ToUint64(cfg.Source.Limit())

		// store as dataset
		if cfg.Options.Dataset {
			df.Limit = limit
			data, err := df.Collect()
			if err != nil {
				err = g.Error(err, "Could not collect dataflow")
				return cnt, err
			}
			t.data = &data
			cnt = cast.ToUint64(len(data.Rows))
			return cnt, nil
		}

		options := map[string]string{"delimiter": ","}
		g.Unmarshal(g.Marshal(cfg.Target.Options), &options)

		for stream := range df.StreamCh {
			// stream.SetConfig(options)
			// c := iop.CSV{File: os.Stdout}
			// cnt, err = c.WriteStream(stream)
			// if err != nil {
			// 	err = g.Error(err, "Could not write to Stdout")
			// 	return
			// }

			// continue

			stream.SetConfig(options)
			for batchR := range stream.NewCsvReaderChnl(cast.ToInt(limit), 0) {
				if limit > 0 && cnt >= limit {
					return
				}

				if len(batchR.Columns) != len(df.Columns) {
					err = g.Error(err, "number columns have changed, not compatible with stdout.")
					return
				}
				bufStdout := bufio.NewWriter(os.Stdout)
				bw, err = filesys.Write(batchR.Reader, bufStdout)
				bufStdout.Flush()
				if err != nil {
					err = g.Error(err, "Could not write to Stdout")
					return
				} else if err = stream.Context.Err(); err != nil {
					err = g.Error(err, "encountered stream error")
					return
				}
				cnt = cnt + uint64(batchR.Counter)
			}
		}
	} else {
		err = g.Error("target for output is not specified")
		return
	}

	g.DebugLow(
		"wrote %s: %d rows [%s r/s]",
		humanize.Bytes(cast.ToUint64(bw)), cnt, getRate(cnt),
	)
	setStage("6 - closing")

	return
}

// WriteToDb writes to a target DB
// create temp table
// load into temp table
// insert / incremental / replace into target table
func (t *TaskExecution) WriteToDb(cfg *Config, df *iop.Dataflow, tgtConn database.Connection) (cnt uint64, err error) {
	defer t.PBar.Finish()

	// detect empty
	if len(df.Columns) == 0 {
		return 0, g.Error("no stream columns detected")
	}

	allowDirectInsert := cast.ToBool(os.Getenv("SLING_ALLOW_DIRECT_INSERT"))

	return t.processDataTransfer(cfg, df, tgtConn, allowDirectInsert)
}

func (t *TaskExecution) processDataTransfer(cfg *Config, df *iop.Dataflow, tgtConn database.Connection, isDirectInsert bool) (cnt uint64, err error) {
	targetTable, err := t.setupTargetTable(cfg, tgtConn)
	if err != nil {
		return 0, err
	}

	var tempTable database.Table
	if !isDirectInsert {
		tempTable, err = t.setupTempTable(cfg, tgtConn, targetTable)
		if err != nil {
			return 0, err
		}

		// Add cleanup task for temp table
		t.AddCleanupTaskFirst(func() {
			if cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
				return
			}

			conn := tgtConn
			if tgtConn.Context().Err() != nil {
				conn, err = t.getTgtDBConn(context.Background())
				if err == nil {
					conn.Connect()
				}
			}
			g.LogError(conn.DropTable(tempTable.FullName()))
			conn.Close()
		})
	}

	defer func() {
		tgtConn.Close()
	}()

	err = t.validateColumns(cfg, df, tgtConn, targetTable)
	if err != nil {
		return 0, err
	}

	err = tgtConn.BeginContext(df.Context.Ctx)
	if err != nil {
		return 0, g.Error(err, "could not open transaction to write to table")
	}
	defer func() {
		if err != nil {
			tgtConn.Rollback()
		}
	}()

	err = t.executePreSQL(cfg, tgtConn)
	if err != nil {
		return 0, err
	}

	// Setup column change handlers for both scenarios
	if isDirectInsert {
		t.setupColumnChangeHandlers(cfg, df, tgtConn, &targetTable)
	} else {
		t.setupColumnChangeHandlers(cfg, df, tgtConn, &tempTable)
	}

	if isDirectInsert {
		cnt, err = t.insertDataDirectly(cfg, df, tgtConn, targetTable)
	} else {
		cnt, err = t.loadDataIntoTempTable(cfg, df, tgtConn, tempTable)
		if err != nil {
			return 0, err
		}

		err = t.validateTempTableData(cfg, tgtConn, tempTable, cnt)
		if err != nil {
			return 0, err
		}

		err = t.transferDataFromTempToTarget(cfg, tgtConn, targetTable, tempTable, cnt)
	}
	if err != nil {
		return 0, err
	}

	err = t.executePostSQL(cfg, tgtConn)
	if err != nil {
		return cnt, err
	}

	err = tgtConn.Commit()
	if err != nil {
		return cnt, g.Error(err, "could not commit")
	}

	return cnt, nil
}

func (t *TaskExecution) setupTargetTable(cfg *Config, tgtConn database.Connection) (database.Table, error) {
	targetTable, err := database.ParseTableName(cfg.Target.Object, tgtConn.GetType())
	if err != nil {
		return database.Table{}, g.Error(err, "could not parse object table name")
	}

	if cfg.Target.Options.TableDDL != nil {
		targetTable.DDL = *cfg.Target.Options.TableDDL
	}
	targetTable.DDL = g.R(targetTable.DDL, "object_name", targetTable.Raw, "table", targetTable.Raw)
	targetTable.SetKeys(cfg.Source.PrimaryKey(), cfg.Source.UpdateKey, cfg.Target.Options.TableKeys)

	// check table ddl
	if targetTable.DDL != "" && !strings.Contains(targetTable.DDL, targetTable.Raw) {
		return database.Table{}, g.Error("The Table DDL provided needs to contains the exact object table name: %s\nProvided:\n%s", targetTable.Raw, targetTable.DDL)
	}

	return targetTable, nil
}

func (t *TaskExecution) setupTempTable(cfg *Config, tgtConn database.Connection, targetTable database.Table) (database.Table, error) {
	var tableTmp database.Table
	var err error

	if cfg.Target.Options.TableTmp == "" {
		tableTmp, err = database.ParseTableName(cfg.Target.Object, tgtConn.GetType())
		if err != nil {
			return database.Table{}, g.Error(err, "could not parse object table name")
		}
		suffix := lo.Ternary(tgtConn.GetType().DBNameUpperCase(), "_TMP", "_tmp")
		if g.In(tgtConn.GetType(), dbio.TypeDbOracle) {
			if len(tableTmp.Name) > 24 {
				tableTmp.Name = tableTmp.Name[:24] // max is 30 chars
			}

			// some weird column / commit error, not picking up latest columns
			suffix2 := g.RandString(g.NumericRunes, 1) + g.RandString(g.AlphaNumericRunes, 1)
			suffix2 = lo.Ternary(
				tgtConn.GetType().DBNameUpperCase(),
				strings.ToUpper(suffix2),
				strings.ToLower(suffix2),
			)
			suffix = suffix + suffix2
		}

		tableTmp.Name = tableTmp.Name + suffix
		cfg.Target.Options.TableTmp = tableTmp.FullName()
	} else {
		tableTmp, err = database.ParseTableName(cfg.Target.Options.TableTmp, tgtConn.GetType())
		if err != nil {
			return database.Table{}, g.Error(err, "could not parse temp table name")
		}
	}

	// set DDL
	tableTmp.DDL = strings.Replace(targetTable.DDL, targetTable.Raw, tableTmp.FullName(), 1)
	tableTmp.Raw = tableTmp.FullName()
	err = tableTmp.SetKeys(cfg.Source.PrimaryKey(), cfg.Source.UpdateKey, cfg.Target.Options.TableKeys)
	if err != nil {
		return database.Table{}, g.Error(err, "could not set keys for "+tableTmp.FullName())
	}

	// create schema if not exist
	_, err = createSchemaIfNotExists(tgtConn, tableTmp.Schema)
	if err != nil {
		return database.Table{}, g.Error(err, "Error checking & creating schema "+tableTmp.Schema)
	}

	// Drop & Create the temp table
	err = tgtConn.DropTable(tableTmp.FullName())
	if err != nil {
		return database.Table{}, g.Error(err, "could not drop table "+tableTmp.FullName())
	}

	return tableTmp, nil
}

func (t *TaskExecution) validateColumns(cfg *Config, df *iop.Dataflow, tgtConn database.Connection, targetTable database.Table) error {
	tgtColumns, err := pullTargetTableColumns(cfg, tgtConn, true)
	if err != nil {
		return g.Error(err, "could not get column list for "+targetTable.FullName())
	}

	_, err = tgtConn.ValidateColumnNames(tgtColumns, df.Columns.Names(), true)
	if err != nil {
		return g.Error(err, "columns mismatched for table "+targetTable.FullName())
	}

	return nil
}

func (t *TaskExecution) loadDataIntoTempTable(cfg *Config, df *iop.Dataflow, tgtConn database.Connection, tempTable database.Table) (uint64, error) {
	t.SetProgress("streaming data")

	// set batch size if specified
	if batchLimit := cfg.Target.Options.BatchLimit; batchLimit != nil {
		df.SetBatchLimit(*batchLimit)
	}

	cnt, err := tgtConn.BulkImportFlow(tempTable.FullName(), df)
	if err != nil {
		tgtConn.Rollback()
		if cast.ToBool(os.Getenv("SLING_CLI")) && cfg.sourceIsFile() {
			err = g.Error(err, "could not insert into %s.", tempTable.FullName())
		} else {
			err = g.Error(err, "could not insert into "+tempTable.FullName())
		}
		return 0, err
	}

	tgtConn.Commit()
	return cnt, nil
}

func (t *TaskExecution) validateTempTableData(cfg *Config, tgtConn database.Connection, tempTable database.Table, cnt uint64) error {
	// Checksum Comparison, data quality. Limit to env var SLING_CHECKSUM_ROWS, cause sums get too high
	if val := cast.ToUint64(os.Getenv("SLING_CHECKSUM_ROWS")); val > 0 && cnt <= val {
		err := tgtConn.CompareChecksums(tempTable.FullName(), tempTable.Columns)
		if err != nil {
			return g.Error(err, "Checksum comparison failed for table "+tempTable.FullName())
		}
	}
	return nil
}

func (t *TaskExecution) executePreSQL(cfg *Config, tgtConn database.Connection) error {
	if preSQL := cfg.Target.Options.PreSQL; preSQL != nil && *preSQL != "" {
		t.SetProgress("executing pre-sql")
		_, err := tgtConn.ExecMulti(*preSQL)
		if err != nil {
			return g.Error(err, "could not execute pre-sql on target")
		}
	}
	return nil
}

func (t *TaskExecution) insertDataDirectly(cfg *Config, df *iop.Dataflow, tgtConn database.Connection, targetTable database.Table) (uint64, error) {
	t.SetProgress("streaming data directly")

	// set batch size if specified
	if batchLimit := cfg.Target.Options.BatchLimit; batchLimit != nil {
		df.SetBatchLimit(*batchLimit)
	}

	cnt, err := tgtConn.BulkImportFlow(targetTable.FullName(), df)
	if err != nil {
		tgtConn.Rollback()
		if cast.ToBool(os.Getenv("SLING_CLI")) && cfg.sourceIsFile() {
			err = g.Error(err, "could not insert into %s.", targetTable.FullName())
		} else {
			err = g.Error(err, "could not insert into "+targetTable.FullName())
		}
		return 0, err
	}

	tgtConn.Commit()
	return cnt, nil
}

func (t *TaskExecution) transferDataFromTempToTarget(cfg *Config, tgtConn database.Connection, targetTable database.Table, tempTable database.Table, cnt uint64) error {
	// Put data from tmp to final
	if cnt == 0 {
		t.SetProgress("0 rows inserted. Nothing to do.")
		return nil
	}

	switch cfg.Mode {
	case FullRefreshMode:
		// use swap
		err := tgtConn.SwapTable(tempTable.FullName(), targetTable.FullName())
		if err != nil {
			return g.Error(err, "could not swap tables %s to %s", tempTable.FullName(), targetTable.FullName())
		}
	case IncrementalMode, SnapshotMode:
		// create if not exists and insert directly
		err := insertFromTemp(cfg, tgtConn)
		if err != nil {
			return g.Error(err, "Could not insert from temp")
		}
	case TruncateMode:
		// truncate (create if not exists) and insert directly
		truncSQL := g.R(
			tgtConn.GetTemplateValue("core.truncate_table"),
			"table", targetTable.FullName(),
		)
		_, err := tgtConn.Exec(truncSQL)
		if err != nil {
			return g.Error(err, "Could not truncate table: "+targetTable.FullName())
		}
		t.SetProgress("truncated table " + targetTable.FullName())

		err = insertFromTemp(cfg, tgtConn)
		if err != nil {
			return g.Error(err, "Could not insert from temp")
		}
	case BackfillMode:
		rowAffCnt, err := tgtConn.Upsert(tempTable.FullName(), targetTable.FullName(), cfg.Source.PrimaryKey())
		if err != nil {
			return g.Error(err, "Could not incremental from temp")
		}
		if rowAffCnt > 0 {
			g.DebugLow("%d TOTAL INSERTS / UPDATES", rowAffCnt)
		}
	default:
		return g.Error("Unsupported mode: %s", cfg.Mode)
	}

	return nil
}

func (t *TaskExecution) executePostSQL(cfg *Config, tgtConn database.Connection) error {
	if postSQL := cfg.Target.Options.PostSQL; postSQL != nil && *postSQL != "" {
		t.SetProgress("executing post-sql")
		_, err := tgtConn.ExecMulti(*postSQL)
		if err != nil {
			return g.Error(err, "Error executing post-sql")
		}
	}
	return nil
}

func (t *TaskExecution) setupColumnChangeHandlers(cfg *Config, df *iop.Dataflow, tgtConn database.Connection, table *database.Table) {
	df.OnColumnChanged = func(col iop.Column) error {
		time.Sleep(300 * time.Millisecond)

		columns, err := tgtConn.GetSQLColumns(*table)
		if err != nil {
			return g.Error(err, "could not get table columns for optimization")
		}
		table.Columns = columns

		table.SetKeys(cfg.Source.PrimaryKey(), cfg.Source.UpdateKey, cfg.Target.Options.TableKeys)

		ok, err := tgtConn.OptimizeTable(table, iop.Columns{col}, false)
		if err != nil {
			return g.Error(err, "could not optimize table schema")
		} else if ok {
			cfg.Target.columns = table.Columns
			for i := range df.Columns {
				if df.Columns[i].Name == col.Name {
					df.Columns[i].Type = table.Columns[i].Type
					df.Columns[i].DbType = table.Columns[i].DbType
					for _, ds := range df.StreamMap {
						if len(ds.Columns) == len(df.Columns) {
							ds.Columns[i].Type = table.Columns[i].Type
							ds.Columns[i].DbType = table.Columns[i].DbType
						}
					}
					break
				}
			}
		}

		return nil
	}

	if *cfg.Target.Options.AddNewColumns {
		df.OnColumnAdded = func(col iop.Column) error {
			time.Sleep(300 * time.Millisecond)

			ok, err := tgtConn.AddMissingColumns(*table, iop.Columns{col})
			if err != nil {
				return g.Error(err, "could not add missing columns")
			} else if ok {
				columns, err := pullTargetTempTableColumns(cfg, tgtConn, true)
				if err != nil {
					return g.Error(err, "could not get table columns")
				}
				table.Columns = columns
			}
			return nil
		}
	}
}
