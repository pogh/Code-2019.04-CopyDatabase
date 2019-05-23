using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Security;
using System.Text;
using System.Threading.Tasks;

namespace CopyDatabase
{
    class Program
    {
        static void Main(string[] args)
        {
#if DEBUG
            string dataSource = "MSSQL-DEV.local";
#else
            string dataSource = "localhost";
#endif
            Console.WriteLine(string.Format(Resources.Strings.TryConnect, dataSource));

            //-----------------------------------------------------------------

            SqlConnection sourceConnection = new SqlConnection(string.Concat("Data Source=", dataSource));
            SqlConnection targetConnection = new SqlConnection(string.Concat("Data Source=", dataSource));
            SqlConnection sourceReaderConnection = new SqlConnection(string.Concat("Data Source=", dataSource));

            try
            {
                string userName = GetString(Resources.Strings.UserName);

                if (string.IsNullOrEmpty(userName))
                {
                    CloseConnectionsAndDie(sourceConnection, targetConnection, sourceReaderConnection);
                }

                //-----------------------------------------------------------------
                // Set password using SecureString 

                using (SecureString password = GetSecureString(Resources.Strings.Password))
                {
                    if (password.Length == 0)
                    {
                        CloseConnectionsAndDie(sourceConnection, targetConnection, sourceReaderConnection);
                    }

                    sourceConnection.Credential = new SqlCredential(userName, password);
                    targetConnection.Credential = new SqlCredential(userName, password);

                    sourceReaderConnection.Credential = new SqlCredential(userName, password);

                    sourceConnection.Open();
                    targetConnection.Open();
                    sourceReaderConnection.Open();
                }

                //-----------------------------------------------------------------
                // List the available databases and ask for source and target

                Dictionary<int, string> databases = new Dictionary<int, string>();
                int databaseId = 0;

                using (SqlDataReader dataReader = new SqlCommand("SELECT name FROM sys.databases WHERE database_id > 4 AND state_desc = 'ONLINE' ORDER BY name", sourceConnection).ExecuteReader())
                {
                    while (dataReader.Read())
                        databases.Add(databaseId += 1, dataReader.GetString(0));
                    dataReader.Close();
                }

                foreach (KeyValuePair<int, string> keyValue in databases.OrderBy(x => x.Value))
                    Console.WriteLine(String.Format("{0,3}  {1}", keyValue.Key, keyValue.Value));

                string sourceDatabaseName = string.Empty;
                try
                {
                    sourceDatabaseName = databases[GetInt(Resources.Strings.SourceDatabase)];
                }
                catch
                {
                    CloseConnectionsAndDie(sourceConnection, targetConnection, sourceReaderConnection);
                }

                string targetDatabaseName = string.Empty;
                try
                {
                    targetDatabaseName = databases[GetInt(Resources.Strings.TargetDatabase)];
                }
                catch
                {
                    CloseConnectionsAndDie(sourceConnection, targetConnection, sourceReaderConnection);
                }

                if (sourceDatabaseName == targetDatabaseName)
                {
                    Console.WriteLine(Resources.Strings.SourceIsTarget);
                    CloseConnectionsAndDie(sourceConnection, targetConnection, sourceReaderConnection);
                }

                //-----------------------------------------------------------------
                // Confirm before proceding

                if (GetYN(string.Format(Resources.Strings.YouWillBeCopying, System.Environment.NewLine, sourceDatabaseName, targetDatabaseName)))
                {
                    Console.WriteLine(string.Empty);

                    //-----------------------------------------------------------------
                    // Get a list of tables and their object ids

                    Task<Dictionary<int, string>> sourceTablesTask = GetTablesDictionary(sourceConnection, sourceDatabaseName);
                    Task<Dictionary<int, string>> targetTablesTask = GetTablesDictionary(targetConnection, targetDatabaseName);

                    Task.WaitAll(sourceTablesTask, targetTablesTask);

                    Dictionary<int, string> sourceTables = sourceTablesTask.Result;
                    Dictionary<int, string> targetTables = targetTablesTask.Result;

                    sourceTablesTask = null;
                    targetTablesTask = null;

                    //----------------------------------------------------------
                    // Absolutely last chance! 

                    string resultsFileName = string.Concat(DateTime.Now.ToString("yyyyMMdd.HHmm"), ".exceptions.sql");

                    if (!GetYN(string.Format(Resources.Strings.BeyondThisPoint, System.Environment.NewLine)))
                    {
                        CloseConnectionsAndDie(sourceConnection, targetConnection, sourceReaderConnection);
                    }

                    //----------------------------------------------------------
                    // ALTER DATABASE SET RECOVERY SIMPLE; 

                    using (SqlCommand command = new SqlCommand(string.Concat("ALTER DATABASE [", targetDatabaseName, "] SET RECOVERY SIMPLE"), targetConnection))
                        command.ExecuteNonQuery();

                    Console.WriteLine(Resources.Strings.SIMPLE);

                    //----------------------------------------------------------
                    // Set the target database the same size as the source database
                    // to avoid file grows during the import

                    Task.WaitAll(ResizeDataFile(sourceConnection, sourceDatabaseName, targetConnection, targetDatabaseName));

                    Console.WriteLine(Resources.Strings.ResizedDataFile);

                    Task.WaitAll(ResizeLogFile(targetConnection, targetDatabaseName, 12));

                    Console.WriteLine(Resources.Strings.ResizedLogFile);

                    //----------------------------------------------------------

                    DateTime totalCopyStart = DateTime.Now;

                    // Loop through the tables...
                    foreach (KeyValuePair<int, string> keyValue in sourceTables.OrderBy(x => x.Value))
                    {
                        DateTime tableCopyStart = DateTime.Now;

                        string sourceTableName = string.Concat("[", sourceDatabaseName, "].", keyValue.Value);
                        int sourceObjectId = keyValue.Key;

                        string targetTableName = string.Concat("[", targetDatabaseName, "].", keyValue.Value);
                        int targetObjectId = 0;

                        // Check if there's a corresponding target table for the source table
                        {
                            var targetTablesList = targetTables.Where(x => x.Value.Equals(keyValue.Value));
                            if (targetTablesList.Count() == 0)  // Oh no, no table found
                            {
                                Console.WriteLine(string.Format(Resources.Strings.NotFound, targetTableName));
                                continue;
                            }
                            else if (targetTablesList.Count() == 1)  // Yay, found a table
                            {
                                targetObjectId = targetTablesList.Single().Key;
                            }
                            else // shouldn't happen... unless there's more than one table with the same name in the same schema...?  We want to know if so...
                            {
                                Console.WriteLine(string.Format(Resources.Strings.NeedsInvestigation, targetTableName));
                                GenerateLogFileHeader(targetTableName, Resources.Strings.NeedsInvestigationLog, resultsFileName);
                                GenerateLogFileFooter(resultsFileName);
                                continue;
                            }
                        }

                        //----------------------------------------------------------
                        // Check if the target table is empty

                        int rowCount = 0;
                        using (SqlCommand command = new SqlCommand(string.Concat("SELECT COUNT(*) FROM ", targetTableName), targetConnection))
                            rowCount = (int)command.ExecuteScalar();

                        if (rowCount != 0)
                        {
                            Console.WriteLine(string.Format(Resources.Strings.NotEmpty, targetTableName));
                            continue;
                        }

                        //----------------------------------------------------------
                        // Find out how many rows the source table has for feedback

                        using (SqlCommand command = new SqlCommand(string.Concat("SELECT COUNT(*) FROM ", sourceTableName), sourceConnection))
                            RowsCount = (int)command.ExecuteScalar();
                        LastReportedValue = 0;

                        Console.WriteLine(string.Format(Resources.Strings.Copying, sourceTableName));

                        //----------------------------------------------------------

                        using (SqlDataReader dataReader = (new SqlCommand(string.Concat("SELECT * FROM ", sourceTableName), sourceReaderConnection)).ExecuteReader())
                        {
                            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(targetConnection, SqlBulkCopyOptions.TableLock, null))
                            {
                                bulkCopy.BulkCopyTimeout = 0;
                                bulkCopy.EnableStreaming = true;
                                bulkCopy.DestinationTableName = targetTableName;
                                bulkCopy.NotifyAfter = 1000000;
                                bulkCopy.BatchSize = 0;
                                bulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler(OnSqlRowsCopied);

                                // Assumes the same number of columns in target as in source table
                                foreach (KeyValuePair<int, string> columns in GetColumns(sourceConnection, sourceDatabaseName, keyValue.Key).Result.OrderBy(x => x.Key))
                                    bulkCopy.ColumnMappings.Add(columns.Value, columns.Value);

                                try
                                {
                                    // Turn off triggers and indexes to accelerate load
                                    Task.WaitAll(DisableAllTriggers(targetConnection, targetDatabaseName, targetTableName));
                                    Task.WaitAll(DisableAllIndexes(targetConnection, targetDatabaseName, targetObjectId));

                                    bulkCopy.WriteToServer(dataReader);  //Async version of method disappears and doesn't come back
                                }
                                catch (InvalidOperationException ex)
                                {
                                    // Oh no!  Something happened.  Log it on the sql file.
                                    GenerateLogFileHeader(targetTableName, ex.Message, resultsFileName);
                                    GenerateLogFileEntry(sourceConnection, sourceDatabaseName, sourceTableName, sourceObjectId,
                                                         targetConnection, targetDatabaseName, targetTableName, targetObjectId,
                                                         resultsFileName);
                                    GenerateLogFileFooter(resultsFileName);
                                }
                                catch (Exception ex)
                                {
                                    // Oh no!  Something _unexpected_ happened.  Log it on the sql file.
                                    GenerateLogFileHeader(targetTableName, ex.Message, resultsFileName);
                                    GenerateLogFileFooter(resultsFileName);
                                    Console.WriteLine(ex.Message);
                                }
                                finally
                                {
                                    // We're done.
                                    dataReader.Close();

                                    // Give feedback if loading the data took long enough
                                    if (LastReportedValue != 0
                                    || (DateTime.Now - tableCopyStart).TotalSeconds > 60)
                                        Console.WriteLine(string.Format(Resources.Strings.LoadingDataTookSeconds, Math.Ceiling((DateTime.Now - tableCopyStart).TotalSeconds)));

                                    // Turn triggers and indexes back on
                                    Task.WaitAll(EnableAllIndexes(targetConnection, targetDatabaseName, targetObjectId));
                                    Task.WaitAll(EnableAllTriggers(targetConnection, targetDatabaseName, targetTableName));
                                }

                                bulkCopy.SqlRowsCopied -= new SqlRowsCopiedEventHandler(OnSqlRowsCopied);
                            }
                        }

                        // Give feedback if loading the data took long enough
                        if (LastReportedValue != 0
                         || (DateTime.Now - tableCopyStart).TotalSeconds > 60)
                            Console.WriteLine(string.Format(Resources.Strings.ActivatingConstraintsTookSeconds, Math.Ceiling((DateTime.Now - tableCopyStart).TotalSeconds)));
                    }

                    //----------------------------------------------------------

                    Console.WriteLine(string.Format(Resources.Strings.ThatTookMinutes, Math.Ceiling((DateTime.Now - totalCopyStart).TotalMinutes)));
                    Console.WriteLine(Resources.Strings.Optimising);

                    // Be nice and clean up the database
                    Task.WaitAll(RetrustForeignConstraints(targetConnection, targetDatabaseName));
                    Task.WaitAll(UpdateTableStatistics(targetConnection, targetDatabaseName));
                    Task.WaitAll(ResizeLogFile(targetConnection, targetDatabaseName, 1));

                    //----------------------------------------------------------

                    Console.WriteLine(string.Format(Resources.Strings.ThatTookMinutes, Math.Ceiling((DateTime.Now - totalCopyStart).TotalMinutes)));
                    Console.WriteLine(Resources.Strings.CheckExceptionsSqlFile);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                CloseConnectionsAndDie(sourceConnection, targetConnection, sourceReaderConnection);
            }
        }

        /// <summary>
        /// Be nice and try and close the connections
        /// </summary>
        private static void CloseConnectionsAndDie(SqlConnection sourceConnection, SqlConnection targetConnection, SqlConnection sourceReaderConnection)
        {
            try
            {
                if (sourceConnection != null
                   && sourceConnection.State != System.Data.ConnectionState.Closed)
                    sourceConnection.Close();

                if (targetConnection != null
                && targetConnection.State != System.Data.ConnectionState.Closed)
                    targetConnection.Close();

                if (sourceReaderConnection != null
                && sourceReaderConnection.State != System.Data.ConnectionState.Closed)
                    sourceReaderConnection.Close();
            }
            catch
            {
                //Do nothing
            }

            Console.WriteLine(Resources.Strings.GoodBye);
            System.Threading.Thread.Sleep(1000);
            Environment.Exit(0);
        }

        /// <summary>
        /// Resizes the target database datafile to the same size as the source
        /// </summary>
        private async static Task ResizeDataFile(SqlConnection sourceConnection, string sourceDatabaseName, SqlConnection targetConnection, string targetDatabaseName)
        {
            try
            {
                int sourceDatabaseSizeMb = (int)await new SqlCommand(string.Concat("SELECT SUM(size) * 8 / 1024 AS TotalDiskSpace FROM sys.databases d JOIN sys.master_files mf ON d.database_id = mf.database_id WHERE d.name = '", sourceDatabaseName, "' AND mf.type_desc = 'ROWS'"), sourceConnection).ExecuteScalarAsync();
                sourceDatabaseSizeMb = Convert.ToInt32((Math.Floor(sourceDatabaseSizeMb / 1024.0) + 1) * 1024.0);

                //-------------------------------------------------------------

                string targetPhysicalName = string.Empty;
                string targetLogicalName = string.Empty;
                int targetDatabaseSizeMb = 0;

                using (SqlDataReader dataReader = await new SqlCommand(string.Concat("SELECT mf.physical_name, mf.name, SUM(size) * 8 / 1024 AS TotalDiskSpace FROM sys.databases d JOIN sys.master_files mf ON d.database_id = mf.database_id WHERE d.name = '", targetDatabaseName, "' AND mf.type_desc = 'ROWS' GROUP BY mf.name, mf.physical_name, mf.type_desc"), targetConnection).ExecuteReaderAsync())
                {
                    if (await dataReader.ReadAsync())
                    {
                        targetPhysicalName = dataReader.GetString(0);
                        targetLogicalName = dataReader.GetString(1);
                        targetDatabaseSizeMb = dataReader.GetInt32(2);
                    }
                    dataReader.Close();
                }

                //-------------------------------------------------------------

                if (targetDatabaseSizeMb < sourceDatabaseSizeMb)
                {
                    // Assume there's going to be enough space on the disk
                    using (SqlCommand command = new SqlCommand(string.Concat("USE [", targetDatabaseName, "]; ALTER DATABASE [", targetDatabaseName, "] MODIFY FILE(NAME = [", targetLogicalName, "], SIZE = ", sourceDatabaseSizeMb, "MB); "), targetConnection))
                    {
                        command.CommandTimeout = 0;
                        await command.ExecuteNonQueryAsync();
                    }
                }

                //-------------------------------------------------------------
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format(Resources.Strings.ExceptionMessage, System.Reflection.MethodBase.GetCurrentMethod().Name, ex.Message));
            }
        }

        /// <summary>
        /// Resizes the logfile.  Does a shrink first to try and clean up empty virtual log files
        /// </summary>
        private async static Task ResizeLogFile(SqlConnection connection, string databaseName, int sizeGB)
        {
            try
            {
                string logLogicalName = (string)await new SqlCommand(string.Concat("SELECT f.name LogicalName FROM sys.master_files f JOIN sys.databases d ON d.database_id = f.database_id WHERE d.name = '", databaseName, "' AND f.type_desc = 'LOG'"), connection).ExecuteScalarAsync();

                using (SqlCommand command = new SqlCommand(string.Concat("USE [", databaseName, "]; DBCC SHRINKFILE([", logLogicalName, "], 0); ALTER DATABASE [", databaseName, "] MODIFY FILE(NAME = [", logLogicalName, "], SIZE = ", sizeGB, "GB); "), connection))
                {
                    command.CommandTimeout = 0;
                    await command.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format(Resources.Strings.ExceptionMessage, System.Reflection.MethodBase.GetCurrentMethod().Name, ex.Message));
            }
        }

        private async static Task DisableAllTriggers(SqlConnection connection, string databaseName, string tableName)
        {
            try
            {
                await new SqlCommand(string.Concat("USE [", databaseName, "]; DISABLE TRIGGER ALL ON ", tableName, ";"), connection).ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format(Resources.Strings.ExceptionMessage, System.Reflection.MethodBase.GetCurrentMethod().Name, ex.Message));
            }
        }

        private async static Task EnableAllTriggers(SqlConnection connection, string databaseName, string tableName)
        {
            try
            {
                await new SqlCommand(string.Concat("USE [", databaseName, "]; ENABLE TRIGGER ALL ON ", tableName, ";"), connection).ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format(Resources.Strings.ExceptionMessage, System.Reflection.MethodBase.GetCurrentMethod().Name, ex.Message));
            }
        }

        /// <summary>
        /// Deactivate all non-clustered indexes on a table
        /// </summary>
        private async static Task DisableAllIndexes(SqlConnection connection, string databaseName, int objectId)
        {
            try
            {
                List<string> cmds = new List<string>();

                using (SqlDataReader dataReader = await new SqlCommand(string.Concat("USE [", databaseName, "];SELECT 'ALTER INDEX [' + i.name + '] ON [' + o.name + '] DISABLE;' FROM sys.indexes i JOIN sys.objects o ON i.object_id = o.object_id WHERE o.object_id = ", objectId, " AND o.type_desc = 'USER_TABLE' AND i.type_desc = 'NONCLUSTERED'"), connection).ExecuteReaderAsync())
                {
                    while (await dataReader.ReadAsync())
                        cmds.Add(dataReader.GetString(0));
                    dataReader.Close();
                }

                foreach (string cmd in cmds)
                {
                    SqlCommand command = new SqlCommand(string.Concat("USE [", databaseName, "]; ", cmd), connection);
                    command.CommandTimeout = 0;
                    try
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(cmd);
                        Console.WriteLine(ex.Message);
                    }

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format(Resources.Strings.ExceptionMessage, System.Reflection.MethodBase.GetCurrentMethod().Name, ex.Message));
            }
        }

        /// <summary>
        /// Rebuilds all non-clustered indexes on a table
        /// </summary>
        private async static Task EnableAllIndexes(SqlConnection connection, string databaseName, int objectId)
        {
            try
            {
                List<string> cmds = new List<string>();

                using (SqlDataReader dataReader = await new SqlCommand(string.Concat("USE [", databaseName, "];SELECT 'ALTER INDEX [' + i.name + '] ON .[' + o.name + '] REBUILD;' FROM sys.indexes i JOIN sys.objects o ON i.object_id = o.object_id WHERE o.object_id = ", objectId, " AND o.type_desc = 'USER_TABLE' AND i.type_desc = 'NONCLUSTERED'"), connection).ExecuteReaderAsync())
                {
                    while (await dataReader.ReadAsync())
                        cmds.Add(dataReader.GetString(0));
                    dataReader.Close();
                }

                foreach (string cmd in cmds)
                {
                    SqlCommand command = new SqlCommand(string.Concat("USE [", databaseName, "]; ", cmd), connection);
                    command.CommandTimeout = 0;
                    try
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(cmd);
                        Console.WriteLine(ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format(Resources.Strings.ExceptionMessage, System.Reflection.MethodBase.GetCurrentMethod().Name, ex.Message));
            }
        }

        /// <summary>
        /// Retrusts all untrusted foreign constraints in a database
        /// </summary>
        private async static Task RetrustForeignConstraints(SqlConnection connection, string databaseName)
        {
            try
            {
                List<string> cmds = new List<string>();

                using (SqlDataReader dataReader = await new SqlCommand(string.Concat("USE [", databaseName, "];SELECT 'ALTER TABLE [' + s.name + '].[' + o.name + '] WITH CHECK CHECK CONSTRAINT [' + i.name + ']' AS keyname FROM sys.foreign_keys i JOIN sys.objects o ON i.parent_object_id = o.object_id JOIN sys.schemas s ON o.schema_id = s.schema_id WHERE i.is_not_trusted = 1 AND i.is_not_for_replication = 0 AND i.is_disabled = 0"), connection).ExecuteReaderAsync())
                {
                    while (await dataReader.ReadAsync())
                        cmds.Add(dataReader.GetString(0));
                    dataReader.Close();
                }

                foreach (string cmd in cmds)
                {
                    SqlCommand command = new SqlCommand(string.Concat("USE [", databaseName, "]; ", cmd), connection);
                    command.CommandTimeout = 0;
                    await command.ExecuteNonQueryAsync();
                }

                Console.WriteLine(Resources.Strings.RetrustRelationships);
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format(Resources.Strings.ExceptionMessage, System.Reflection.MethodBase.GetCurrentMethod().Name, ex.Message));
            }
        }

        /// <summary>
        /// Updates all statistics on all tables in a database
        /// </summary>
        private async static Task UpdateTableStatistics(SqlConnection connection, string databaseName)
        {
            try
            {
                List<string> cmds = new List<string>();

                using (SqlDataReader dataReader = await new SqlCommand(string.Concat("USE [", databaseName, "];SELECT 'UPDATE STATISTICS [' + s.name + '].[' + t.name + ']' FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id"), connection).ExecuteReaderAsync())
                {
                    while (await dataReader.ReadAsync())
                        cmds.Add(dataReader.GetString(0));
                    dataReader.Close();
                }

                foreach (string cmd in cmds)
                {
                    SqlCommand command = new SqlCommand(string.Concat("USE [", databaseName, "]; ", cmd), connection);
                    command.CommandTimeout = 0;
                    await command.ExecuteNonQueryAsync();
                }

                Console.WriteLine(Resources.Strings.UpdatedTableStatistics);
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format(Resources.Strings.ExceptionMessage, System.Reflection.MethodBase.GetCurrentMethod().Name, ex.Message));
            }
        }

        /// <summary>
        /// Writes a nice header block to a file
        /// </summary>
        private static void GenerateLogFileHeader(string targetTableName, string exceptionMessage, string fileName)
        {
            using (StreamWriter sw = File.AppendText(fileName))
            {
                sw.WriteLine(new string('-', 80));

                sw.Write("-- ");
                sw.WriteLine(targetTableName);

                sw.Write("-- ");
                sw.WriteLine(exceptionMessage);

            }
        }

        /// <summary>
        /// Generates an select insert statement to a file
        /// </summary>
        private static void GenerateLogFileEntry(SqlConnection sourceConnection, string sourceDatabaseName, string sourceTableName, int sourceObjectId,
                                                 SqlConnection targetConnection, string targetDatabaseName, string targetTableName, int targetObjectId,
                                                 string fileName)
        {

            Task<Dictionary<int, string>> sourceColumnNamesTask = GetColumns(sourceConnection, sourceDatabaseName, sourceObjectId);
            Task<Dictionary<int, string>> targetColumnNamesTask = GetColumns(targetConnection, targetDatabaseName, targetObjectId);

            Task.WaitAll(sourceColumnNamesTask, targetColumnNamesTask);

            Dictionary<int, string> sourceColumnNames = sourceColumnNamesTask.Result;
            Dictionary<int, string> targetColumnNames = targetColumnNamesTask.Result;

            sourceColumnNamesTask = null;
            targetColumnNamesTask = null;

            //----------------------------------------------------------

            StringBuilder sourceColumnsBuilder = new StringBuilder();
            StringBuilder targetColumnsBuilder = new StringBuilder();

            int t = 1;

            for (int s = 1; s <= sourceColumnNames.Count; s += 1)
            {
                if (!sourceColumnNames.ContainsKey(s)
                && !targetColumnNames.ContainsKey(t))
                {
                    t += 1;
                    continue;
                }

                sourceColumnsBuilder.Append(sourceColumnNames[s]);
                sourceColumnsBuilder.Append(", ");

                if (sourceColumnNames[s].Equals(targetColumnNames[t], StringComparison.InvariantCultureIgnoreCase))
                {
                    targetColumnsBuilder.Append(targetColumnNames[t]);
                    t += 1;
                }
                else
                {
                    targetColumnsBuilder.Append(new string('?', sourceColumnNames[s].Length));
                }
                targetColumnsBuilder.Append(", ");
            }

            if (sourceColumnsBuilder.Length > 0)
                sourceColumnsBuilder.Remove(sourceColumnsBuilder.Length - 2, 2);

            if (targetColumnsBuilder.Length > 0)
                targetColumnsBuilder.Remove(targetColumnsBuilder.Length - 2, 2);


            //----------------------------------------------------------

            List<string> missingSourceColumns = targetColumnNames.Where(x => sourceColumnNames.Count(y => y.Value.Equals(x.Value, StringComparison.InvariantCultureIgnoreCase)) == 0).Select(z => z.Value).ToList();
            List<string> missingTargetColumns = sourceColumnNames.Where(x => targetColumnNames.Count(y => y.Value.Equals(x.Value, StringComparison.InvariantCultureIgnoreCase)) == 0).Select(z => z.Value).ToList();

            //----------------------------------------------------------

            using (StreamWriter sw = File.AppendText(fileName))
            {
                if (missingSourceColumns.Count > 0)
                {
                    sw.Write(Resources.Strings.ColumnsMissingSource);
                    sw.WriteLine(string.Join(", ", missingSourceColumns));
                }

                if (missingTargetColumns.Count > 0)
                {
                    sw.Write(Resources.Strings.ColumnsMissingTarget);
                    sw.WriteLine(string.Join(", ", missingTargetColumns));
                }

                sw.WriteLine(string.Empty);
                sw.WriteLine(string.Concat("INSERT INTO ", targetTableName, Environment.NewLine, "      (", targetColumnsBuilder, ")", Environment.NewLine, "SELECT ", sourceColumnsBuilder, Environment.NewLine, "FROM ", sourceTableName));
                sw.WriteLine(string.Empty);
            }
        }

        /// <summary>
        /// Adds an empty line to a log file
        /// </summary>
        private static void GenerateLogFileFooter(string fileName)
        {
            using (StreamWriter sw = File.AppendText(fileName))
            {
                sw.Write(string.Empty);
            }
        }


        private static int _rowsCount = 0;
        private static object _rowsCountLockObject = new object();
        /// <summary>
        /// Thread safe property to remember how many rows in the source table
        /// </summary>
        private static int RowsCount
        {
            get
            {
                lock (_rowsCountLockObject)
                    return _rowsCount;
            }
            set
            {
                lock (_rowsCountLockObject)
                    _rowsCount = value;
            }
        }

        private static int _lastReportedValue = 0;
        private static object _lastReportedValueLockObject = new object();
        /// <summary>
        /// Thread safe property to remember how many rows have been processed already
        /// </summary>
        private static int LastReportedValue
        {
            get
            {
                lock (_lastReportedValueLockObject)
                    return _lastReportedValue;
            }
            set
            {
                lock (_lastReportedValueLockObject)
                    _lastReportedValue = value;
            }
        }

        /// <summary>
        /// Event to process feedback from SqlBulkCopy
        /// </summary>
        private static void OnSqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            int reportedValue = Convert.ToInt32(Math.Floor(e.RowsCopied / (double)RowsCount * 100));

            if (reportedValue != LastReportedValue)
            {
                LastReportedValue = reportedValue;
                Console.WriteLine(Resources.Strings.CopiedSoFar, reportedValue);
            }
        }

        /// <summary>
        /// Returns column names for a table (excluding computed columns)
        /// </summary>
        private async static Task<Dictionary<int, string>> GetColumns(SqlConnection connection, string databaseName, int objectId)
        {
            Dictionary<int, string> returnValue = new Dictionary<int, string>();

            using (SqlDataReader dataReader = await new SqlCommand(string.Concat("USE [", databaseName, "];SELECT column_id, name FROM sys.columns c WHERE c.object_id = ", objectId, " AND is_computed = 0 ORDER BY column_id"), connection).ExecuteReaderAsync())
            {
                while (await dataReader.ReadAsync())
                    returnValue.Add(dataReader.GetInt32(0), dataReader.GetString(1));
                dataReader.Close();
            }

            return returnValue;
        }

        /// <summary>
        /// Returns a Dictionary with object ids and table names
        /// </summary>
        private async static Task<Dictionary<int, string>> GetTablesDictionary(SqlConnection connection, string databaseName)
        {
            Dictionary<int, string> returnValue = new Dictionary<int, string>();

            using (SqlDataReader dataReader = await new SqlCommand(string.Concat("USE [", databaseName, "];SELECT object_id, '[' + s.name + '].[' + t.name + ']' FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.type = 'U' AND s.name = 'dbo' ORDER BY s.name, t.name"), connection).ExecuteReaderAsync())
            {
                while (await dataReader.ReadAsync())
                    returnValue.Add(dataReader.GetInt32(0), dataReader.GetString(1));
                dataReader.Close();
            }

            Console.WriteLine(string.Format(Resources.Strings.TablesFound, returnValue.Count, databaseName));

            return returnValue;
        }

        /// <summary>
        /// Prompts to return a string
        /// </summary>
        private static string GetString(string text)
        {
            Console.Write(text);
            string returnValue = Console.ReadLine();

            return returnValue;
        }

        /// <summary>
        /// Prompts to return an int
        /// </summary>
        private static int GetInt(string text)
        {
            int returnValue = 0;

            do
            {
                Console.Write(text);

                string line = Console.ReadLine();

                if (string.IsNullOrEmpty(line))
                {
                    returnValue = int.MinValue;
                    break;
                }
                else if (int.TryParse(line, out returnValue))
                {
                    break;
                }

                Console.WriteLine(Resources.Strings.NeedNumber);
            }
            while (true);

            return returnValue;
        }

        /// <summary>
        /// Prompts to return an Y or N
        /// </summary>
        private static bool GetYN(string text)
        {
            bool returnValue = false;

            do
            {
                Console.Write(text);

                ConsoleKeyInfo key = Console.ReadKey(true);

                if (key.Key == ConsoleKey.Enter)
                {
                    returnValue = false;
                    break;
                }
                else if (key.Key == ConsoleKey.Y)
                {
                    returnValue = true;
                    break;
                }
                if (key.Key == ConsoleKey.N)
                {
                    returnValue = false;
                    break;
                }

                Console.WriteLine(Resources.Strings.NeedYN);
            }
            while (true);

            Console.WriteLine("");

            return returnValue;
        }

        /// <summary>
        /// Prompts to return a secure string
        /// </summary>
        private static SecureString GetSecureString(string text)
        {
            SecureString returnValue = new SecureString();

            ConsoleKeyInfo key;

            Console.Write(text);
            do
            {
                key = Console.ReadKey(true);
                if (key.Key == ConsoleKey.Enter)
                {
                    Console.WriteLine("");
                }
                else if (key.Key == ConsoleKey.Backspace)
                {
                    if (returnValue.Length > 0)
                        returnValue.RemoveAt(returnValue.Length - 1);
                }
                else
                {
                    returnValue.AppendChar(key.KeyChar);
                    Console.Write("*");
                }
            } while (key.Key != ConsoleKey.Enter);

            returnValue.MakeReadOnly();

            return returnValue;
        }
    }
}
