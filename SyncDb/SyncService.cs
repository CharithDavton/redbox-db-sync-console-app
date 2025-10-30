using Microsoft.Data.SqlClient;
using Serilog;
using System.Data;
using System.Diagnostics;

namespace SqlServerSync
{
    public class SyncService
    {
        private readonly SyncConfiguration _config;
        private int _syncLogId;

        public SyncService(SyncConfiguration config)
        {
            _config = config;
        }

        public async Task<bool> SyncDatabasesAsync()
        {
            var stopwatch = Stopwatch.StartNew();
            var totalInserted = 0;
            var totalUpdated = 0;
            var totalDeleted = 0;
            var errorCount = 0;

            try
            {
                // Log sync start
                _syncLogId = await LogSyncStartAsync();

                // Get tables with change tracking enabled
                var tables = await GetTrackedTablesAsync();
                Log.Information($"Found {tables.Count} tables with change tracking enabled");

                foreach (var table in tables)
                {
                    try
                    {
                        var result = await SyncTableAsync(table);
                        totalInserted += result.Inserted;
                        totalUpdated += result.Updated;
                        totalDeleted += result.Deleted;

                        Log.Information($"Table [{table.Schema}].[{table.Name}] - I:{result.Inserted} U:{result.Updated} D:{result.Deleted}");
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, $"Failed to sync table [{table.Schema}].[{table.Name}]");
                        errorCount++;
                        await LogTableErrorAsync(table, ex.Message);
                    }
                }

                stopwatch.Stop();

                // Log completion
                await LogSyncCompleteAsync(
                    errorCount == 0 ? "Success" : "Partial",
                    totalInserted + totalUpdated + totalDeleted,
                    (int)stopwatch.Elapsed.TotalSeconds);

                Log.Information($"Total - Inserted: {totalInserted}, Updated: {totalUpdated}, Deleted: {totalDeleted}");
                Log.Information($"Duration: {stopwatch.Elapsed.TotalSeconds:F2} seconds, Errors: {errorCount}");

                return errorCount == 0;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Fatal error during sync");
                await LogSyncFailureAsync(ex.Message);
                return false;
            }
        }

        private async Task<List<TableInfo>> GetTrackedTablesAsync()
        {
            var tables = new List<TableInfo>();
            var query = @"
                SELECT 
                    SCHEMA_NAME(t.schema_id) AS SchemaName,
                    t.name AS TableName
                FROM sys.tables t
                INNER JOIN sys.change_tracking_tables ct ON t.object_id = ct.object_id
                WHERE t.is_ms_shipped = 0
                ORDER BY t.name";

            using var connection = new SqlConnection(_config.Source.GetConnectionString());
            await connection.OpenAsync();

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = _config.Source.CommandTimeout;

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                tables.Add(new TableInfo
                {
                    Schema = reader.GetString(0),
                    Name = reader.GetString(1)
                });
            }

            return tables;
        }

        private async Task<SyncResult> SyncTableAsync(TableInfo table)
        {
            var fullTableName = $"[{table.Schema}].[{table.Name}]";
            Log.Information($"Syncing {fullTableName}...");

            var result = new SyncResult();

            // Get last sync version
            var lastSyncVersion = await GetLastSyncVersionAsync(fullTableName);
            Log.Debug($"Last sync version: {lastSyncVersion}");

            // Get current version
            var currentVersion = await GetCurrentVersionAsync();
            Log.Debug($"Current version: {currentVersion}");

            if (currentVersion == lastSyncVersion)
            {
                Log.Information($"No changes for {fullTableName}");
                return result;
            }

            // Get primary key columns
            var pkColumns = await GetPrimaryKeyColumnsAsync(table);
            if (pkColumns.Count == 0)
            {
                Log.Warning($"No primary key found for {fullTableName}. Skipping.");
                return result;
            }

            // Get all columns
            var columns = await GetColumnsAsync(table);

            // Get changes
            var changes = await GetChangesAsync(table, columns, pkColumns, lastSyncVersion, currentVersion);
            var affectedRows = 0;
            if (changes.Rows.Count > 0)
            {
                Log.Information($"Processing {changes.Rows.Count} changes for {fullTableName}");

                foreach (DataRow change in changes.Rows)
                {
                    var operation = change["SYS_CHANGE_OPERATION"].ToString();

                    switch (operation)
                    {
                        case "I":
                            affectedRows += await InsertOrUpdateRowAsync(table, change, columns, pkColumns);
                            result.Inserted++;
                            break;
                        case "U":
                            affectedRows += await UpdateRowAsync(table, change, columns, pkColumns);
                            result.Updated++;
                            break;
                        case "D":
                            affectedRows += await DeleteRowAsync(table, change, pkColumns);
                            result.Deleted++;
                            break;
                    }
                }
            }
            Log.Information($"{affectedRows} row(s) affected on {fullTableName}");
            // Update sync control
            if (affectedRows > 0)
            {
                await UpdateSyncControlAsync(fullTableName, currentVersion, result);
            }
            return result;
        }

        private async Task<long> GetLastSyncVersionAsync(string fullTableName)
        {
            var query = @"
                SELECT ISNULL(LastSyncVersion, 0) 
                FROM dbo.SyncControl 
                WHERE TableName = @TableName";

            using var connection = new SqlConnection(_config.Destination.GetConnectionString());
            await connection.OpenAsync();

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@TableName", fullTableName);
            command.CommandTimeout = _config.Destination.CommandTimeout;

            var result = await command.ExecuteScalarAsync();

            if (result == null || result == DBNull.Value)
            {
                // Initialize sync control
                var insertQuery = @"
                    INSERT INTO dbo.SyncControl (TableName, LastSyncVersion, LastSyncTime)
                    VALUES (@TableName, 0, GETDATE())";

                using var insertCommand = new SqlCommand(insertQuery, connection);
                insertCommand.Parameters.AddWithValue("@TableName", fullTableName);
                await insertCommand.ExecuteNonQueryAsync();

                return 0;
            }

            return Convert.ToInt64(result);
        }

        private async Task<long> GetCurrentVersionAsync()
        {
            var query = "SELECT CHANGE_TRACKING_CURRENT_VERSION()";

            using var connection = new SqlConnection(_config.Source.GetConnectionString());
            await connection.OpenAsync();

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = _config.Source.CommandTimeout;

            var result = await command.ExecuteScalarAsync();
            return result == DBNull.Value ? 0 : Convert.ToInt64(result);
        }

        private async Task<List<string>> GetPrimaryKeyColumnsAsync(TableInfo table)
        {
            var pkColumns = new List<string>();
            var query = @"
                SELECT c.name
                FROM sys.indexes i
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE i.is_primary_key = 1 
                    AND OBJECT_NAME(i.object_id) = @TableName
                    AND SCHEMA_NAME(OBJECTPROPERTY(i.object_id, 'SchemaId')) = @SchemaName
                ORDER BY ic.key_ordinal";

            using var connection = new SqlConnection(_config.Source.GetConnectionString());
            await connection.OpenAsync();

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@TableName", table.Name);
            command.Parameters.AddWithValue("@SchemaName", table.Schema);
            command.CommandTimeout = _config.Source.CommandTimeout;

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                pkColumns.Add(reader.GetString(0));
            }

            return pkColumns;
        }

        private async Task<List<ColumnInfo>> GetColumnsAsync(TableInfo table)
        {
            var columns = new List<ColumnInfo>();
            var query = @"
                SELECT c.name, t.name as type_name
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                WHERE c.object_id = OBJECT_ID(@FullTableName)
                ORDER BY c.column_id";

            using var connection = new SqlConnection(_config.Source.GetConnectionString());
            await connection.OpenAsync();

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@FullTableName", $"{table.Schema}.{table.Name}");
            command.CommandTimeout = _config.Source.CommandTimeout;

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                columns.Add(new ColumnInfo
                {
                    Name = reader.GetString(0),
                    Type = reader.GetString(1)
                });
            }

            return columns;
        }

        private async Task<DataTable> GetChangesAsync(TableInfo table, List<ColumnInfo> columns,
            List<string> pkColumns, long lastSyncVersion, long currentVersion)
        {
            var columnListWithoutPks = string.Join(", ",
                columns
                    .Where(c => !pkColumns.Contains(c.Name, StringComparer.OrdinalIgnoreCase))
                    .Select(c => $"T.[{c.Name}]"));
            var columnList = string.IsNullOrWhiteSpace(columnListWithoutPks)
                    ? string.Empty
                    : ", " + columnListWithoutPks;
            var pkColumnList = string.Join(", ", pkColumns.Select(pk => $"CT.[{pk}]"));
            var pkJoin = string.Join(" AND ", pkColumns.Select(pk => $"T.[{pk}] = CT.[{pk}]"));

            var query = $@"
                SELECT CT.SYS_CHANGE_OPERATION, CT.SYS_CHANGE_VERSION, {pkColumnList} {columnList}
                FROM [{table.Schema}].[{table.Name}] T
                RIGHT OUTER JOIN CHANGETABLE(CHANGES [{table.Schema}].[{table.Name}], {lastSyncVersion}) AS CT
                    ON {pkJoin}
                WHERE CT.SYS_CHANGE_VERSION <= {currentVersion}
                ORDER BY CT.SYS_CHANGE_VERSION";

            using var connection = new SqlConnection(_config.Source.GetConnectionString());
            await connection.OpenAsync();

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = _config.Source.CommandTimeout;

            using var adapter = new SqlDataAdapter(command);
            var dataTable = new DataTable();
            adapter.Fill(dataTable);

            return dataTable;
        }

        private async Task<int> InsertOrUpdateRowAsync(TableInfo table, DataRow change,
            List<ColumnInfo> columns, List<string> pkColumns)
        {
            var fullTableName = $"[{table.Schema}].[{table.Name}]";
            var whereClause = BuildWhereClause(change, pkColumns);
            var columnList = string.Join(", ", columns.Select(c => $"[{c.Name}]"));
            var valuesList = string.Join(", ", columns.Select(c => FormatValue(change[c.Name])));
            var setClause = string.Join(", ",
                columns.Where(c => !pkColumns.Contains(c.Name))
                       .Select(c => $"[{c.Name}] = {FormatValue(change[c.Name])}"));

            var query = $@"
                SET IDENTITY_INSERT {fullTableName} ON;

                IF NOT EXISTS (SELECT 1 FROM {fullTableName} WHERE {whereClause})
                BEGIN
                    INSERT INTO {fullTableName} ({columnList}) VALUES ({valuesList})
                END
                ELSE
                BEGIN
                    UPDATE {fullTableName} SET {setClause} WHERE {whereClause}
                END

                SET IDENTITY_INSERT {fullTableName} OFF;
                ";

            return await ExecuteDestinationCommandAsync(query);
        }

        private async Task<int> UpdateRowAsync(TableInfo table, DataRow change,
            List<ColumnInfo> columns, List<string> pkColumns)
        {
            var fullTableName = $"[{table.Schema}].[{table.Name}]";
            var whereClause = BuildWhereClause(change, pkColumns);
            var setClause = string.Join(", ",
                columns.Where(c => !pkColumns.Contains(c.Name))
                       .Select(c => $"[{c.Name}] = {FormatValue(change[c.Name])}"));

            var query = $"UPDATE {fullTableName} SET {setClause} WHERE {whereClause}";

            return await ExecuteDestinationCommandAsync(query);
        }

        private async Task<int> DeleteRowAsync(TableInfo table, DataRow change, List<string> pkColumns)
        {
            var fullTableName = $"[{table.Schema}].[{table.Name}]";
            var whereClause = BuildWhereClause(change, pkColumns);

            var query = $"DELETE FROM {fullTableName} WHERE {whereClause}";

            return await ExecuteDestinationCommandAsync(query);
        }

        private string BuildWhereClause(DataRow change, List<string> pkColumns)
        {
            var conditions = pkColumns.Select(pk =>
            {
                var value = change[pk];
                if (value == null || value == DBNull.Value)
                    return $"[{pk}] IS NULL";
                return $"[{pk}] = {FormatValue(value)}";
            });

            return string.Join(" AND ", conditions);
        }

        private string FormatValue(object? value)
        {
            if (value == null || value == DBNull.Value)
                return "NULL";

            return value switch
            {
                string s => $"'{s.Replace("'", "''")}'",
                DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss.fff}'",
                bool b => b ? "1" : "0",
                byte[] bytes => $"0x{BitConverter.ToString(bytes).Replace("-", "")}",
                _ => value.ToString() ?? "NULL"
            };
        }

        private async Task<int> ExecuteDestinationCommandAsync(string query)
        {
            using var connection = new SqlConnection(_config.Destination.GetConnectionString());
            await connection.OpenAsync();

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = _config.Destination.CommandTimeout;

            return await command.ExecuteNonQueryAsync();
        }

        private async Task UpdateSyncControlAsync(string fullTableName, long currentVersion, SyncResult result)
        {
            var query = @"
                UPDATE dbo.SyncControl
                SET LastSyncVersion = @Version,
                    LastSyncTime = GETDATE(),
                    RowsInserted = RowsInserted + @Inserted,
                    RowsUpdated = RowsUpdated + @Updated,
                    RowsDeleted = RowsDeleted + @Deleted
                WHERE TableName = @TableName";

            using var connection = new SqlConnection(_config.Destination.GetConnectionString());
            await connection.OpenAsync();

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@Version", currentVersion);
            command.Parameters.AddWithValue("@Inserted", result.Inserted);
            command.Parameters.AddWithValue("@Updated", result.Updated);
            command.Parameters.AddWithValue("@Deleted", result.Deleted);
            command.Parameters.AddWithValue("@TableName", fullTableName);
            command.CommandTimeout = _config.Destination.CommandTimeout;

            await command.ExecuteNonQueryAsync();
        }

        private async Task<int> LogSyncStartAsync()
        {
            var query = @"
                INSERT INTO dbo.SyncLog (SyncStartTime, Status)
                VALUES (GETDATE(), 'Running');
                SELECT CAST(SCOPE_IDENTITY() AS INT)";

            using var connection = new SqlConnection(_config.Destination.GetConnectionString());
            await connection.OpenAsync();

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = _config.Destination.CommandTimeout;

            var result = await command.ExecuteScalarAsync();
            return Convert.ToInt32(result);
        }

        private async Task LogSyncCompleteAsync(string status, int rowsProcessed, int durationSeconds)
        {
            var query = @"
                UPDATE dbo.SyncLog
                SET SyncEndTime = GETDATE(),
                    Status = @Status,
                    RowsProcessed = @RowsProcessed,
                    DurationSeconds = @Duration
                WHERE LogID = @LogID";

            using var connection = new SqlConnection(_config.Destination.GetConnectionString());
            await connection.OpenAsync();

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@Status", status);
            command.Parameters.AddWithValue("@RowsProcessed", rowsProcessed);
            command.Parameters.AddWithValue("@Duration", durationSeconds);
            command.Parameters.AddWithValue("@LogID", _syncLogId);
            command.CommandTimeout = _config.Destination.CommandTimeout;

            await command.ExecuteNonQueryAsync();
        }

        private async Task LogSyncFailureAsync(string errorMessage)
        {
            var query = @"
                UPDATE dbo.SyncLog
                SET SyncEndTime = GETDATE(),
                    Status = 'Failed',
                    ErrorMessage = @ErrorMessage
                WHERE LogID = @LogID";

            using var connection = new SqlConnection(_config.Destination.GetConnectionString());
            await connection.OpenAsync();

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@ErrorMessage", errorMessage);
            command.Parameters.AddWithValue("@LogID", _syncLogId);
            command.CommandTimeout = _config.Destination.CommandTimeout;

            await command.ExecuteNonQueryAsync();
        }

        private async Task LogTableErrorAsync(TableInfo table, string errorMessage)
        {
            var query = @"
                INSERT INTO dbo.SyncLog (SyncStartTime, SyncEndTime, Status, TableName, ErrorMessage)
                VALUES (GETDATE(), GETDATE(), 'Failed', @TableName, @ErrorMessage)";

            using var connection = new SqlConnection(_config.Destination.GetConnectionString());
            await connection.OpenAsync();

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@TableName", $"[{table.Schema}].[{table.Name}]");
            command.Parameters.AddWithValue("@ErrorMessage", errorMessage);
            command.CommandTimeout = _config.Destination.CommandTimeout;

            await command.ExecuteNonQueryAsync();
        }
    }

    public class TableInfo
    {
        public string Schema { get; set; } = null!;
        public string Name { get; set; } = null!;
    }

    public class ColumnInfo
    {
        public string Name { get; set; } = null!;
        public string Type { get; set; } = null!;
    }

    public class SyncResult
    {
        public int Inserted { get; set; }
        public int Updated { get; set; }
        public int Deleted { get; set; }
    }
}
