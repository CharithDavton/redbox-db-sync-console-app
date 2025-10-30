# SQL Server Database Sync

A lightweight console application that synchronizes SQL Server databases using SQL Server Change Tracking. This tool automatically detects and applies INSERT, UPDATE, and DELETE operations from a source database to a destination database.

## Features

- **Automatic Change Detection**: Uses SQL Server's built-in Change Tracking to identify modified data
- **Scheduled Synchronization**: Runs automatically every 5 minutes
- **Comprehensive Logging**: Logs all sync operations with daily log files
- **Error Handling**: Continues syncing other tables even if one fails
- **Detailed Statistics**: Tracks inserts, updates, deletes, and sync duration
- **Support for Multiple Tables**: Automatically syncs all tables with change tracking enabled

## Prerequisites

- .NET 8.0 or higher
- SQL Server with Change Tracking enabled on source database
- Network connectivity between source and destination databases
- Appropriate permissions on both databases

## Required Database Setup

### Source Database (Enable Change Tracking)

```sql
-- Enable change tracking on database
ALTER DATABASE [YourDatabaseName]
SET CHANGE_TRACKING = ON
(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);

-- Enable change tracking on each table you want to sync
ALTER TABLE [Schema].[TableName]
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = OFF);
```

### Destination Database (Setup Sync Tables)

```sql
-- Create sync control table
CREATE TABLE dbo.SyncControl (
    TableName NVARCHAR(255) PRIMARY KEY,
    LastSyncVersion BIGINT NOT NULL,
    LastSyncTime DATETIME NOT NULL,
    RowsInserted INT DEFAULT 0,
    RowsUpdated INT DEFAULT 0,
    RowsDeleted INT DEFAULT 0
);

-- Create sync log table
CREATE TABLE dbo.SyncLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    SyncStartTime DATETIME NOT NULL,
    SyncEndTime DATETIME NULL,
    Status NVARCHAR(50) NOT NULL,
    TableName NVARCHAR(255) NULL,
    RowsProcessed INT DEFAULT 0,
    DurationSeconds INT DEFAULT 0,
    ErrorMessage NVARCHAR(MAX) NULL
);
```

## Configuration

1. Copy `appsettings.example.json` to `appsettings.json`
2. Update the configuration with your database details:

```json
{
  "Source": {
    "Server": "localhost\\SQLEXPRESS",
    "Database": "YourSourceDatabase",
    "Username": "",
    "Password": "",
    "ConnectionTimeout": 30,
    "CommandTimeout": 300
  },
  "Destination": {
    "Server": "your-server.database.windows.net",
    "Database": "YourDestinationDatabase",
    "Username": "admin",
    "Password": "YourPassword",
    "ConnectionTimeout": 30,
    "CommandTimeout": 300
  }
}
```

**Authentication Options:**
- **Windows Authentication**: Leave `Username` and `Password` empty
- **SQL Authentication**: Provide `Username` and `Password`

## Installation

1. Extract the application to a folder (e.g., `C:\SqlServerSync`)
2. Create the log directory: `C:\RedBoxDbSyncLog`
3. Configure `appsettings.json` with your database settings
4. Run the application

## Usage

### Running the Application

Simply build and run the executable:
```
SqlServerSync.exe
```

The application will:
- Start automatically
- Sync databases every 5 minutes
- Display "App started. Press Enter to exit."
- Continue running until you press Enter

### Logs

Logs are saved to: `C:\RedBoxDbSyncLog\Sync_DD-MM-YYYY.txt`

Log files include:
- Sync start/end times
- Tables processed
- Number of inserts, updates, deletes per table
- Duration and error information
- Retention: 30 days

## Monitoring

Check sync status by querying the destination database:

```sql
-- View recent sync logs
SELECT TOP 10 *
FROM dbo.SyncLog
ORDER BY SyncStartTime DESC;

-- View sync statistics per table
SELECT 
    TableName,
    LastSyncTime,
    RowsInserted,
    RowsUpdated,
    RowsDeleted
FROM dbo.SyncControl
ORDER BY LastSyncTime DESC;
```

## Troubleshooting

### Common Issues

**"No tables with change tracking enabled"**
- Verify change tracking is enabled on your source database and tables

**Connection timeout errors**
- Increase `ConnectionTimeout` in appsettings.json
- Check network connectivity and firewall settings

**"No primary key found"**
- The application requires all synced tables to have primary keys
- Add a primary key to tables without one

**Permission errors**
- Ensure the application has SELECT permissions on source database
- Ensure INSERT, UPDATE, DELETE permissions on destination database

## Security Considerations

- Store `appsettings.json` securely with restricted file permissions
- Use SQL Authentication for RDS/cloud databases
- Use Windows Authentication for local/domain SQL Servers when possible
- Regularly rotate database credentials
- Enable TLS/SSL for database connections in production

## Customization

### Adjust Sync Interval

Edit `Program.cs` line 14:
```csharp
timer = new System.Timers.Timer(5 * 60 * 1000); // 5 minutes
```

Change `5` to your desired interval in minutes.

### Change Log Location

Edit `Program.cs` line 27:
```csharp
path: $@"C:\YourLogPath\Sync_{DateTime.Now:dd-MM-yyyy}.txt",
```

## Dependencies

- Microsoft.Data.SqlClient
- Serilog
- Newtonsoft.Json

## License

This is internal application code. Please ensure you have appropriate rights to use and distribute.

## Support

For issues or questions, contact your database administrator or development team.

---

**Version**: 1.0  
**Last Updated**: October 2025
