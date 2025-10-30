using Microsoft.Data.SqlClient;
using Serilog;
using System.Data;
using System.Diagnostics;
using System.Timers;

namespace SqlServerSync
{
    class Program
    {
        static System.Timers.Timer timer;
        static void Main()
        {
            timer = new System.Timers.Timer(5 * 60 * 1000); // 15 minutes in milliseconds
            timer.Elapsed += Timer_Elapsed;
            timer.AutoReset = true; // repeat automatically
            timer.Enabled = true;

            Console.WriteLine("App started. Press Enter to exit.");
            Console.ReadLine(); // keep app running
        }

        private static async void Timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            // Setup logging
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .WriteTo.Console()
                .WriteTo.File(
                    path: $@"C:\RedBoxDbSyncLog\Sync_{DateTime.Now:dd-MM-yyyy}.txt",
                    rollingInterval: RollingInterval.Infinite,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss} [{Level}] {Message}{NewLine}{Exception}",
                    retainedFileCountLimit: 30 // keep only last 30 days
                )
                .CreateLogger();

            try
            {
                Log.Information("========================================");
                Log.Information("SQL Server Database Sync Starting");
                Log.Information("========================================");

                // Load configuration
                var config = ConfigurationManager.LoadConfiguration("appsettings.json");
                if (config == null)
                {
                    Log.Error("Failed to load configuration");
                }

                var syncService = new SyncService(config);
                var success = await syncService.SyncDatabasesAsync();

                Log.Information("========================================");
                Log.Information($"Sync completed with status: {(success ? "SUCCESS" : "FAILED")}");
                Log.Information("========================================");
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Fatal error during synchronization");
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }
    }
}
