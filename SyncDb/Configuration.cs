using Newtonsoft.Json;

namespace SqlServerSync
{
    public class SyncConfiguration
    {
        public DatabaseConfig Source { get; set; } = null!;
        public DatabaseConfig Destination { get; set; } = null!;
    }

    public class DatabaseConfig
    {
        public string Server { get; set; } = null!;
        public string Database { get; set; } = null!;
        public string? Username { get; set; }
        public string? Password { get; set; }
        public int ConnectionTimeout { get; set; } = 30;
        public int CommandTimeout { get; set; } = 300;

        public string GetConnectionString()
        {
            var builder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder
            {
                DataSource = Server,
                InitialCatalog = Database,
                ConnectTimeout = ConnectionTimeout,
                TrustServerCertificate = true
            };

            if (!string.IsNullOrWhiteSpace(Username) && !string.IsNullOrWhiteSpace(Password))
            {
                builder.UserID = Username;
                builder.Password = Password;
                builder.IntegratedSecurity = false;
            }
            else
            {
                builder.IntegratedSecurity = true;
            }

            return builder.ConnectionString;
        }
    }

    public static class ConfigurationManager
    {
        public static SyncConfiguration? LoadConfiguration(string filePath)
        {
            try
            {
                if (!File.Exists(filePath))
                {
                    Console.WriteLine($"Configuration file not found: {filePath}");
                    return null;
                }

                var json = File.ReadAllText(filePath);
                var config = JsonConvert.DeserializeObject<SyncConfiguration>(json);
                
                if (config == null)
                {
                    Console.WriteLine("Failed to deserialize configuration");
                    return null;
                }

                // Validate configuration
                if (string.IsNullOrWhiteSpace(config.Source.Server) || 
                    string.IsNullOrWhiteSpace(config.Source.Database))
                {
                    Console.WriteLine("Invalid source configuration");
                    return null;
                }

                if (string.IsNullOrWhiteSpace(config.Destination.Server) || 
                    string.IsNullOrWhiteSpace(config.Destination.Database))
                {
                    Console.WriteLine("Invalid destination configuration");
                    return null;
                }

                return config;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading configuration: {ex.Message}");
                return null;
            }
        }
    }
}
