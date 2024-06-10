﻿using System.Data;
using Microsoft.Data.SqlClient;

namespace DataIngestion_APPS
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            DateTime? lastProcessedTimestamp = null;
            const string stagingDbConnectionString = "Server=localhost;Database=StagingDbAPPS;Trusted_Connection=True;TrustServerCertificate=true;";

            const string mainDbConnectionString =
                "Server=localhost;Database=APPS;Trusted_Connection=True;TrustServerCertificate=true;";

            using (SqlConnection stagingConnection = new SqlConnection(stagingDbConnectionString))
            {
                stagingConnection.Open();

                //Get latest ProcessedTimestamp
                const string query = "SELECT MAX(ProcessedTimestamp) FROM HeartRateData WHERE IsProcessed = 1";
                try
                {
                    using var command = new SqlCommand(query, stagingConnection);
                    lastProcessedTimestamp = (DateTime?)command.ExecuteScalar();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed to retrieve lastProcessedTimestamp: {ex.Message}");
                }

                Console.WriteLine(lastProcessedTimestamp.HasValue
                    ? $"Last processed timestamp: {lastProcessedTimestamp.Value}"
                    : "No processed records found.");

                //Check for NULL values in the database, set IsFaulty = true when NULL value(s) are found
                const string updateQueryIsFaulty = "UPDATE HeartRateData SET IsFaulty = CASE WHEN SensorId IS NULL THEN 1 WHEN HeartRateBPM IS NULL THEN 1 WHEN EnterTime IS NULL THEN 1 ELSE 0 END WHERE IsProcessed = 0";
                try
                {
                    using var updateCommandIsFaulty = new SqlCommand(updateQueryIsFaulty, stagingConnection);
                    int rowsAffected = updateCommandIsFaulty.ExecuteNonQuery();
                    Console.WriteLine($"{rowsAffected} records updated.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error executing UPDATE IsFaulty: {ex.Message}");
                }
                
                //Check for medical condition
                const string updateQueryPossibleCondition = "UPDATE HeartRateData SET PossibleCondition = CASE WHEN HeartRateBPM < 60 THEN 'Bradycardia' WHEN HeartRateBPM > 100 THEN 'Tachycardia' ELSE 'None' END";
                try
                {
                    using var updateCommandPossibleCondition = new SqlCommand(updateQueryPossibleCondition, stagingConnection);
                    int rowsAffected = updateCommandPossibleCondition.ExecuteNonQuery();
                    Console.WriteLine($"{rowsAffected} records updated.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error executing UPDATE PossibleCondition: {ex.Message}");
                }

                //Update IsProcessed
                const string updateQueryProcessed = "UPDATE HeartRateData SET ProcessedTimestamp = GETDATE(), IsProcessed = 1 WHERE IsProcessed = 0";
                try
                {
                    using var updateCommandProcessed = new SqlCommand(updateQueryProcessed, stagingConnection);
                    int rowsAffected = updateCommandProcessed.ExecuteNonQuery();
                    Console.WriteLine($"{rowsAffected} records updated.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error executing UPDATE IsProcessed: {ex.Message}");
                }

                //SQL string for selecting the processed data
                const string selectDataFromStagingDbQuery = "SELECT SensorId, HeartRateBPM, EnterTime, IsFaulty FROM HeartRateData WHERE IsProcessed = 1 AND ProcessedTimestamp > @lastProcessedTimestamp";

                //Insert data into main database
                using (var selectDataFromStagingDbCommand = new SqlCommand(selectDataFromStagingDbQuery, stagingConnection))
                {
                    selectDataFromStagingDbCommand.Parameters.AddWithValue("@lastProcessedTimestamp", lastProcessedTimestamp);

                    using (SqlDataReader reader = selectDataFromStagingDbCommand.ExecuteReader())
                    {   
                        //Datatable gets created for the retrieved data
                        var dataTable = new DataTable();
                        dataTable.Load(reader);

                        using (var mainConnection = new SqlConnection(mainDbConnectionString))
                        {
                            mainConnection.Open();

                            var targetTable = "HeartRateData";

                            //Data gets inserted using SqlBulkCopy
                            using (var bulkCopy = new SqlBulkCopy(mainConnection))
                            {
                                bulkCopy.DestinationTableName = targetTable;
                                bulkCopy.ColumnMappings.Add("SensorId", "SensorId");
                                bulkCopy.ColumnMappings.Add("HeartRateBPM", "HeartRateBPM");
                                bulkCopy.ColumnMappings.Add("EnterTime", "EnterTime");
                                bulkCopy.ColumnMappings.Add("PossibleCondition", "PossibleCondition");
                                bulkCopy.ColumnMappings.Add("IsFaulty", "IsFaulty");

                                try
                                {
                                    bulkCopy.WriteToServer(dataTable);
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"Error during data transfer: {ex.Message}");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
