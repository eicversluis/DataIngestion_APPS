using System.Data;
using Microsoft.Data.SqlClient;

namespace DataIngestion_APPS
{
    public class Program
    {
        internal static void Main(string[] args)
        {
            const string stagingDbConnectionString = "Server=localhost;Database=StagingDbAPPS;Trusted_Connection=True;TrustServerCertificate=true;";

            const string mainDbConnectionString =
                "Server=localhost;Database=APPS;Trusted_Connection=True;TrustServerCertificate=true;";

            using (SqlConnection stagingConnection = new SqlConnection(stagingDbConnectionString))
            {
                stagingConnection.Open();
                
                //Check for NULL values in the database, set IsFaulty = true when NULL value(s) are found
                var updateQueryIsFaulty =
                        "UPDATE HeartRateData WHERE IsProcessed = 0 SET IsFaulty = CASE WHEN SensorId IS NULL THEN 1 WHEN HeartRateBPM IS NULL THEN 1 WHEN EnterTime IS NULL THEN 1 ELSE 0 END";

                using (SqlCommand updateCommandIsFaulty = new SqlCommand(updateQueryIsFaulty, stagingConnection))
                {
                    updateCommandIsFaulty.ExecuteNonQuery();
                }
                
                //Check for medical condition
                var updateQueryPossibleCondition =
                        "UPDATE HeartRateData SET PossibleCondition = CASE WHEN HeartRateBPM < 60 THEN 'Bradycardia' WHEN HeartRateBPM > 100 THEN 'Tachycardia' ELSE 'None' END";

                using (SqlCommand updateCommandPossibleCondition = new SqlCommand(updateQueryPossibleCondition, stagingConnection))
                {
                    updateCommandPossibleCondition.ExecuteNonQuery();
                }

                //Update IsProcessed
                var updateQueryIsProcessed =
                        "UPDATE HeartRateData SET IsProcessed = 1 WHERE IsProcessed = 0";

                using (SqlCommand updateCommandIsProcessed = new SqlCommand(updateQueryIsProcessed, stagingConnection))
                {
                    updateCommandIsProcessed.ExecuteNonQuery();
                }

                //SQL string for selecting the processed data
                var selectDataFromStagingDbQuery =
                    "SELECT SensorId, HeartRateBPM, EnterTime, IsFaulty FROM HeartRateData WHERE IsProcessed = 1";

                //Insert data into main database
                using (SqlCommand selectDataFromStagingDbCommand = new SqlCommand(selectDataFromStagingDbQuery, stagingConnection))
                {
                    using (SqlDataReader reader = selectDataFromStagingDbCommand.ExecuteReader())
                    {   
                        //Datatable gets created for the retrieved data
                        var dataTable = new DataTable();
                        dataTable.Load(reader);

                        using (SqlConnection mainConnection = new SqlConnection(mainDbConnectionString))
                        {
                            mainConnection.Open();

                            var targetTable = "HeartRateData";

                            //Data gets inserted using SqlBulkCopy
                            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(mainConnection))
                            {
                                bulkCopy.DestinationTableName = targetTable;
                                bulkCopy.WriteToServer(dataTable);
                            }
                        }
                    }
                }
            }
        }
    }
}
