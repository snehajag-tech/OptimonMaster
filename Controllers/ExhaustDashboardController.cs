using Microsoft.AspNetCore.Mvc;
using System.Data;
using Microsoft.Data.SqlClient;

namespace DashboardsAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ExhaustDashboardController : ControllerBase
    {
        private readonly IConfiguration _configuration;

        public ExhaustDashboardController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        private string ConnectionString => _configuration.GetConnectionString("DBConnStr") ?? throw new InvalidOperationException("Connection string 'DBConnStr' not found.");

        /// <summary>
        /// Returns exhaust historical data for a given tenant, asset parameter, and device.
        /// </summary>
        [HttpGet("GetExhasutHistricalData")]
        public IActionResult GetExhasutHistricalData(string tenantID, string assetParameterId, string deviceId)
        {
            try
            {
                const string query = @"
            SELECT ar.LogDateTime,ap.AssetId,ap.AssetParameterId,ap.ParameterName, ap.DisplayText,ROUND(TRY_CAST(j.Value AS FLOAT), 2) AS Val
            FROM AssetParameters ap
            JOIN AssetReadings3Days ar ON ap.AssetId = ar.AssetId
            CROSS APPLY OPENJSON(ar.JsonPayload)
            WITH (
                Prop NVARCHAR(50),
                Value NVARCHAR(50)
            ) j
            WHERE 
                'P' + CAST(ap.ReferenceParameterId AS VARCHAR) = j.Prop
                AND ap.AssetId = @deviceId
                AND ap.AssetParameterId = @assetParameterId
                AND ap.TenantId = @tenantID
                AND ap.StatusId = 1
                AND ar.LogDateTime BETWEEN DATEADD(HOUR, -24, DATEADD(MINUTE, 330, GETDATE())) 
                                      AND DATEADD(MINUTE, 330, GETDATE())
            ORDER BY ar.LogDateTime ASC;";

                using var connection = new SqlConnection(ConnectionString);
                using var cmd = new SqlCommand(query, connection);
                cmd.Parameters.AddWithValue("@TenantID", tenantID);
                cmd.Parameters.AddWithValue("@assetParameterId", assetParameterId);
                cmd.Parameters.AddWithValue("@deviceId", deviceId);

                using var da = new SqlDataAdapter(cmd);
                var dt = new DataTable();
                connection.Open();
                da.Fill(dt);

                return Ok(DataTableToList(dt));
            }
            catch (Exception ex)
            {
                var error = new { Message = "Error while fetching data.", ExceptionMessage = ex.Message };
                return StatusCode(500, error);
            }
        }

        /// <summary>
        /// Helper method to convert DataTable to List<Dictionary<string, object>> for JSON serialization.
        /// </summary>
        private List<Dictionary<string, object>> DataTableToList(DataTable dt)
        {
            var result = new List<Dictionary<string, object>>();
            foreach (DataRow row in dt.Rows)
            {
                var dict = new Dictionary<string, object>();
                foreach (DataColumn col in dt.Columns)
                {
                    dict[col.ColumnName] = row[col];
                }
                result.Add(dict);
            }
            return result;
        }
    }
}