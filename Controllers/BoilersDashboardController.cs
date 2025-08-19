using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using System.Data;

namespace DashboardsAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class BoilersDashboardController : ControllerBase
    {
        private readonly IConfiguration _configuration;

        public BoilersDashboardController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        private string ConnectionString => _configuration.GetConnectionString("DBConnStr") ?? throw new InvalidOperationException("Connection string 'DBConnStr' not found.");

        /// <summary>
        /// Returns historical parameters data for a tenant.
        /// </summary>
        [HttpGet("getHistoricalParametersData")]
        public IActionResult GetHistoricalParametersData(string tenantID)
        {
            try
            {
                using var connection = new SqlConnection(ConnectionString);
                using var cmd = new SqlCommand("GetDeviceData", connection);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@TenantId", tenantID);

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
        /// Returns CPP dashboard historical data for a tenant and asset parameter.
        /// </summary>
        [HttpGet("GetCPPDashboardlHistricalData")]
        public IActionResult GetCPPDashboardlHistricalData(string tenantID, string assetParameterId)
        {
            try
            {
                const string query = @"
            DECLARE @NowIST DATETIME = DATEADD(MINUTE, 330, GETDATE());

            SELECT 
                d.deviceName,
                CASE 
                    WHEN apc.ParameterCategory LIKE '%Pressure%' THEN 'Pressure'
                    WHEN apc.ParameterCategory LIKE '%Temperature%' THEN 'Temperature'
                    ELSE 'Others'
                END AS CustomLabel,
                ar.LogDateTime,
                ap.ParameterName,
                ap.AssetParameterId,
                apc.ParameterCategory,
                ROUND(TRY_CAST(j.Value AS FLOAT), 2) AS Val,
                ap.Units,
                ap.SortOrder
            FROM  
                devices d 
            JOIN AssetParameters ap ON d.DeviceID = ap.AssetId
            JOIN AssetParameterCategory apc ON ap.AssetParameterCategoryID = apc.AssetParameterCategoryId
            JOIN AssetReadings3Days ar ON ap.AssetId = ar.AssetId
            CROSS APPLY OPENJSON(ar.JsonPayload)
            WITH (
                Prop NVARCHAR(50),
                Value NVARCHAR(50)
            ) j
            WHERE 
                'P' + CAST(ap.ReferenceParameterId AS VARCHAR) = j.Prop
                AND ap.Statusid = 1
                AND ap.AssetParameterId = @assetParameterId
                AND ap.TenantId = @tenantID
                AND TRY_CAST(j.Value AS FLOAT) IS NOT NULL
                AND ar.LogDateTime BETWEEN DATEADD(HOUR, -24, @NowIST) AND @NowIST
            ORDER BY 
                ar.LogDateTime DESC;
            ";

                using var connection = new SqlConnection(ConnectionString);
                using var cmd = new SqlCommand(query, connection);
                cmd.Parameters.AddWithValue("@tenantID", tenantID);
                cmd.Parameters.AddWithValue("@assetParameterId", assetParameterId);

                using var da = new SqlDataAdapter(cmd);
                var dt = new DataTable();
                connection.Open();
                da.Fill(dt);

                return Ok(DataTableToList(dt));
            }
            catch (Exception ex)
            {
                var error = new
                {
                    Message = "Error while fetching data.",
                    ExceptionMessage = ex.Message
                };
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