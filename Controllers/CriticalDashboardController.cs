using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using System.Data;

namespace DashboardsAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CriticalDashboardController : ControllerBase
    {
        private readonly IConfiguration _configuration;

        public CriticalDashboardController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        private string ConnectionString => _configuration.GetConnectionString("DBConnStr") ?? throw new InvalidOperationException("Connection string 'DBConnStr' not found.");

        /// <summary>
        /// Returns critical parameters data for a tenant.
        /// </summary>
        [HttpGet("GetCriticalParametersData")]
        public IActionResult GetCriticalParametersData(string tenantID)
        {
            try
            {
                using var connection = new SqlConnection(ConnectionString);
                const string query = @"
                SELECT 
                    cp.CriticalParameterId, ap.ParameterName,
                    cp.IsCritical,cp.HH,ap.Units,cp.AssetParameterId,cp.IsCritical,
                    cp.H,cp.L,cp.LL,cp.TenantId,cp.StatusId,ROUND(alr.Val, 2) AS Val,alr.LogDateTime 
                FROM 
                    CriticalParameters as cp 
                JOIN 
                    AssetParameters as ap ON cp.AssetParameterId = ap.AssetParameterId 
                LEFT JOIN 
                    AssetLiveReadingsV2 as alr ON 'P' + CAST(ap.ReferenceParameterId AS VARCHAR) = alr.Prop 
                    AND cp.AssetId = alr.AssetId 
                WHERE 
                    cp.Statusid = 1 AND cp.TenantId = @TenantID AND cp.AssetId = 937;";
                using var cmd = new SqlCommand(query, connection);
                cmd.Parameters.AddWithValue("@TenantID", tenantID);
                connection.Open();
                var dt = new DataTable();
                using (var da = new SqlDataAdapter(cmd))
                {
                    da.Fill(dt);
                }
                return Ok(DataTableToList(dt));
            }
            catch (Exception ex)
            {
                var error = new { Message = "Error while fetching data.", ExceptionMessage = ex.Message };
                return StatusCode(500, error);
            }
        }

        /// <summary>
        /// Returns historical data for a critical parameter.
        /// </summary>
        [HttpGet("GetCriticalHistoricalData")]
        public IActionResult GetCriticalHistoricalData(string tenantID, string CriticalParameterid)
        {
            try
            {
                using var connection = new SqlConnection(ConnectionString);
                const string query = @"
               DECLARE @NowIST DATETIME = DATEADD(MINUTE, 330, GETDATE());

                SELECT 
                    ar.LogDateTime,
                    cp.CriticalParameterId,
                    ap.ParameterName,
                    cp.IsCritical,
                    cp.HH,
                    ap.Units,
                    cp.H,
                    cp.L,
                    cp.LL,
                    cp.TenantId,
                    cp.StatusId,
                    ROUND(TRY_CAST(j.Value AS FLOAT), 2) AS Val
                FROM CriticalParameters cp
                JOIN AssetParameters ap ON cp.AssetParameterId = ap.AssetParameterId
                JOIN AssetReadings3Days ar ON cp.AssetId = ar.AssetId
                CROSS APPLY OPENJSON(ar.JsonPayload)
                WITH (
                    Prop NVARCHAR(50),
                    Value NVARCHAR(50)
                ) j
                WHERE 
                    'P' + CAST(ap.ReferenceParameterId AS VARCHAR) = j.Prop
                    AND cp.Statusid = 1
                    AND cp.TenantId = @tenantID
                    AND cp.AssetId = 937
                    AND cp.CriticalParameterId = @CriticalParameterid
                    AND ar.LogDateTime BETWEEN DATEADD(HOUR, -24, @NowIST) AND @NowIST
                ORDER BY 
                    ar.LogDateTime DESC;";
                using var cmd = new SqlCommand(query, connection);
                cmd.Parameters.AddWithValue("@TenantID", tenantID);
                cmd.Parameters.AddWithValue("@CriticalParameterid", CriticalParameterid);
                connection.Open();
                var dt = new DataTable();
                using (var da = new SqlDataAdapter(cmd))
                {
                    da.Fill(dt);
                }
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