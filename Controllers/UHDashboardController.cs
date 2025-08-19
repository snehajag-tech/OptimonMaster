using Microsoft.AspNetCore.Mvc;
using System.Data;
using Microsoft.Data.SqlClient;

namespace DashboardsAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class UHDashboardController : ControllerBase
    {
        private readonly IConfiguration _configuration;

        /// <summary>
        /// Initializes a new instance of the <see cref="UHDashboardController"/> class.
        /// </summary>
        public UHDashboardController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        private string ConnectionString => _configuration.GetConnectionString("DBConnStr") ?? throw new InvalidOperationException("Connection string 'DBConnStr' not found.");

        /// <summary>
        /// Returns gain/loss data for a given month and year.
        /// </summary>
        [HttpGet("GainLoss")]
        public IActionResult GetGainLossData(string tenantId, string month, string year)
        {
            try
            {
                using var connection = new SqlConnection(ConnectionString);
                const string query = @"
                    SELECT glu.DepartmentID,Department, Total, Month, Year,IsClickable, [1], [2], [3], [4], [5], [6], [7], [8], [9], [10], 
                                [11], [12], [13], [14], [15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25], 
                                [26], [27], [28], [29], [30], [31] 
                         FROM Gain_LossSummary_UH glu  join PlantWiseDepartment_UH pwd on glu.DepartmentID=pwd.DepartmentID
                         WHERE Month = @Month AND Year = @Year and glu.StatusID = 1 and pwd.StatusID=1 ;";
                using var command = new SqlCommand(query, connection);
                command.CommandType = CommandType.Text;
                command.Parameters.AddWithValue("@Month", month);
                command.Parameters.AddWithValue("@Year", year);
                connection.Open();
                var dt = new DataTable();
                using (var adapter = new SqlDataAdapter(command))
                {
                    adapter.Fill(dt);
                }
                return Ok(DataTableToList(dt));
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "Error while fetching data.", ExceptionMessage = ex.Message });
            }
        }

        /// <summary>
        /// Returns section performance ratio data for a department and month.
        /// </summary>
        [HttpGet("SectionPerformanceRatio")]
        public IActionResult GetSectionPerformanceRatioData(string tenantId, string departmentId, string month)
        {
            try
            {
                using var connection = new SqlConnection(ConnectionString);
                using var command = new SqlCommand("USP_GetDepartmentSectionPerformance", connection);
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.AddWithValue("@DepartmentID", departmentId);
                command.Parameters.AddWithValue("@Month", month);
                using var adapter = new SqlDataAdapter(command);
                var ds = new DataSet();
                adapter.Fill(ds);
                var result = new
                {
                    RatioData = ds.Tables.Count > 0 ? DataTableToList(ds.Tables[0]) : null,
                    GainLossData = ds.Tables.Count > 1 ? DataTableToList(ds.Tables[1]) : null
                };
                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "Error while fetching data.", ExceptionMessage = ex.Message });
            }
        }

        /// <summary>
        /// Returns production data for a given month and year.
        /// </summary>
        [HttpGet("Production")]
        public IActionResult GetProductionData(string tenantId, string month, string year)
        {
            try
            {
                using var connection = new SqlConnection(ConnectionString);
                const string query = @"
                select ptu.Name TitleName,pdu.* from ProductionType_UH ptu
                join ProductionData_UH  pdu on ptu.ProductionTypeId=pdu.ProductionTypeId
                WHERE Month = @Month AND Year = @Year and pdu.TenantID = @TenantID AND pdu.StatusID = 1";
                using var command = new SqlCommand(query, connection);
                command.CommandType = CommandType.Text;
                command.Parameters.AddWithValue("@TenantID", tenantId);
                command.Parameters.AddWithValue("@Month", month);
                command.Parameters.AddWithValue("@Year", year);
                connection.Open();
                var dt = new DataTable();
                using (var adapter = new SqlDataAdapter(command))
                {
                    adapter.Fill(dt);
                }
                return Ok(DataTableToList(dt));
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "Error while fetching data.", ExceptionMessage = ex.Message });
            }
        }

        /// <summary>
        /// Returns quality data for a given month and year.
        /// </summary>
        [HttpGet("Quality")]
        public IActionResult GetQualityData(string tenantId, string month, string year)
        {
            try
            {
                using var connection = new SqlConnection(ConnectionString);
                const string query = @"
               select qtu.Name TitleName,qdu.* from QualityType_UH qtu join
                QualityData_UH qdu on qtu.QualityTypeId=qdu.QualityTypeId
                WHERE  Month = @Month AND Year = @Year and qtu.TenantID = @TenantID AND qtu.StatusID = 1";
                using var command = new SqlCommand(query, connection);
                command.CommandType = CommandType.Text;
                command.Parameters.AddWithValue("@TenantID", tenantId);
                command.Parameters.AddWithValue("@Month", month);
                command.Parameters.AddWithValue("@Year", year);
                connection.Open();
                var dt = new DataTable();
                using (var adapter = new SqlDataAdapter(command))
                {
                    adapter.Fill(dt);
                }
                return Ok(DataTableToList(dt));
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "Error while fetching data.", ExceptionMessage = ex.Message });
            }
        }

        /// <summary>
        /// Returns ratios data for a given month and year.
        /// </summary>
        [HttpGet("Ratios")]
        public IActionResult GetRatiosData(string tenantId, string month, string year)
        {
            try
            {
                using var connection = new SqlConnection(ConnectionString);
                const string query = @"
               select rtu.Name TittleName,rdu.* from RatioType_UH rtu
            join RatioData_UH  rdu on rtu.RatioTypeId=rdu.RatioTypeId
                WHERE Month = @Month AND Year = @Year  and rdu.TenantID = @TenantID AND rdu.StatusID = 1";
                using var command = new SqlCommand(query, connection);
                command.CommandType = CommandType.Text;
                command.Parameters.AddWithValue("@TenantID", tenantId);
                command.Parameters.AddWithValue("@Month", month);
                command.Parameters.AddWithValue("@Year", year);
                connection.Open();
                var dt = new DataTable();
                using (var adapter = new SqlDataAdapter(command))
                {
                    adapter.Fill(dt);
                }
                return Ok(DataTableToList(dt));
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "Error while fetching data.", ExceptionMessage = ex.Message });
            }
        }

        /// <summary>
        /// Returns gain/loss summary data.
        /// </summary>
        [HttpGet("GainLossSummary")]
        public IActionResult GetGainLossSummaryData()
        {
            try
            {
                using var connection = new SqlConnection(ConnectionString);
                const string query = "SELECT glu.DepartmentID,Department, Total, Month, Year,IsClickable, [1], [2], [3], [4], [5], [6], [7], [8], [9], [10], " +
                                   "[11], [12], [13], [14], [15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25], " +
                                   "[26], [27], [28], [29], [30], [31] FROM Gain_LossSummary_UH glu  join PlantWiseDepartment_UH pwd on glu.DepartmentID=pwd.DepartmentID";
                using var command = new SqlCommand(query, connection);
                connection.Open();
                var dt = new DataTable();
                using (var adapter = new SqlDataAdapter(command))
                {
                    adapter.Fill(dt);
                }
                return Ok(DataTableToList(dt));
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "Error while fetching data.", ExceptionMessage = ex.Message });
            }
        }

        /// <summary>
        /// Returns raw material data for a given period.
        /// </summary>
        [HttpGet("RawMaterialData")]
        public IActionResult GetRawMaterialData(string tenantId, string startMonth, string startYear, string endMonth, string endYear)
        {
            var dt = new DataTable();
            try
            {
                const string query = "GetMaterialDataPivotedByMonth";
                using var conn = new SqlConnection(ConnectionString);
                using var cmd = new SqlCommand(query, conn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@StartYear", startYear);
                cmd.Parameters.AddWithValue("@StartMonth", startMonth);
                cmd.Parameters.AddWithValue("@EndYear", endYear);
                cmd.Parameters.AddWithValue("@EndMonth", endMonth);
                using var adapter = new SqlDataAdapter(cmd);
                conn.Open();
                adapter.Fill(dt);
                return Ok(DataTableToList(dt));
            }
            catch (Exception ex)
            {
                var error = new { Message = "Error while fetching data.", ExceptionMessage = ex.Message };
                return StatusCode(500, error);
            }
        }

        /// <summary>
        /// Returns safety data for a given month and year.
        /// </summary>
        [HttpGet("SafetyDataByMonthYear")]
        public IActionResult GetSafetyDataByGivenMonthYear(string tenantId, string startMonth, string startYear)
        {
            var dt = new DataTable();
            try
            {
                const string query = "USP_GetSafetyParticularsByMonthYear";
                using var conn = new SqlConnection(ConnectionString);
                using var cmd = new SqlCommand(query, conn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@FromYear", startYear);
                cmd.Parameters.AddWithValue("@FromMonth", startMonth);
                cmd.Parameters.AddWithValue("@TenantID", tenantId);
                using var adapter = new SqlDataAdapter(cmd);
                conn.Open();
                adapter.Fill(dt);
                return Ok(DataTableToList(dt));
            }
            catch (Exception ex)
            {
                var error = new { Message = "Error while fetching data.", ExceptionMessage = ex.Message };
                return StatusCode(500, error);
            }
        }

        /// <summary>
        /// Returns other production data for a tenant.
        /// </summary>
        [HttpGet("OtherProduction")]
        public IActionResult GetOtherProductionData(string tenantId)
        {
            try
            {
                using var connection = new SqlConnection(ConnectionString);
                const string query = @"
                SELECT Year, Month,OtherProduction, Unit,PreviousMonthBudget,Budget,CurrentMonthBudget,
                       [1], [2], [3], [4], [5], [6], [7], [8], [9], [10],
                       [11], [12], [13], [14], [15], [16], [17], [18], [19], [20],
                       [21], [22], [23], [24], [25], [26], [27], [28], [29], [30], [31],MTD
                FROM OtherProduction_UH
                WHERE TenantID = @TenantID AND StatusID = 1";
                using var command = new SqlCommand(query, connection);
                command.CommandType = CommandType.Text;
                command.Parameters.AddWithValue("@TenantID", tenantId);
                connection.Open();
                var dt = new DataTable();
                using (var adapter = new SqlDataAdapter(command))
                {
                    adapter.Fill(dt);
                }
                return Ok(DataTableToList(dt));
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "Error while fetching data.", ExceptionMessage = ex.Message });
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