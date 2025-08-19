using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using System.Data;
using System.Globalization;

namespace SPCCoreMigration.Controllers
{
    [Route("api/[controller]")]
    public class ProcessCapabilityReportController : Controller
    {
        private readonly IConfiguration _configuration;

        private string ConnectionString => _configuration.GetConnectionString("DefaultConnection") ?? throw new InvalidOperationException("Connection string 'DBConnStr' not found.");

        public ProcessCapabilityReportController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        [HttpGet("getConditionData")]
        public IActionResult getConditionData(string tenantID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = "select * from ProcessCapabilityConditions ";
                        var da = new SqlDataAdapter(cmd);
                        connection.Open();
                        da.Fill(dt);
                        var result = DataTableToList(dt);
                        return Ok(result);
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }
        [HttpGet("getcolorConditionData")]
        public IActionResult getcolorConditionData([FromQuery] string tenantID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = @"SELECT Value, JSON_VALUE(Condition, '$[0].Condition') AS Condition, JSON_VALUE(Condition, '$[0].Value') AS JSONValue, JSON_VALUE(Condition, '$[0].Color') AS Color FROM ProcessCapabilityConditions";
                        var da = new SqlDataAdapter(cmd);
                        connection.Open();
                        da.Fill(dt);
                        var result = DataTableToList(dt);
                        return Ok(result);
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("GetFloorsData")]
        public IActionResult GetFloorsData(int buildingID)
        {
            var floors = new List<object>();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = "Select FloorID ,FloorName from Floor Where BuildingID = @BuildingID AND StatusID = 1";
                        cmd.Parameters.AddWithValue("@BuildingID", buildingID);
                        var da = new SqlDataAdapter(cmd);
                        var dt = new DataTable();
                        connection.Open();
                        da.Fill(dt);
                        foreach (DataRow row in dt.Rows)
                        {
                            floors.Add(new
                            {
                                FloorID = row["FloorID"],
                                FloorName = row["FloorName"]
                            });
                        }
                    }
                }

                return Ok(floors);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("GetAssetData")]
        public IActionResult GetAssetData(int branchId)
        {
            var responseData = new
            {
                Assets = new List<object>(),
                Parameters = new List<object>()
            };
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();
                    using (var assetCmd = new SqlCommand("SELECT DeviceID, DeviceName FROM Devices WHERE FloorID = @FloorID AND StatusID = 1", connection))
                    {
                        assetCmd.Parameters.Add("@FloorID", SqlDbType.Int).Value = branchId;
                        using (var reader = assetCmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                responseData.Assets.Add(new
                                {
                                    DeviceID = reader["DeviceID"] != DBNull.Value ? Convert.ToInt32(reader["DeviceID"]) : 0,
                                    DeviceName = reader["DeviceName"].ToString()
                                });
                            }
                        }
                    }
                    using (var paramCmd = new SqlCommand("SELECT pc.ProcessCapabilityID,pc.AssetParameterId,ap.ParameterName,pc.AssetID,d.DeviceName FROM ProcessCapabilityConfiguration pc INNER JOIN Devices d ON pc.AssetId = d.DeviceID INNER JOIN AssetParameters ap ON pc.AssetParameterID = ap.AssetParameterId WHERE d.FloorID = @FloorID AND ap.StatusID = 1", connection))
                    {
                        paramCmd.Parameters.Add("@FloorID", SqlDbType.Int).Value = branchId;
                        using (var reader = paramCmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                responseData.Parameters.Add(new
                                {
                                    AssetParameterId = reader["AssetParameterId"] != DBNull.Value ? Convert.ToInt32(reader["AssetParameterId"]) : 0,
                                    ParameterName = reader["ParameterName"].ToString(),
                                    AssetId = reader["AssetId"] != DBNull.Value ? Convert.ToInt32(reader["AssetId"]) : 0,
                                    DeviceName = reader["DeviceName"].ToString()
                                });
                            }
                        }
                    }
                }
                return Ok(responseData);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "Error retrieving asset data", Details = ex.Message });
            }
        }

        [HttpGet]
        public IActionResult GetAssetData1(int branchId, int DeviceID, int TenantID)
        {
            DataSet ds = new DataSet();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = @"SELECT d.DeviceID, d.FloorID, d.DeviceName, asp.AssetParameterId, asp.ParameterName, aspc.ParameterCategory, aspc.AssetCategoryID, aspc.SortOrder AS CategorySortOrder, asp.SortOrder AS ParameterSortOrder FROM ProcessCapabilityConfiguration AS pc JOIN Devices AS d ON pc.AssetID = d.DeviceID JOIN AssetParameters AS asp ON pc.AssetParameterID = asp.AssetParameterId JOIN AssetParameterCategory AS aspc ON asp.AssetParameterCategoryID = aspc.AssetParameterCategoryID WHERE d.FloorID =@branchId AND (@DeviceID IS NULL OR @DeviceID = 0 OR d.DeviceID = @DeviceID) AND d.StatusID = 1  and asp.StatusId=1 and pc.StatusId=1 and d.TenantID =@TenantID order by d.SortOrder ,aspc.SortOrder, asp.SortOrder;";
                        cmd.Parameters.AddWithValue("@branchId", branchId);
                        cmd.Parameters.AddWithValue("@DeviceID", DeviceID);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);
                        var da = new SqlDataAdapter(cmd);
                        connection.Open();
                        da.Fill(ds);
                    }
                }
                if (ds.Tables.Count > 0)
                {
                    var result = DataTableToList(ds.Tables[0]);
                    return Ok(result);
                }
                else
                {
                    return Ok(new List<Dictionary<string, object>>());
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("getAllAssetsData1")]
        public IActionResult getAllAssetsData1(int branchId, int TenantID)
        {
            DataSet ds = new DataSet();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = "SELECT * FROM Devices d WHERE d.FloorID = @branchId AND d.StatusID = 1 AND d.TenantID = @TenantID";
                        cmd.Parameters.AddWithValue("@branchId", branchId);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);
                        var da = new SqlDataAdapter(cmd);
                        connection.Open();
                        da.Fill(ds);
                    }
                }
                if (ds.Tables.Count > 0)
                {
                    var result = DataTableToList(ds.Tables[0]);
                    return Ok(result);
                }
                else
                {
                    return Ok(new List<Dictionary<string, object>>());
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("GetTimeZoneCode")]
        public IActionResult GetTimeZoneCode(string buildingID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = "SELECT * FROM AspenLoadingPlantOrder WHERE buildingID = @buildingID";
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@buildingID", buildingID);
                        using (var da = new SqlDataAdapter(cmd))
                        {
                            connection.Open();
                            da.Fill(dt);
                        }
                    }
                }
                return Ok(DataTableToList(dt));
            }            
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }



        // Replace the following method to fix CS0029 error
        [Route("GetAssetParametersData")]
        [HttpGet]
        public IActionResult GetAssetParametersData(int assetID)
        {
            var assetsparameters = new List<object>();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                using (var cmd = new SqlCommand())
                {
                    cmd.Connection = connection;
                    cmd.CommandText = "SELECT pc.AssetParameterId, ap.ParameterName FROM ProcessCapabilityConfiguration pc JOIN AssetParameters ap ON pc.AssetParameterID = ap.AssetParameterId WHERE pc.AssetID = @AssetID AND pc.StatusID = 1";
                    cmd.Parameters.Add(new SqlParameter("@AssetID", SqlDbType.Int) { Value = assetID });
                    connection.Open();
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            assetsparameters.Add(new
                            {
                                AssetParameterId = reader["AssetParameterId"],
                                ParameterName = reader["ParameterName"]
                            });
                        }
                    }
                }
                return Ok(assetsparameters);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        // Replace the following method to fix CS0029 error
        [HttpGet]
        [Route("GetProcessCapabilityDetails")]
        public IActionResult GetProcessCapabilityDetails(string parameterIds, int tenantId, string fromDate, string toDate)
        {
            try
            {
                var parameterIdList = parameterIds.Split(',').Select(id => id.Trim()).ToList();
                string parameterIdParams = string.Join(",", parameterIdList.Select((id, index) => $"@ParameterId{index}"));
                List<dynamic> results = new List<dynamic>();
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();
                    string query = $@"SELECT ap.AssetParameterId, ap.ParameterName, cr.Cp, cr.Ppk, cr.SampleN_Pp AS Data_Points, cr.SampleMean_Pp AS Sample_Mean, cr.StDev_Overall AS StDev_Overall, cr.StDev_Within AS StDev_Within, cr.LCL AS LSL, cr.UCL AS USL, cr.Target, cr.ImgSRC FROM ProcessCapabilityReadings cr JOIN ProcessCapabilityConfiguration pc ON cr.ProcessCapabilityID = pc.ProcessCapabilityID JOIN AssetParameters ap ON ap.AssetParameterId = pc.AssetParameterID WHERE ap.AssetParameterId IN ({parameterIdParams}) AND ap.TenantId = @TenantId AND cr.FromDate = @FromDate AND cr.ToDate = @ToDate;";
                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        for (int i = 0; i < parameterIdList.Count; i++)
                        {
                            command.Parameters.AddWithValue($"@ParameterId{i}", parameterIdList[i]);
                        }
                        command.Parameters.AddWithValue("@TenantId", tenantId);
                        command.Parameters.AddWithValue("@FromDate", fromDate);
                        command.Parameters.AddWithValue("@ToDate", toDate);
                        using (SqlDataAdapter adapter = new SqlDataAdapter(command))
                        {
                            DataTable dataTable = new DataTable();
                            adapter.Fill(dataTable);
                            results = dataTable.AsEnumerable().Select(row => new
                            {
                                AssetParameterId = row["AssetParameterId"],
                                ParameterName = row["ParameterName"],
                                Cp = row["Cp"],
                                Ppk = row["Ppk"],
                                Data_Points = row["Data_Points"],
                                Sample_Mean = row["Sample_Mean"],
                                StDev_Overall = row["StDev_Overall"],
                                StDev_Within = row["StDev_Within"],
                                LSL = row["LSL"],
                                USL = row["USL"],
                                Target = row["Target"],
                                ImgSRC = row["ImgSRC"]
                            }).ToList<dynamic>();
                        }
                    }
                }
                var jsonResult = JsonConvert.SerializeObject(new { Status = "Success", Results = results });
                return Content(jsonResult, "application/json");
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("getPlantDashboardData")]
        public IActionResult GetPlantDashboardData(string tenantID, string userID, string fromDate, string toDate)
        {
            DataSet ds = new DataSet();
            try
            {
                string query = "USP_GetSPCPlantwiseDashboardData_Demo";
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@tenantID", tenantID);
                    cmd.Parameters.AddWithValue("@userID", userID);
                    cmd.Parameters.AddWithValue("@fromDate", fromDate);
                    cmd.Parameters.AddWithValue("@toDate", toDate);
                    conn.Open();
                    SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                    adapter.Fill(ds);
                }

                if (ds.Tables.Count > 0)
                {
                    var result = DataTableToList(ds.Tables[0]);
                    return Ok(result);
                }
                else
                {
                    return Ok(new List<Dictionary<string, object>>());
                }
            }
            catch (Exception ex)
            {
                // For robustness, return an empty list on an exception.
                // It's also a good practice to log the exception here.
                return Ok(new List<Dictionary<string, object>>());
            }
        }

        [HttpGet("getPlantData")]
        public IActionResult getPlantData(string from, string to, string utilityID, string tenantID)
        {
            DataSet ds = new DataSet();
            try
            {
                string query = "USP_GetPlantwiseDashboardDataForSPC";
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@FromDate", from);
                    cmd.Parameters.AddWithValue("@ToDate", to);
                    cmd.Parameters.AddWithValue("@BuildingID", utilityID);
                    cmd.Parameters.AddWithValue("@TenantID", tenantID);
                    conn.Open();
                    SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                    adapter.Fill(ds);
                }
                if (ds.Tables.Count > 0)
                {
                    var result = DataTableToList(ds.Tables[0]);
                    return Ok(result);
                }
                else
                {
                    return Ok(new List<Dictionary<string, object>>());
                }
            }
            catch (Exception ex)
            {
                return Ok(new DataSet());
            }
        }

        [HttpGet("GetAssetParameters")]
        public IActionResult GetAssetParameters(string tenantID, string floorID, string userID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = "select * from AssetDefaultParameters_SPCMultiSelection WHERE TenantID = @tenantID and FloorID=@floorID and  Userid=@userID and  StatusID=1 ";
                        cmd.Parameters.AddWithValue("@TenantID", tenantID);
                        cmd.Parameters.AddWithValue("@FloorID", floorID);
                        cmd.Parameters.AddWithValue("@Userid", userID);
                        var da = new SqlDataAdapter(cmd);
                        connection.Open();
                        da.Fill(dt);
                        var result = DataTableToList(dt);
                        return Ok(result);
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("GetParameters")]
        public IActionResult GetParameters(string tenantID, string floorID, string userID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = "select * from AssetDefaultParameters_SPCMultiSelection WHERE TenantID = @tenantID and FloorID=@floorID and Userid=@userID and StatusID=1 ";
                        cmd.Parameters.AddWithValue("@TenantID", tenantID);
                        cmd.Parameters.AddWithValue("@FloorID", floorID);
                        cmd.Parameters.AddWithValue("@Userid", userID);
                        var da = new SqlDataAdapter(cmd);
                        connection.Open();
                        da.Fill(dt);
                        var result = DataTableToList(dt);
                        return Ok(result);
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("GetStatus")]
        public IActionResult GetStatus(string tenantId, string floorID, string parameterID)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = "DELETE FROM AssetDefaultParameters_SPCMultiSelection WHERE TenantID = @TenantID AND FloorID = @FloorID AND AssetParameterID = @AssetParameterID;";
                        cmd.Parameters.AddWithValue("@TenantID", tenantId);
                        cmd.Parameters.AddWithValue("@FloorID", floorID);
                        cmd.Parameters.AddWithValue("@AssetParameterID", parameterID);
                        connection.Open();
                        int rowsAffected = cmd.ExecuteNonQuery();
                        if (rowsAffected > 0)
                        {
                            return Ok("Status updated successfully.");
                        }
                        else
                        {
                            return NotFound("No matching record found.");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpPost("getLclUclData")]
        public IActionResult getLclUclData(string tenantId, string assetID, string processCapabilityID, string fromDate, string toDate)
        {
            SqlCommand cmd = new SqlCommand("[dbo].[USP_GetLclUclData]");
            var ds = new DataSet();
            try
            {
                cmd.Connection = new SqlConnection(ConnectionString);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@fromdt", fromDate);
                cmd.Parameters.AddWithValue("@todt", toDate);
                cmd.Parameters.AddWithValue("@processCapabilityID", processCapabilityID);
                cmd.Parameters.AddWithValue("@assetID", assetID);
                cmd.Parameters.AddWithValue("@tenantId", tenantId);
                var da1 = new SqlDataAdapter { SelectCommand = cmd };
                if (cmd.Connection != null && cmd.Connection.State == ConnectionState.Closed)
                    cmd.Connection.Open();
                da1.Fill(ds);
                if (ds.Tables.Count > 0)
                {
                    var result = DataTableToList(ds.Tables[0]);
                    return Ok(result);
                }
                else
                {
                    return Ok(new List<Dictionary<string, object>>());
                }
            }
            catch (Exception ex)
            {
                return Ok(ds);
            }
            finally
            {
                cmd.Connection.Close();
            }
        }

        [HttpPost("GetSelectedParameters")]
        public IActionResult GetSelectedParameters(string tenantID, string CreatedBy, string CreatedDate, string AssetID, string FloorID, string userID, string Parameters, string deletedParams)
        {
            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    DateTime createdDateTime;
                    if (!DateTime.TryParseExact(CreatedDate, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out createdDateTime))
                    {
                        return BadRequest("Invalid CreatedDate format. Expected: yyyy-MM-dd HH:mm:ss");
                    }
                    string insertQuery = @"IF NOT EXISTS (SELECT 1 FROM AssetDefaultParameters_SPCMultiSelection WHERE TenantID = @TenantID AND FloorID = @FloorID AND AssetParameterID = @ParameterID and UserID =@userID) BEGIN INSERT INTO AssetDefaultParameters_SPCMultiSelection (TenantID, CreatedBy, CreatedDate, FloorID, AssetParameterID,UserID) VALUES (@TenantID, @CreatedBy, @CreatedDate, @FloorID, @ParameterID,@userID) END";
                    if (!string.IsNullOrEmpty(Parameters))
                    {
                        string[] parameterList = Parameters.Split(',');
                        using (SqlCommand cmd = new SqlCommand(insertQuery, conn))
                        {
                            cmd.Parameters.AddWithValue("@TenantID", tenantID);
                            cmd.Parameters.AddWithValue("@CreatedBy", CreatedBy);
                            cmd.Parameters.AddWithValue("@CreatedDate", createdDateTime);
                            cmd.Parameters.AddWithValue("@FloorID", FloorID);
                            cmd.Parameters.AddWithValue("@UserID", userID);
                            cmd.Parameters.Add("@ParameterID", SqlDbType.NVarChar);
                            foreach (var param in parameterList)
                            {
                                string trimmedParam = param.Trim();
                                if (!string.IsNullOrEmpty(trimmedParam))
                                {
                                    cmd.Parameters["@ParameterID"].Value = trimmedParam;
                                    cmd.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                    if (!string.IsNullOrEmpty(deletedParams))
                    {
                        string[] deletedParamList = deletedParams.Split(',');
                        string deleteQuery = @"DELETE FROM AssetDefaultParameters_SPCMultiSelection WHERE TenantID = @TenantID AND FloorID = @FloorID AND AssetParameterID = @ParameterID and UserID=@userID";
                        using (SqlCommand cmd = new SqlCommand(deleteQuery, conn))
                        {
                            cmd.Parameters.AddWithValue("@TenantID", tenantID);
                            cmd.Parameters.AddWithValue("@FloorID", FloorID);
                            cmd.Parameters.AddWithValue("@UserID", userID);
                            cmd.Parameters.Add("@ParameterID", SqlDbType.NVarChar);
                            foreach (var param in deletedParamList)
                            {
                                string trimmedParam = param.Trim();
                                if (!string.IsNullOrEmpty(trimmedParam))
                                {
                                    cmd.Parameters["@ParameterID"].Value = trimmedParam;
                                    cmd.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                    return Ok("Parameters saved successfully!");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Error: {ex.Message}");
            }
        }

        [HttpPost("getDecimalCountList")]
        public IActionResult GetDecimalCountList(string Parameters)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();
                    string insertQuery = "SELECT AssetParameterID, DecimalCount FROM ProcessCapabilityConfiguration WHERE AssetParameterID IN (@Parameters)";
                    if (!string.IsNullOrEmpty(Parameters))
                    {
                        string[] parameterList = Parameters.Split(',');
                        string inClause = string.Join(",", parameterList.Select(p => $"'{p.Trim()}'"));
                        insertQuery = insertQuery.Replace("@Parameters", inClause);
                        using (SqlCommand cmd = new SqlCommand(insertQuery, connection))
                        {
                            using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                            {
                                adapter.Fill(dt);
                            }
                        }
                    }
                }
                return Ok(DataTableToList(dt));
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpPost("saveComment")]
        public IActionResult SaveComment([FromBody] List<ProcessCapabilityoutoflimitInfo> model)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();
                    foreach (var item in model)
                    {
                        using (var cmd = new SqlCommand("USP_SaveProcessCapabilityOutofLimit", connection))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.Parameters.AddWithValue("@ProcessCapabilityID", item.ProcessCapabilityID);
                            cmd.Parameters.AddWithValue("@LogTime", item.LogTime);
                            cmd.Parameters.AddWithValue("@Value", item.Value);
                            cmd.Parameters.AddWithValue("@LCL", item.LCL);
                            cmd.Parameters.AddWithValue("@UCL", item.UCL);
                            cmd.Parameters.AddWithValue("@Target", item.Target);
                            cmd.Parameters.AddWithValue("@Comment", (object)item.Comment ?? DBNull.Value);
                            cmd.Parameters.AddWithValue("@CreatedDate", item.CreatedDate);
                            cmd.Parameters.AddWithValue("@CreatedBy", (object)item.CreatedBy ?? DBNull.Value);
                            cmd.Parameters.AddWithValue("@ModifiedDate", item.ModifiedDate);
                            cmd.Parameters.AddWithValue("@ModifiedBy", (object)item.ModifiedBy ?? DBNull.Value);
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                return Ok("Comments saved successfully.");
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }

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
    // To fix CS0246, you need to define the missing type 'ProcessCapabilityoutoflimitInfo'.
    // If you have a specification for this type, please provide it.
    // Otherwise, you can add a placeholder class definition like below (adjust properties as needed):

    public class ProcessCapabilityoutoflimitInfo
    {
        public int ProcessCapabilityID { get; set; }
        public DateTime LogTime { get; set; }
        public decimal Value { get; set; }
        public decimal LCL { get; set; }
        public decimal UCL { get; set; }
        public decimal Target { get; set; }
        public string? Comment { get; set; }
        public DateTime CreatedDate { get; set; }
        public string? CreatedBy { get; set; }
        public DateTime ModifiedDate { get; set; }
        public string? ModifiedBy { get; set; }
    }
}
