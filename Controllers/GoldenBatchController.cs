using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DashboardsAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class GoldenBatchController : ControllerBase
    {
        private readonly IConfiguration _configuration;

        public GoldenBatchController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        private string ConnectionString => _configuration.GetConnectionString("DBConnStr") ?? string.Empty;

        /// <summary>
        /// Returns golden batch data for a given asset and date range.
        /// </summary>
        [HttpGet("GetGoldenBatchData")]
        public async Task<IActionResult> GetGoldenBatchData(string from, string to, string tenantId, string assetId)
        {
            if (string.IsNullOrWhiteSpace(from) || string.IsNullOrWhiteSpace(to) || string.IsNullOrWhiteSpace(assetId))
                return BadRequest(new { Message = "Missing required parameters." });
            try
            {
                var resultList = new List<Dictionary<string, object>>();
                string formattedFrom = DateTime.Parse(from).ToString("yyyy-MM-dd");
                string formattedTo = DateTime.Parse(to).ToString("yyyy-MM-dd");
                const string query = "USP_GetBatchAnalysis_New";

                using var conn = new SqlConnection(ConnectionString);
                using var cmd = new SqlCommand(query, conn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@FromDate", formattedFrom);
                cmd.Parameters.AddWithValue("@ToDate", formattedTo);
                cmd.Parameters.AddWithValue("@AssetId", assetId);
                await conn.OpenAsync();
                using var reader = await cmd.ExecuteReaderAsync();
                var dt = new DataTable();
                dt.Load(reader);
                resultList = DataTableToList(dt);
                return Ok(resultList);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "Error while fetching data.", ExceptionMessage = ex.Message });
            }
        }

        /// <summary>
        /// Returns process capability data for a given asset parameter.
        /// </summary>
        [HttpGet("GetProcessCapabilityData")]
        public async Task<IActionResult> GetProcessCapabilityData(string tenantId, string assetParameterId)
        {
            if (string.IsNullOrWhiteSpace(tenantId) || string.IsNullOrWhiteSpace(assetParameterId))
                return BadRequest(new { Message = "Missing required parameters." });
            try
            {
                var resultList = new List<Dictionary<string, object>>();
                const string query = @"SELECT * FROM ProcessCapabilityConfiguration WHERE AssetParameterId = @AssetParameterId AND TenantId = @TenantId";
                using var conn = new SqlConnection(ConnectionString);
                using var cmd = new SqlCommand(query, conn);
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.AddWithValue("@AssetParameterId", assetParameterId);
                cmd.Parameters.AddWithValue("@TenantId", tenantId);
                await conn.OpenAsync();
                using var reader = await cmd.ExecuteReaderAsync();
                var dt = new DataTable();
                dt.Load(reader);
                resultList = DataTableToList(dt);
                return Ok(resultList);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "Error while fetching data.", ExceptionMessage = ex.Message });
            }
        }

        /// <summary>
        /// Returns LCL/UCL data for a golden batch asset and date range.
        /// </summary>
        [HttpPost("GetGoldeLclUclData")]
        public async Task<IActionResult> GetGoldeLclUclData(string tenantId, string assetId, string fromDate, string toDate)
        {
            if (string.IsNullOrWhiteSpace(tenantId) || string.IsNullOrWhiteSpace(assetId) || string.IsNullOrWhiteSpace(fromDate) || string.IsNullOrWhiteSpace(toDate))
                return BadRequest(new { Message = "Missing required parameters." });
            try
            {
                var resultList = new List<List<Dictionary<string, object>>>();
                using var conn = new SqlConnection(ConnectionString);
                using var cmd = new SqlCommand("[dbo].[USP_GetLclUclDataByAsset]", conn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@tenantId", tenantId);
                cmd.Parameters.AddWithValue("@assetID", assetId);
                cmd.Parameters.AddWithValue("@todt", toDate);
                cmd.Parameters.AddWithValue("@fromdt", fromDate);
                await conn.OpenAsync();
                using var reader = await cmd.ExecuteReaderAsync();
                do
                {
                    var tableList = new List<Dictionary<string, object>>();
                    var dt = new DataTable();
                    dt.Load(reader);
                    tableList = DataTableToList(dt);
                    resultList.Add(tableList);
                } while (!reader.IsClosed && reader.NextResult());
                return Ok(resultList);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "Error while fetching data.", ExceptionMessage = ex.Message });
            }
        }

        /// <summary>
        /// Returns golden batch data for all assets in a date range.
        /// </summary>
        [HttpGet("GetGoldenBatchData_AllAssets")]
        public async Task<IActionResult> GetGoldenBatchData_AllAssets(string from, string to, string tenantId, string userId, string buildingIDs, string deviceIDs)
        {
            if (string.IsNullOrWhiteSpace(from) || string.IsNullOrWhiteSpace(to) || string.IsNullOrWhiteSpace(tenantId) || string.IsNullOrWhiteSpace(deviceIDs))
                return BadRequest(new { Message = "Missing required parameters." });
            try
            {
                string formattedFrom = DateTime.Parse(from).ToString("yyyy-MM-dd");
                string formattedTo = DateTime.Parse(to).ToString("yyyy-MM-dd");
                if (!int.TryParse(tenantId, out int TenantID))
                    return BadRequest(new { Message = "Invalid TenantID" });
                const string query = "USP_GoldenBatch_LandingDashboard_New";
                var resultList = new List<Dictionary<string, object>>();
                using var conn = new SqlConnection(ConnectionString);
                using var cmd = new SqlCommand(query, conn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@FromDate", formattedFrom);
                cmd.Parameters.AddWithValue("@ToDate", formattedTo);
                cmd.Parameters.AddWithValue("@DeviceIDs", deviceIDs);
                await conn.OpenAsync();
                using var reader = await cmd.ExecuteReaderAsync();
                var dt = new DataTable();
                dt.Load(reader);
                resultList = DataTableToList(dt);
                return Ok(resultList);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "Error while fetching data.", ExceptionMessage = ex.Message });
            }
        }

        /// <summary>
        /// Returns golden batch data for all plants for a user.
        /// </summary>
        [HttpGet("GetGoldenBatchData_AllPlants")]
        public async Task<IActionResult> GetGoldenBatchData_AllPlants(string tenantId, string userId)
        {
            if (string.IsNullOrWhiteSpace(tenantId) || string.IsNullOrWhiteSpace(userId))
                return BadRequest(new { Message = "Missing required parameters." });
            try
            {
                if (!int.TryParse(tenantId, out int TenantID))
                    return BadRequest(new { Message = "Invalid TenantID" });
                if (!int.TryParse(userId, out int UserID))
                    return BadRequest(new { Message = "Invalid UserID" });
                const string query = "USP_GetDeviceLocationBuildingByUser";
                var resultList = new List<Dictionary<string, object>>();
                using var conn = new SqlConnection(ConnectionString);
                using var cmd = new SqlCommand(query, conn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@tenantID", TenantID);
                cmd.Parameters.AddWithValue("@userID", UserID);
                await conn.OpenAsync();
                using var reader = await cmd.ExecuteReaderAsync();
                var dt = new DataTable();
                dt.Load(reader);
                resultList = DataTableToList(dt);
                return Ok(resultList);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "Error while fetching data.", ExceptionMessage = ex.Message });
            }
        }

        /// <summary>
        /// Returns asset list for a user and tenant for golden batch.
        /// </summary>
        [HttpGet("GetGoldenBatchAssets")]
        public async Task<IActionResult> GetGoldenBatchAssets(string tenantId, string userId)
        {
            if (string.IsNullOrWhiteSpace(tenantId) || string.IsNullOrWhiteSpace(userId))
                return BadRequest(new { Message = "Missing required parameters." });
            try
            {
                var resultList = new List<Dictionary<string, object>>();
                using var conn = new SqlConnection(ConnectionString);
                using var cmd = new SqlCommand("USP_GetAssetListByUser_GoldenBatch", conn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@UserID", int.Parse(userId));
                cmd.Parameters.AddWithValue("@TenantID", int.Parse(tenantId));
                await conn.OpenAsync();
                using var reader = await cmd.ExecuteReaderAsync();
                var dt = new DataTable();
                dt.Load(reader);
                resultList = DataTableToList(dt);
                return Ok(resultList);
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