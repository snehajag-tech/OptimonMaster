using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using SPCCoreMigration.Model;

namespace SPCCoreMigration.Controllers
{
    [Route("api/[controller]")]
    public class UserApprovalController : Controller
    {

        private readonly IConfiguration _configuration;

        private string ConnectionString => _configuration.GetConnectionString("DefaultConnection") ?? throw new InvalidOperationException("Connection string 'DBConnStr' not found.");

        public UserApprovalController(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        [HttpGet("approval-levels")]
        public IActionResult GetApprovalLevels()
        {
            try
            {
                List<ApprovalLevel> approvalLevels = new List<ApprovalLevel>();
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    using (SqlCommand command = new SqlCommand("dbo.usp_GetApprovalLevels", connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        connection.Open();
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ApprovalLevel level = new ApprovalLevel
                                {
                                    ApprovalLevelLookupID = reader["ApprovalLevelLookupID"] != DBNull.Value ? Convert.ToInt32(reader["ApprovalLevelLookupID"]) : 0,
                                    Name = reader["Name"] != DBNull.Value ? Convert.ToString(reader["Name"]) : string.Empty,
                                    Code = reader["Code"] != DBNull.Value ? Convert.ToString(reader["Code"]) : string.Empty,
                                    StatusID = reader["StatusID"] != DBNull.Value ? Convert.ToInt32(reader["StatusID"]) : 0
                                };
                                approvalLevels.Add(level);
                            }
                        }
                    }
                }
                return Ok(approvalLevels);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("users-by-building/{plantId}")]
        public IActionResult GetUsersByBuilding(int plantId)
        {
            try
            {
                List<UserPlantMapping> users = new List<UserPlantMapping>();
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    using (SqlCommand command = new SqlCommand("dbo.usp_GetUsersByBuilding", connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("@PlantID", plantId);
                        connection.Open();
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                UserPlantMapping user = new UserPlantMapping
                                {
                                    SSOUserPlantMappingID = reader["SSOUserPlantMappingID"] != DBNull.Value ? Convert.ToInt32(reader["SSOUserPlantMappingID"]) : 0,
                                    SSOUserID = reader["UserID"] != DBNull.Value ? Convert.ToInt32(reader["UserID"]) : 0,
                                    UserName = reader["UserName"] != DBNull.Value ? Convert.ToString(reader["UserName"]) : string.Empty,
                                    EmailId = reader["EmailId"] != DBNull.Value ? Convert.ToString(reader["EmailId"]) : string.Empty,
                                    BuildingID = reader["BuildingID"] != DBNull.Value ? Convert.ToInt32(reader["BuildingID"]) : 0
                                };
                                users.Add(user);
                            }
                        }
                    }
                }
                return Ok(users);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("current-mappings/{plantId}")]
        public IActionResult GetCurrentMappings(int plantId)
        {
            try
            {
                List<UserApprovalMapping> mappings = GetUserApprovalMappingsByBuilding(plantId);
                return Ok(mappings);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpPost("save-mappings")]
        public IActionResult SaveMappings([FromBody] UserLevelMappingsViewModel request)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    using (SqlCommand command = new SqlCommand("dbo.usp_SaveUserApprovalMappings", connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("@PlantID", request.PlantID);
                        command.Parameters.AddWithValue("@TenantID", request.TenantID);
                        command.Parameters.AddWithValue("@UserID", User?.Identity?.Name ?? "");
                        var jsonMappings = JsonConvert.SerializeObject(request.Mappings);
                        command.Parameters.AddWithValue("@MappingsJSON", jsonMappings);
                        connection.Open();
                        command.ExecuteNonQuery();
                    }
                }
                return Ok("User level mappings saved successfully.");
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        public List<UserApprovalMapping> GetUserApprovalMappingsByBuilding(int plantId, string connectionString = null)
        {
            try
            {
                List<UserApprovalMapping> mappings = new List<UserApprovalMapping>();
                using (SqlConnection connection = new SqlConnection(connectionString ?? ConnectionString))
                {
                    using (SqlCommand command = new SqlCommand("dbo.usp_GetUserApprovalMappingsByBuilding", connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("@PlantID", plantId);
                        connection.Open();
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                UserApprovalMapping mapping = new UserApprovalMapping
                                {
                                    UserApprovalLevelMappingID = reader["UserApprovalLevelMappingID"] != DBNull.Value ? Convert.ToInt32(reader["UserApprovalLevelMappingID"]) : 0,
                                    ApprovalLevelLookupID = reader["ApprovalLevelLookupID"] != DBNull.Value ? Convert.ToInt32(reader["ApprovalLevelLookupID"]) : 0,
                                    ApprovalLevelName = reader["ApprovalLevelName"] != DBNull.Value ? Convert.ToString(reader["ApprovalLevelName"]) : string.Empty,
                                    SSOUserPlantMappingID = reader["SSOUserPlantMappingID"] != DBNull.Value ? Convert.ToInt32(reader["SSOUserPlantMappingID"]) : 0,
                                    UserID = reader["UserID"] != DBNull.Value ? Convert.ToInt32(reader["UserID"]) : 0,
                                    UserName = reader["UserName"] != DBNull.Value ? Convert.ToString(reader["UserName"]) : string.Empty,
                                    Email = reader["EmailId"] != DBNull.Value ? Convert.ToString(reader["EmailId"]) : string.Empty,
                                    StatusID = reader["StatusID"] != DBNull.Value ? Convert.ToInt32(reader["StatusID"]) : 0
                                };
                                mappings.Add(mapping);
                            }
                        }
                    }
                }
                return mappings;
            }
            catch (Exception)
            {
                return new List<UserApprovalMapping>();
            }
        }
    }
}
