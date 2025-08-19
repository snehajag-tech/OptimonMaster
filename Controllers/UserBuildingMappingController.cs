using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using System.Data;
using Microsoft.Extensions.Configuration;
using System.Net;
using SPCCoreMigration.Model;

namespace SPCCoreMigration.Controllers
{
    [Route("api/[controller]")]
    public class UserBuildingMappingController : Controller
    {
        private readonly IConfiguration _configuration;

        private string ConnectionString => _configuration.GetConnectionString("DefaultConnection") ?? throw new InvalidOperationException("Connection string 'DBConnStr' not found.");

        public UserBuildingMappingController(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        /// <summary>
        /// Gets user mappings for a building based on user ID, tenant code, and tenant ID
        /// </summary>
        /// <param name="userID">The ID of the user</param>
        /// <param name="tenantCode">The tenant code</param>
        /// <param name="TenantID">The tenant ID</param>
        /// <returns>List of user building mappings</returns>
        [HttpGet]
        [Route("GetUserBuildingMappings")]
        public IActionResult GetUserBuildingMappings(string userID, string tenantCode, string TenantID, string LocationId, string BuildingId)
        {
            try
            {
                // Create list to hold results
                List<UserPlantMapping> allMappingsData = new List<UserPlantMapping>();
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    // Create SQL command
                    using (SqlCommand command = new SqlCommand("dbo.sp_GetUserMappingsForBuilding", connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;

                        // Add parameters
                        command.Parameters.AddWithValue("@userID", Convert.ToInt32(userID));
                        command.Parameters.AddWithValue("@tenantCode", tenantCode == "True" ? 0 : 1);
                        command.Parameters.AddWithValue("@tenantID", Convert.ToInt32(TenantID));
                        command.Parameters.AddWithValue("@locationId", Convert.ToInt32(LocationId));
                        command.Parameters.AddWithValue("@buildingId", Convert.ToInt32(BuildingId));
                        connection.Open();

                        // Execute reader
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                UserPlantMapping mapping = new UserPlantMapping
                                {
                                    SSOUserPlantMappingID = reader["SSOUserPlantMappingID"] != DBNull.Value ? Convert.ToInt32(reader["SSOUserPlantMappingID"]) : 0,
                                    SSOUserID = reader["SSOUserID"] != DBNull.Value ? Convert.ToInt32(reader["SSOUserID"]) : 0,
                                    UserName = reader["UserName"] != DBNull.Value ? Convert.ToString(reader["UserName"]) : string.Empty,
                                    LocationID = reader["LocationID"] != DBNull.Value ? Convert.ToInt32(reader["LocationID"]) : 0,
                                    LocationName = reader["LocationName"] != DBNull.Value ? Convert.ToString(reader["LocationName"]) : string.Empty,
                                    BuildingID = reader["BuildingID"] != DBNull.Value ? Convert.ToInt32(reader["BuildingID"]) : 0,
                                    BuildingName = reader["BuildingName"] != DBNull.Value ? reader["BuildingName"].ToString() : string.Empty,
                                    TenantID = reader["TenantID"] != DBNull.Value ? Convert.ToInt32(reader["TenantID"]) : 0,
                                    StatusID = reader["StatusID"] != DBNull.Value ? Convert.ToInt32(reader["StatusID"]) : 0
                                };
                                allMappingsData.Add(mapping);
                            }
                        }
                    }
                }

                List<UserPlantMapping> mappings = new List<UserPlantMapping>();
                var groupedData = allMappingsData
                    .GroupBy(m => new { m.BuildingID, m.BuildingName, m.LocationID, m.LocationName })
                    .Select(g => new UserPlantMapping
                    {
                        BuildingID = g.Key.BuildingID,
                        BuildingName = g.Key.BuildingName,
                        LocationID = g.Key.LocationID,
                        LocationName = g.Key.LocationName,
                        // Other properties can be set from the first item in the group
                        TenantID = g.First().TenantID,
                        StatusID = g.First().StatusID,
                        // Set SelectedUserIDs to the list of SSOUserIDs in this group
                        SelectedUserIDs = g.Select(x => x.SSOUserID).Distinct().ToList(),
                        MappedUsers = g.Select(x => x.UserName).Distinct().ToList()
                    }).ToList();

                mappings.AddRange(groupedData);
                // Return success with data
                return Ok(mappings);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpPost("SaveUserBuildingMappings")]
        public IActionResult SaveUserBuildingMappings([FromBody] List<UserPlantMapping> mappings)
        {
            try
            {
                //var userMappings = JsonConvert.DeserializeObject<List<UserPlantMapping>>(mappings);

                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();
                    foreach (var mapping in mappings)
                    {
                        using (var command = new SqlCommand("sp_SaveUserBuildingMapping", connection))
                        {
                            command.CommandType = CommandType.StoredProcedure;
                            command.Parameters.AddWithValue("@SSOUserID", mapping.SSOUserID);
                            command.Parameters.AddWithValue("@LocationID", mapping.LocationID);
                            command.Parameters.AddWithValue("@BuildingID", mapping.BuildingID);
                            command.Parameters.AddWithValue("@TenantID", mapping.TenantID);
                            command.Parameters.AddWithValue("@StatusID", mapping.StatusID);
                            command.Parameters.AddWithValue("@CreatedBy", mapping.CreatedBy);
                            command.ExecuteNonQuery();
                        }
                    }
                }
                return Ok("User building mappings saved successfully.");
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

       
    }
}