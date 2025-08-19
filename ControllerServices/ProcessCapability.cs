using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace SPCCoreMigration.ControllerServices
{
    public class ProcessCapability
    {
        private static readonly TimeZoneInfo INDIAN_ZONE = TimeZoneInfo.FindSystemTimeZoneById("India Standard Time");
        private readonly IConfiguration _configuration;

        public ProcessCapability(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task<string> loadProcessCapabilityDataAsync(
            DataTable dt, DataTable pcSpecificationsData, DataTable excludeDates, string tenantCode, int assetID, int paramID, int tenantID, int ID,
            string fromDate, string toDate, int datatypeID, double LSL, double USL,
            double Target, int isFilter, int subgroupSize, int assetParameterCategoryId, int is20DataPoints)
        {
            DateTime indianTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, INDIAN_ZONE);

            try
            {
                if (dt == null || dt.Rows.Count == 0)
                {
                    return "Error: Input data table is empty.";
                }

                DataTable specificationDetails = new DataTable();

                if (pcSpecificationsData != null && pcSpecificationsData.Rows.Count > 0)
                {
                    var data = pcSpecificationsData.AsEnumerable()
                        .Where(row => row.Field<int>("ProcessCapabilityID") == ID);

                    if (data.Any())
                    {
                        specificationDetails = data.CopyToDataTable();
                    }
                }

                DataTable filteredExcludeDates = new DataTable();

                if (excludeDates != null && excludeDates.Rows.Count > 0)
                {
                    var edata = excludeDates.AsEnumerable()
                        .Where(row =>
                            (row.Field<int?>("ProcessCapabilityID") == null || row.Field<int?>("ProcessCapabilityID") == ID) &&
                            (row.Field<int?>("AssetParameterCategoryID") == null || row.Field<int?>("AssetParameterCategoryID") == assetParameterCategoryId)
                        );

                    if (edata.Any())
                    {
                        filteredExcludeDates = edata.CopyToDataTable();
                    }
                }

                var filteredRows = dt.AsEnumerable().Where(row =>
                {
                    DateTime currentDate = row.Field<DateTime>("D");
                    return filteredExcludeDates.Rows.Count == 0 ||
                        !filteredExcludeDates.AsEnumerable().Any(filterRow =>
                            currentDate >= filterRow.Field<DateTime>("FromDate") &&
                            currentDate <= filterRow.Field<DateTime>("ToDate"));
                });

                DataTable resultDataTable = new DataTable();

                if (filteredRows != null && filteredRows.Any())
                {
                    resultDataTable = filteredRows.CopyToDataTable();
                }

                var body = new
                {
                    Data = resultDataTable,
                    LSL = LSL,
                    USL = USL,
                    Target = Target,
                    TenantCode = tenantCode,
                    AssetID = assetID,
                    paramID = paramID,
                    SubgroupSize = subgroupSize,
                    IsFilter = isFilter,
                    TenantID = tenantID,
                    DatatypeID = datatypeID,
                    FromDate = fromDate,
                    ToDate = toDate,
                    CreatedDate = indianTime,
                    ProcessCapabilityID = ID,
                    Is20DataPoints = is20DataPoints,
                    Specifications = specificationDetails,
                };

                using (var client = new HttpClient())
                {
                    // Use IConfiguration to get the URL from appsettings.json
                    var uri = _configuration["ProcessCapabilityUrl"];
                    if (string.IsNullOrWhiteSpace(uri))
                    {
                        return "Error: ProcessCapabilityUrl not configured.";
                    }

                    client.BaseAddress = new Uri(uri);
                    var jsonBody = JsonConvert.SerializeObject(body);
                    var content = new StringContent(jsonBody, Encoding.UTF8, "application/json");

                    try
                    {
                        var response = await client.PostAsync("processCapability_job", content);

                        if (response.IsSuccessStatusCode)
                        {
                            var jsonResponse = await response.Content.ReadAsStringAsync();
                            return "true";
                        }
                        else
                        {
                            return "Error";
                        }
                    }
                    catch (Exception ex)
                    {
                        return $"Error: {ex.Message}";
                    }
                }
            }
            catch (Exception ex)
            {
                return $"Error: {ex.Message}";
            }
        }
    }
}