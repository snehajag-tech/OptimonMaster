using Azure;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using SPCCoreMigration.Models;
using System.Data;
using System.Net.Http;
using System.Text;

namespace SPCCoreMigration.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ProcessCapabilityController : Controller
    {
        private static TimeZoneInfo INDIAN_ZONE = TimeZoneInfo.FindSystemTimeZoneById("India Standard Time");

        private readonly IConfiguration _configuration;
        private readonly IHttpClientFactory _httpClientFactory;

        ControllerServices.ProcessCapability pc;

        private string ConnectionString => _configuration.GetConnectionString("DefaultConnection") ?? throw new InvalidOperationException("Connection string 'DBConnStr' not found.");

        public ProcessCapabilityController(IConfiguration configuration, IHttpClientFactory httpClientFactory)
        {
            _configuration = configuration;
            _httpClientFactory = httpClientFactory;
            pc = new ControllerServices.ProcessCapability(_configuration);
        }

        [HttpPost("LoadUploadedProcessCapabilityData")]
        public IActionResult LoadUploadedProcessCapabilityData([FromBody] PCParams pcs)
        {
            try
            {
                DataTable dts = JsonConvert.DeserializeObject<DataTable>(pcs.JSONData.ToString());
                DataTableConverter converter = new DataTableConverter();
                Dictionary<string, string> jsonOutput = converter.ConvertDataTableToColumnWiseJson(dts);

                foreach (var kvp in jsonOutput)
                {
                    var dt1 = JsonConvert.DeserializeObject<DataTable>(kvp.Value.ToString());
                    if (dt1.Rows.Count > 0)
                    {
                        var sortedRows = dt1.AsEnumerable()
                            .OrderBy(row => Convert.ToDateTime(row[0]))
                            .ToList();

                        DateTime fromDate = Convert.ToDateTime(sortedRows.First()[0]);
                        DateTime toDate = Convert.ToDateTime(sortedRows.Last()[0]);

                        string fromDt = fromDate.ToString("yyyy-MM-dd");
                        string toDt = toDate.ToString("yyyy-MM-dd");
                        string parameterName = kvp.Key;
                        int tenantId = Convert.ToInt32(pcs.TenantID);

                        DataSet pcDataSet = GetProcessCapabilityData(tenantId, parameterName, fromDt, toDt);

                        if (pcDataSet.Tables.Count > 0)
                        {
                            DataTable dt2 = pcDataSet.Tables[1];
                            DataTable dt3 = pcDataSet.Tables[0];

                            dt1.Columns.Add("D_DateTime", typeof(DateTime));
                            foreach (DataRow row in dt1.Rows)
                            {
                                row["D_DateTime"] = DateTime.Parse(row["D"].ToString());
                            }

                            var filteredRows = dt1.AsEnumerable().Where(row =>
                            {
                                DateTime dValue = row.Field<DateTime>("D_DateTime");
                                return !dt2.AsEnumerable().Any(range =>
                                {
                                    DateTime from = range.Field<DateTime>("FromDate");
                                    DateTime to = range.Field<DateTime>("ToDate");
                                    return dValue >= from && dValue <= to;
                                });
                            });

                            DataTable resultTable = dt1.Clone();
                            resultTable.Columns.Remove("D_DateTime");
                            foreach (var row in filteredRows)
                            {
                                resultTable.ImportRow(row);
                            }

                            foreach (DataRow row in dt3.Rows)
                            {
                                int processCapabilityID = Convert.ToInt32(row["ProcessCapabilityID"]);
                                int assetID = Convert.ToInt32(row["AssetID"]);
                                int assetParameterID = Convert.ToInt32(row["AssetParameterID"]);
                                double lcl = Convert.ToDouble(row["LCL"]);
                                double ucl = Convert.ToDouble(row["UCL"]);
                                int target = Convert.ToInt32(row["Target"]);
                                int datatypeID = Convert.ToInt32(row["DatatypeID"]);
                                int subgroupSize = Convert.ToInt32(row["SubgroupSize"]);
                                int isFilter = Convert.ToInt32(row["IsFilter"]);
                                string tenantCode = row["TenantCode"].ToString();

                                loadProcessCapabilityData(
                                    resultTable,
                                    tenantCode,
                                    assetID,
                                    assetParameterID,
                                    tenantId,
                                    processCapabilityID,
                                    fromDt,
                                    toDt,
                                    datatypeID,
                                    lcl,
                                    ucl,
                                    target, isFilter, subgroupSize
                                );
                            }
                        }
                    }
                }

                return Ok("Success");
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }
        //Added pc.SubgroupSize,pc.IsFilter
        private DataSet GetProcessCapabilityData(int tenantId, string parameterName, string fromDate, string toDate)
        {
            DataSet ds = new DataSet();
            try
            {
                string query = "USP_GetProcessCapabilityData";
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@ParameterName", parameterName);
                    cmd.Parameters.AddWithValue("@TenantId", tenantId);
                    cmd.Parameters.AddWithValue("@FromDate", fromDate);
                    cmd.Parameters.AddWithValue("@ToDate", toDate);

                    conn.Open();
                    SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                    adapter.Fill(ds);
                }
            }
            catch
            {
                return new DataSet();
            }
            return ds;
        }

        private void loadProcessCapabilityData(DataTable dt, string tenantCode, int assetID, int paramID, int tenantID,
            int ID, string fromDate, string toDate, int datatypeID, double LSL, double USL, int Target, int isFilter, int subgroupSize)
        {
            DateTime indianTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, INDIAN_ZONE);
            if (dt.Rows.Count > 0)
            {
                var body = new
                {
                    Data = dt,
                    LSL = LSL,
                    USL = USL,
                    Target = Target,
                    TenantCode = tenantCode,
                    AssetID = assetID,
                    paramID = paramID,
                    SubgroupSize = subgroupSize,
                    IsFilter = isFilter
                };
                var client = new HttpClient();
                var uri = _configuration["ProcessCapabilityUrl"];
                client.BaseAddress = new Uri(uri);
                var jsonBody = JsonConvert.SerializeObject(body);
                var content = new StringContent(jsonBody, Encoding.UTF8, "application/json");
                var response = client.PostAsync("processCapability_job", content).Result;

                if (response.IsSuccessStatusCode)
                {
                    var jsonResponse = response.Content.ReadAsStringAsync().Result;
                    var structuredResponse = JsonConvert.DeserializeObject<Response>(jsonResponse);

                    string GetValue(string parameterName)
                    {
                        var value = structuredResponse.Metrics.FirstOrDefault(p => p.Parameter == parameterName)?.Value;
                        if (value == null || value.ToString() == "NaN")
                        {
                            return "0.0";
                        }
                        return value;
                    }
                    string folderPath = null;
                    foreach (var metric in structuredResponse.Metrics)
                    {
                        if (metric.Parameter == "FolderPath")
                        {
                            folderPath = metric.Value.ToString();
                            break;
                        }
                    }

                    var sampleMeanCp = GetValue("SampleMean(Cp)");
                    var sampleNCp = GetValue("SampleN(Cp)");
                    var sampleMeanPp = GetValue("SampleMean(Pp)");
                    var sampleNPp = Convert.ToInt32(GetValue("SampleN(Pp)"));
                    var stDevOverall = GetValue("StDev(Overall)");
                    var stDevBetween = GetValue("StDev(Between)");
                    var stDevWithin = GetValue("StDev(Within)");
                    var stDevBW = GetValue("StDev(B/W)");
                    var pp = GetValue("Pp");
                    var ppk = GetValue("Ppk");
                    var ppl = GetValue("Ppl");
                    var ppu = GetValue("Ppu");
                    var cpm = GetValue("Cpm");
                    var cp = GetValue("Cp");
                    var cpk = GetValue("Cpk");
                    var cpl = GetValue("Cpl");
                    var cpu = GetValue("Cpu");
                    var ppmTotalObserved = GetValue("PPMTotal(Observed)");
                    var ppmTotalExpectedOverall = GetValue("PPMTotal(ExpectedOverall)");
                    var ppmTotalExpectedBW = GetValue("PPMTotal(ExpectedB/W)");
                    var outliersCount = GetValue("Outliers_count");
                    var outliersAvgValue = GetValue("Outliers_avg_value");
                    var outliersAvgResidual = GetValue("Outliers_avg_residual");
                    var numSubgroups = GetValue("NumSubgroups");
                    var meanWithinStd = GetValue("MeanWithinStd");
                    var ppmLSLObserved = GetValue("PPM<LSL(Observed)");
                    var ppmUSLObserved = GetValue("PPM>USL(Observed)");
                    var ppmLSLExpectedOverall = GetValue("PPM<LSL(ExpectedOverall)");
                    var ppmUSLExpectedOverall = GetValue("PPM>USL(ExpectedOverall)");
                    var ppmLSLExpectedBW = GetValue("PPM<LSL(ExpectedB/W)");
                    var ppmUSLExpectedBW = GetValue("PPM>USL(ExpectedB/W)");

                    using (var sqlConnection = new SqlConnection(ConnectionString))
                    {
                        sqlConnection.Open();
                        var checkExistenceCommand = @"select COUNT(*) from ProcessCapabilityReadings WHERE ProcessCapabilityID = @ProcessCapabilityID";
                        using (var checkCmd = new SqlCommand(checkExistenceCommand, sqlConnection))
                        {
                            checkCmd.Parameters.AddWithValue("@ProcessCapabilityID", ID);
                            var exists = Convert.ToInt32(checkCmd.ExecuteScalar()) > 0;

                            if (exists)
                            {
                                var updateCommand = @"UPDATE ProcessCapabilityReadings
                            SET SampleMean_Cp = @SampleMean_Cp,
                                SampleN_Cp = @SampleN_Cp,
                                SampleMean_Pp = @SampleMean_Pp,
                                SampleN_Pp = @SampleN_Pp,
                                StDev_Overall = @StDev_Overall,
                                StDev_Between = @StDev_Between,
                                StDev_Within = @StDev_Within,
                                StDev_B_W = @StDev_B_W,
                                Pp = @Pp,
                                Ppk = @Ppk,
                                Ppl = @Ppl,
                                Ppu = @Ppu,
                                Cpm = @Cpm,
                                Cp = @Cp,
                                Cpk = @Cpk,
                                Cpl = @Cpl,
                                Cpu = @Cpu,
                                ImgSRC = @ImgSRC,
                                TenantID = @TenantID,
                                CreatedDate = @CreatedDate,
                                PPMTotal_Observed = @PPMTotal_Observed,
                                PPMTotal_ExpectedOverall = @PPMTotal_ExpectedOverall,
                                PPMTotal_ExpectedB_W = @PPMTotal_ExpectedB_W,
                                Outliers_count = @Outliers_count,
                                Outliers_avg_value = @Outliers_avg_value,
                                Outliers_avg_residual = @Outliers_avg_residual,
                                NumSubgroups = @NumSubgroups,
                                MeanWithinStd = @MeanWithinStd,
                                PPM_LSL_Observed = @PPM_LSL_Observed,
                                PPM_USL_Observed = @PPM_USL_Observed,
                                PPM_LSL_ExpectedOverall = @PPM_LSL_ExpectedOverall,
                                PPM_USL_ExpectedOverall = @PPM_USL_ExpectedOverall,
                                PPM_LSL_ExpectedB_W = @PPM_LSL_ExpectedB_W,
                                PPM_USL_ExpectedB_W = @PPM_USL_ExpectedB_W,
                                FromDate = @FromDate,
                                ToDate = @ToDate,
                                DatatypeID = @DatatypeID,
                                LCL=@LCL, UCL=@UCL,Target= @Target
                            WHERE ProcessCapabilityID = @ProcessCapabilityID";
                                using (var updateCmd = new SqlCommand(updateCommand, sqlConnection))
                                {
                                    updateCmd.Parameters.AddWithValue("@ProcessCapabilityID", ID);
                                    updateCmd.Parameters.AddWithValue("@SampleMean_Cp", sampleMeanCp);
                                    updateCmd.Parameters.AddWithValue("@SampleN_Cp", sampleNCp);
                                    updateCmd.Parameters.AddWithValue("@SampleMean_Pp", sampleMeanPp);
                                    updateCmd.Parameters.AddWithValue("@SampleN_Pp", sampleNPp);
                                    updateCmd.Parameters.AddWithValue("@StDev_Overall", stDevOverall);
                                    updateCmd.Parameters.AddWithValue("@StDev_Between", stDevBetween);
                                    updateCmd.Parameters.AddWithValue("@StDev_Within", stDevWithin);
                                    updateCmd.Parameters.AddWithValue("@StDev_B_W", stDevBW);
                                    updateCmd.Parameters.AddWithValue("@Pp", pp);
                                    updateCmd.Parameters.AddWithValue("@Ppk", ppk);
                                    updateCmd.Parameters.AddWithValue("@Ppl", ppl);
                                    updateCmd.Parameters.AddWithValue("@Ppu", ppu);
                                    updateCmd.Parameters.AddWithValue("@Cpm", cpm);
                                    updateCmd.Parameters.AddWithValue("@Cp", cp);
                                    updateCmd.Parameters.AddWithValue("@Cpk", cpk);
                                    updateCmd.Parameters.AddWithValue("@Cpl", cpl);
                                    updateCmd.Parameters.AddWithValue("@Cpu", cpu);
                                    updateCmd.Parameters.AddWithValue("@ImgSRC", folderPath);
                                    updateCmd.Parameters.AddWithValue("@TenantID", tenantID);
                                    updateCmd.Parameters.AddWithValue("@CreatedDate", indianTime);
                                    updateCmd.Parameters.AddWithValue("@PPMTotal_Observed", ppmTotalObserved);
                                    updateCmd.Parameters.AddWithValue("@PPMTotal_ExpectedOverall", ppmTotalExpectedOverall);
                                    updateCmd.Parameters.AddWithValue("@PPMTotal_ExpectedB_W", ppmTotalExpectedBW);
                                    updateCmd.Parameters.AddWithValue("@Outliers_count", outliersCount);
                                    updateCmd.Parameters.AddWithValue("@Outliers_avg_value", outliersAvgValue);
                                    updateCmd.Parameters.AddWithValue("@Outliers_avg_residual", outliersAvgResidual);
                                    updateCmd.Parameters.AddWithValue("@NumSubgroups", numSubgroups);
                                    updateCmd.Parameters.AddWithValue("@MeanWithinStd", meanWithinStd);
                                    updateCmd.Parameters.AddWithValue("@PPM_LSL_Observed", ppmLSLObserved);
                                    updateCmd.Parameters.AddWithValue("@PPM_USL_Observed", ppmUSLObserved);
                                    updateCmd.Parameters.AddWithValue("@PPM_LSL_ExpectedOverall", ppmLSLExpectedOverall);
                                    updateCmd.Parameters.AddWithValue("@PPM_USL_ExpectedOverall", ppmUSLExpectedOverall);
                                    updateCmd.Parameters.AddWithValue("@PPM_LSL_ExpectedB_W", ppmLSLExpectedBW);
                                    updateCmd.Parameters.AddWithValue("@PPM_USL_ExpectedB_W", ppmUSLExpectedBW);
                                    updateCmd.Parameters.AddWithValue("@FromDate", fromDate);
                                    updateCmd.Parameters.AddWithValue("@ToDate", toDate);
                                    updateCmd.Parameters.AddWithValue("@DatatypeID", datatypeID);
                                    updateCmd.Parameters.AddWithValue("@LCL", LSL);
                                    updateCmd.Parameters.AddWithValue("@UCL", USL);
                                    updateCmd.Parameters.AddWithValue("@Target", Target);

                                    updateCmd.ExecuteNonQuery();
                                }
                            }
                            else
                            {
                                var insertCommand = @"INSERT INTO ProcessCapabilityReadings
                            (ProcessCapabilityID, SampleMean_Cp, SampleN_Cp, SampleMean_Pp, SampleN_Pp, StDev_Overall,
                             StDev_Between, StDev_Within, StDev_B_W, Pp, Ppk, Ppl, Ppu, Cpm, Cp, Cpk, Cpl, Cpu, ImgSRC, TenantID,
                             CreatedDate, PPMTotal_Observed, PPMTotal_ExpectedOverall, PPMTotal_ExpectedB_W, Outliers_count, Outliers_avg_value,
                             Outliers_avg_residual, NumSubgroups, MeanWithinStd, PPM_LSL_Observed, PPM_USL_Observed, PPM_LSL_ExpectedOverall,
                             PPM_USL_ExpectedOverall, PPM_LSL_ExpectedB_W, PPM_USL_ExpectedB_W, FromDate, ToDate, DatatypeID,LCL,UCL,Target)
                            VALUES (@ProcessCapabilityID, @SampleMean_Cp, @SampleN_Cp, @SampleMean_Pp, @SampleN_Pp, @StDev_Overall,
                                    @StDev_Between, @StDev_Within, @StDev_B_W, @Pp, @Ppk, @Ppl, @Ppu, @Cpm, @Cp, @Cpk, @Cpl, @Cpu, @ImgSRC,
                                    @TenantID, @CreatedDate, @PPMTotal_Observed, @PPMTotal_ExpectedOverall, @PPMTotal_ExpectedB_W, @Outliers_count,
                                    @Outliers_avg_value, @Outliers_avg_residual, @NumSubgroups, @MeanWithinStd, @PPM_LSL_Observed, @PPM_USL_Observed,
                                    @PPM_LSL_ExpectedOverall, @PPM_USL_ExpectedOverall, @PPM_LSL_ExpectedB_W, @PPM_USL_ExpectedB_W, @FromDate, @ToDate,
                                    @DatatypeID,@LCL,@UCL,@Target)";
                                using (var insertCmd = new SqlCommand(insertCommand, sqlConnection))
                                {
                                    insertCmd.Parameters.AddWithValue("@ProcessCapabilityID", ID);
                                    insertCmd.Parameters.AddWithValue("@SampleMean_Cp", sampleMeanCp);
                                    insertCmd.Parameters.AddWithValue("@SampleN_Cp", sampleNCp);
                                    insertCmd.Parameters.AddWithValue("@SampleMean_Pp", sampleMeanPp);
                                    insertCmd.Parameters.AddWithValue("@SampleN_Pp", sampleNPp);
                                    insertCmd.Parameters.AddWithValue("@StDev_Overall", stDevOverall);
                                    insertCmd.Parameters.AddWithValue("@StDev_Between", stDevBetween);
                                    insertCmd.Parameters.AddWithValue("@StDev_Within", stDevWithin);
                                    insertCmd.Parameters.AddWithValue("@StDev_B_W", stDevBW);
                                    insertCmd.Parameters.AddWithValue("@Pp", pp);
                                    insertCmd.Parameters.AddWithValue("@Ppk", ppk);
                                    insertCmd.Parameters.AddWithValue("@Ppl", ppl);
                                    insertCmd.Parameters.AddWithValue("@Ppu", ppu);
                                    insertCmd.Parameters.AddWithValue("@Cpm", cpm);
                                    insertCmd.Parameters.AddWithValue("@Cp", cp);
                                    insertCmd.Parameters.AddWithValue("@Cpk", cpk);
                                    insertCmd.Parameters.AddWithValue("@Cpl", cpl);
                                    insertCmd.Parameters.AddWithValue("@Cpu", cpu);
                                    insertCmd.Parameters.AddWithValue("@ImgSRC", folderPath);
                                    insertCmd.Parameters.AddWithValue("@TenantID", tenantID);
                                    insertCmd.Parameters.AddWithValue("@CreatedDate", indianTime);
                                    insertCmd.Parameters.AddWithValue("@PPMTotal_Observed", ppmTotalObserved);
                                    insertCmd.Parameters.AddWithValue("@PPMTotal_ExpectedOverall", ppmTotalExpectedOverall);
                                    insertCmd.Parameters.AddWithValue("@PPMTotal_ExpectedB_W", ppmTotalExpectedBW);
                                    insertCmd.Parameters.AddWithValue("@Outliers_count", outliersCount);
                                    insertCmd.Parameters.AddWithValue("@Outliers_avg_value", outliersAvgValue);
                                    insertCmd.Parameters.AddWithValue("@Outliers_avg_residual", outliersAvgResidual);
                                    insertCmd.Parameters.AddWithValue("@NumSubgroups", numSubgroups);
                                    insertCmd.Parameters.AddWithValue("@MeanWithinStd", meanWithinStd);
                                    insertCmd.Parameters.AddWithValue("@PPM_LSL_Observed", ppmLSLObserved);
                                    insertCmd.Parameters.AddWithValue("@PPM_USL_Observed", ppmUSLObserved);
                                    insertCmd.Parameters.AddWithValue("@PPM_LSL_ExpectedOverall", ppmLSLExpectedOverall);
                                    insertCmd.Parameters.AddWithValue("@PPM_USL_ExpectedOverall", ppmUSLExpectedOverall);
                                    insertCmd.Parameters.AddWithValue("@PPM_LSL_ExpectedB_W", ppmLSLExpectedBW);
                                    insertCmd.Parameters.AddWithValue("@PPM_USL_ExpectedB_W", ppmUSLExpectedBW);
                                    insertCmd.Parameters.AddWithValue("@FromDate", fromDate);
                                    insertCmd.Parameters.AddWithValue("@ToDate", toDate);
                                    insertCmd.Parameters.AddWithValue("@DatatypeID", datatypeID);
                                    insertCmd.Parameters.AddWithValue("@LCL", LSL);
                                    insertCmd.Parameters.AddWithValue("@UCL", USL);
                                    insertCmd.Parameters.AddWithValue("@Target", Target);

                                    insertCmd.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                }
            }
        }

        [HttpPost("GetProcessCapabilityData")]
        public IActionResult GetProcessCapabilityData(int tenantId, int assetId, string paramID, string fromDate, string toDate)
        {
            try
            {
                DateTime indianTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, INDIAN_ZONE);
                var data = getAssetParamData(tenantId, assetId, paramID, fromDate, toDate);
                if (data.Tables.Count < 4)
                {
                    return BadRequest("Insufficient data returned.");
                }

                DataTable dt0 = data.Tables[0];
                DataTable dt1 = data.Tables[1];
                DataTable dt2 = data.Tables[2];
                DataTable dt3 = data.Tables[3];

                dt0.Columns.Add("D_DateTime", typeof(DateTime));
                foreach (DataRow row in dt0.Rows)
                {
                    row["D_DateTime"] = DateTime.Parse(row["D"].ToString());
                }

                var filteredRows = dt0.AsEnumerable().Where(row =>
                {
                    DateTime dValue = row.Field<DateTime>("D_DateTime");
                    return !dt2.AsEnumerable().Any(range =>
                    {
                        DateTime from = range.Field<DateTime>("FromDate");
                        DateTime to = range.Field<DateTime>("ToDate");
                        return dValue >= from && dValue <= to;
                    });
                });

                DataTable resultTable = dt0.Clone();
                resultTable.Columns.Remove("D_DateTime");
                foreach (var row in filteredRows)
                {
                    resultTable.ImportRow(row);
                }

                int totalRows = resultTable.Rows.Count;
                int excludedRowsCount = dt0.Rows.Count - totalRows;

                int is20DataPoints = 0;
                if (totalRows < 20 && dt1.Columns.Contains("is20Datapointsenable") && Convert.ToBoolean(dt1.Rows[0]["is20Datapointsenable"]))
                {
                    is20DataPoints = 1;
                    resultTable.Clear();
                    DataTable fallbackResult = GetParameterreadings_Aggration(assetId.ToString(), paramID, tenantId.ToString());
                    foreach (DataRow row in fallbackResult.Rows)
                    {
                        if (!row.IsNull("LogDateTime") && !row.IsNull("Value") && !string.IsNullOrWhiteSpace(row["Value"].ToString()))
                        {
                            DataRow newRow = resultTable.NewRow();
                            newRow["D"] = Convert.ToDateTime(row["LogDateTime"]);
                            newRow["V"] = row["Value"].ToString();
                            resultTable.Rows.Add(newRow);
                        }
                    }
                    totalRows = resultTable.Rows.Count;
                    excludedRowsCount = 0;
                }

                var body = new
                {
                    Data = resultTable,
                    LSL = float.Parse(data.Tables[1].Rows[0]["LCL"].ToString()),
                    USL = float.Parse(data.Tables[1].Rows[0]["UCL"].ToString()),
                    Target = float.Parse(data.Tables[1].Rows[0]["Target"].ToString()),
                    SubgroupSize = int.Parse(data.Tables[1].Rows[0]["SubgroupSize"].ToString()),
                    IsFilter = int.Parse(data.Tables[1].Rows[0]["IsFilter"].ToString()),
                    AssetID = int.Parse(data.Tables[1].Rows[0]["AssetID"].ToString()),
                    paramID = int.Parse(data.Tables[1].Rows[0]["AssetParameterID"].ToString()),
                    TenantCode = data.Tables[1].Rows[0]["TenantCode"],
                    TenantID = data.Tables[1].Rows[0]["TenantID"],
                    ProcessCapabilityID = data.Tables[1].Rows[0]["ProcessCapabilityID"],
                    DatatypeID = 1,
                    FromDate = fromDate,
                    ToDate = toDate,
                    CreatedDate = indianTime,
                    Specifications = dt3,
                    is20DataPoints = is20DataPoints
                };

                using (var client = new HttpClient())
                {
                    var uri = _configuration["ProcessCapabilityUrl"];
                    client.BaseAddress = new Uri(uri);
                    var jsonBody = JsonConvert.SerializeObject(body);
                    var content = new StringContent(jsonBody, Encoding.UTF8, "application/json");
                    var response = client.PostAsync("processCapability_live", content).Result;
                    if (response.IsSuccessStatusCode)
                    {
                        var jsonResponse = response.Content.ReadAsStringAsync().Result;
                        var Response = new
                        {
                            ApiResponse = JsonConvert.DeserializeObject(jsonResponse),
                            Data = resultTable,
                            ExcludedRowsCount = excludedRowsCount,
                            ProcessCapabilityID = int.Parse(data.Tables[1].Rows[0]["ProcessCapabilityID"].ToString())
                        };
                        return Ok(Response);
                    }
                    else
                    {
                        return StatusCode((int)response.StatusCode, "Failed to process the request.");
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        private DataSet getAssetParamData(int tenantId, int assetId, string paramID, string fromDate, string toDate)
        {
            var ds = new DataSet();
            using (var conn = new SqlConnection(ConnectionString))
            using (var cmd = new SqlCommand("[dbo].[USP_GetAssetData_ProcessCapability]", conn))
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@deviceid", assetId);
                cmd.Parameters.AddWithValue("@fromdt", fromDate);
                cmd.Parameters.AddWithValue("@todt", toDate);
                cmd.Parameters.AddWithValue("@tenantId", tenantId);
                cmd.Parameters.AddWithValue("@AssetParameterId", paramID);

                var da1 = new SqlDataAdapter { SelectCommand = cmd };
                conn.Open();
                da1.Fill(ds);
            }
            return ds;
        }

        [HttpGet("GetProcessCapabilityData")]
        public IActionResult GetProcessCapabilityData(int AssetID, int TenantID)
        {
            try
            {
                using (var conn = new SqlConnection(ConnectionString))
                using (var cmd = new SqlCommand(@"
            SELECT 
                PCC.ProcessCapabilityID,
                ParameterName,
                PCC.LCL,
                PCC.UCL,
                PCC.Target,
                Type,
                PCC.StatusId,
                PCC.IsJobRun,
                PCC.JobInterval,
                PCC.ProcessName,
                PCC.ProcessID,
                PCC.JobInterval,
                PCC.SubgroupSize,
                PCC.IsFilter,
                PCC.CreatedBy,
                PCC.Comment,
                PCC.CreatedDate,
                PCC.ModifiedBy,
                PCC.ModifiedDate
            FROM ProcessCapabilityConfiguration PCC
            JOIN AssetParameters AP ON PCC.AssetParameterId = AP.AssetParameterId
            JOIN ProcessCapabilityDataTypeLookup PCDT ON PCC.DataTypeID = PCDT.DataTypeID
            WHERE AP.AssetID = @AssetID 
              AND AP.TenantID = @TenantID 
              AND PCC.StatusId IN (1, 2)
            ORDER BY PCC.ProcessCapabilityID DESC;", conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddWithValue("@AssetID", AssetID);
                    cmd.Parameters.AddWithValue("@TenantID", TenantID);

                    var dt = new DataTable();
                    using (var da = new SqlDataAdapter(cmd))
                    {
                        conn.Open();
                        da.Fill(dt);
                    }
                    return Ok(dt);
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("GetProcessCapabilityExcludeData")]
        public IActionResult GetProcessCapabilityExcludeData(int AssetID, int TenantID)
        {
            try
            {
                using (var conn = new SqlConnection(ConnectionString))
                using (var cmd = new SqlCommand(@"
            SELECT ProcessCapabiltyExcludedDatesID,FromDate, ToDate, Reason, CreatedBy, CreatedDate 
            FROM ProcessCapabiltyExcludedDates 
            WHERE TenantID = @TenantID AND AssetID = @AssetID AND ProcessCapabilityID IS NULL AND StatusId = 1 
            ORDER BY 1 DESC;", conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddWithValue("@AssetID", AssetID);
                    cmd.Parameters.AddWithValue("@TenantID", TenantID);

                    var dt = new DataTable();
                    using (var da = new SqlDataAdapter(cmd))
                    {
                        conn.Open();
                        da.Fill(dt);
                    }
                    return Ok(dt);
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("GetParameterData")]
        public IActionResult GetParameterData(int AssetID, int TenantID)
        {
            try
            {
                var parameterData = new List<KeyValuePair<int, string>>();
                using (var conn = new SqlConnection(ConnectionString))
                using (var cmd = new SqlCommand(@"
            SELECT AP.AssetParameterID, AP.ParameterName 
            FROM AssetParameters AP 
            WHERE AP.AssetId = @AssetID 
              AND AP.TenantID = @TenantID
              AND AP.StatusID = 1 
              AND AP.ParameterName NOT IN (
                  SELECT AP2.ParameterName 
                  FROM AssetParameters AP2 
                  JOIN ProcessCapabilityConfiguration PCC 
                  ON AP2.AssetParameterID = PCC.AssetParameterID where PCC.statusId in (1,2)
              );", conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddWithValue("@AssetID", AssetID);
                    cmd.Parameters.AddWithValue("@TenantID", TenantID);

                    var dt = new DataSet();
                    using (var da = new SqlDataAdapter(cmd))
                    {
                        conn.Open();
                        da.Fill(dt);
                    }

                    if (dt.Tables.Count > 0 && dt.Tables[0].Rows.Count > 0)
                    {
                        foreach (DataRow dr in dt.Tables[0].Rows)
                        {
                            if (dr["AssetParameterID"] != DBNull.Value && dr["ParameterName"] != DBNull.Value)
                            {
                                if (int.TryParse(dr["AssetParameterID"].ToString(), out int parameterId))
                                {
                                    string parameterName = dr["ParameterName"].ToString();
                                    parameterData.Add(new KeyValuePair<int, string>(parameterId, parameterName));
                                }
                            }
                        }
                    }
                }
                return Ok(parameterData);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("GetDataType")]
        public IActionResult GetDataType()
        {
            try
            {
                using (var con = new SqlConnection(ConnectionString))
                {
                    con.Open();
                    var dataTypeList = new List<object>();
                    string query = @"
                SELECT 
                    DataTypeID,
                    Type 
                FROM ProcessCapabilityDataTypeLookup
                WHERE StatusID IN (1);";
                    using (var cmd = new SqlCommand(query, con))
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            dataTypeList.Add(new
                            {
                                DataTypeID = reader["DataTypeID"],
                                Type = reader["Type"]
                            });
                        }
                    }
                    var response = new
                    {
                        success = true,
                        data = dataTypeList
                    };
                    return Ok(response);
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }





        [HttpPost("AddProcessCapabilityData")]
        public IActionResult AddProcessCapabilityData(List<ProcessCapabilityConfigurationViewModel> model, string userId)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    // Set session context for auditing
                    var sessionContextQuery = "EXEC sp_set_session_context @key = N'UserId', @value = @userId";
                    using (var command = new SqlCommand(sessionContextQuery, connection))
                    {
                        command.Parameters.AddWithValue("@userId", userId);
                        command.ExecuteNonQuery();
                    }

                    foreach (var item in model)
                    {
                        string insertQuery = item.IsJobRun == 1
                            ? @"INSERT INTO ProcessCapabilityConfiguration 
                        (AssetID, AssetParameterID, LCL, UCL, Target, DataTypeID, TenantID, TimeInterval, ProcessID, ProcessName, IsJobRun, JobInterval, SubgroupSize, IsFilter, CreatedBy, CreatedDate, StatusID, Comment)
                        VALUES 
                        (@AssetID, @AssetParameterID, @LCL, @UCL, @Target, @DataTypeID, @TenantID, @TimeInterval, @ProcessID, @ProcessName, @IsJobRun, @JobInterval, @SubgroupSize, @IsFilter, @CreatedBy, @CreatedDate, @StatusID, @Comment)"
                            : @"INSERT INTO ProcessCapabilityConfiguration 
                        (AssetID, AssetParameterID, LCL, UCL, Target, DataTypeID, TenantID, TimeInterval, ProcessID, ProcessName, IsJobRun, SubgroupSize, IsFilter, CreatedBy, CreatedDate, StatusID, Comment)
                        VALUES 
                        (@AssetID, @AssetParameterID, @LCL, @UCL, @Target, @DataTypeID, @TenantID, @TimeInterval, @ProcessID, @ProcessName, @IsJobRun, @SubgroupSize, @IsFilter, @CreatedBy, @CreatedDate, @StatusID, @Comment)";

                        int processCapabilityID = 0;
                        using (var cmd = new SqlCommand(insertQuery, connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.Parameters.AddWithValue("@AssetID", item.AssetID);
                            cmd.Parameters.AddWithValue("@AssetParameterID", item.AssetParameterID);
                            cmd.Parameters.AddWithValue("@LCL", item.LCL);
                            cmd.Parameters.AddWithValue("@UCL", item.UCL);
                            cmd.Parameters.AddWithValue("@Target", item.Target);
                            cmd.Parameters.AddWithValue("@DataTypeID", item.DatatypeID);
                            cmd.Parameters.AddWithValue("@TenantID", item.TenantID);
                            cmd.Parameters.AddWithValue("@TimeInterval", item.TimeInterval);
                            cmd.Parameters.AddWithValue("@ProcessID", item.ProcessID);
                            cmd.Parameters.AddWithValue("@ProcessName", item.ProcessName);
                            cmd.Parameters.AddWithValue("@IsJobRun", item.IsJobRun);
                            cmd.Parameters.AddWithValue("@SubgroupSize", item.SubgroupSize);
                            cmd.Parameters.AddWithValue("@IsFilter", item.IsFilter);
                            cmd.Parameters.AddWithValue("@CreatedBy", item.CreatedBy);
                            cmd.Parameters.AddWithValue("@CreatedDate", item.CreatedDate);
                            cmd.Parameters.AddWithValue("@StatusID", item.StatusID);
                            cmd.Parameters.AddWithValue("@Comment", string.IsNullOrEmpty(item.Comment) ? (object)DBNull.Value : item.Comment);

                            if (item.IsJobRun == 1)
                            {
                                cmd.Parameters.AddWithValue("@JobInterval", item.JobInterval);
                            }

                            cmd.ExecuteNonQuery();
                        }

                        string selectQuery = "SELECT TOP 1 ProcessCapabilityID FROM ProcessCapabilityConfiguration " +
                                             "WHERE AssetID = @AssetID AND AssetParameterID = @AssetParameterID " +
                                             "ORDER BY CreatedDate DESC";
                        using (var cmd = new SqlCommand(selectQuery, connection))
                        {
                            cmd.Parameters.AddWithValue("@AssetID", item.AssetID);
                            cmd.Parameters.AddWithValue("@AssetParameterID", item.AssetParameterID);

                            var result = cmd.ExecuteScalar();
                            if (result != null)
                            {
                                processCapabilityID = Convert.ToInt32(result);
                            }
                        }

                        if (processCapabilityID > 0)
                        {
                            InsertProcessCapabilitySpecifications(new List<ProcessCapabilitySpecifications>
                    {
                        new ProcessCapabilitySpecifications
                        {
                            ProcessCapabilityID = processCapabilityID,
                            LCL = item.LCL,
                            UCL = item.UCL,
                            Target = item.Target,
                            FromDate = item.FromDate,
                            ToDate = item.ToDate,
                            TenantID = item.TenantID,
                            StatusID = item.StatusID,
                            CreatedBy = item.CreatedBy,
                            CreatedDate = item.CreatedDate
                        }
                    });
                        }
                    }

                    return Ok("Process capability data added successfully.");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"An error occurred: {ex.Message}");
            }
        }

        public IActionResult InsertProcessCapabilitySpecifications(List<ProcessCapabilitySpecifications> model)
        {
            string insertSpecificationsQuery = "INSERT INTO ProcessCapabilitySpecifications " +
                                               "(ProcessCapabilityID, LCL, UCL, Target, FromDate, ToDate, TenantID, StatusID, CreatedBy, CreatedDate) " +
                                               "VALUES (@ProcessCapabilityID, @LCL, @UCL, @Target, @FromDate,@ToDate,@TenantID, @StatusID, @CreatedBy, @CreatedDate)";

            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    foreach (var item in model)
                    {
                        using (var cmd = new SqlCommand(insertSpecificationsQuery, connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.Parameters.AddWithValue("@ProcessCapabilityID", item.ProcessCapabilityID);
                            cmd.Parameters.AddWithValue("@LCL", item.LCL);
                            cmd.Parameters.AddWithValue("@UCL", item.UCL);
                            cmd.Parameters.AddWithValue("@Target", item.Target);
                            cmd.Parameters.AddWithValue("@FromDate", item.FromDate);
                            cmd.Parameters.AddWithValue("@ToDate", item.ToDate);
                            cmd.Parameters.AddWithValue("@TenantID", item.TenantID);
                            cmd.Parameters.AddWithValue("@StatusID", item.StatusID);
                            cmd.Parameters.AddWithValue("@CreatedBy", item.CreatedBy);
                            cmd.Parameters.AddWithValue("@CreatedDate", item.CreatedDate);

                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                return Ok("Process capability specifications inserted successfully.");
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }

        [HttpGet("GetAuditLogData")]
        public async Task<IActionResult> GetAuditLogData(int TenantID, int AssetID)
        {
            try
            {
                if (TenantID == 0 || AssetID <= 0)
                {
                    return BadRequest("Invalid input parameters.");
                }

                string query = "SELECT AuditLogID, TenantID, TableName, OperationType, RecordID, OldValue, NewValue, ChangedBy, ChangedDate " +
                               "FROM AuditLog WHERE TenantID = @TenantID";

                List<AuditlogViewModel> result = new List<AuditlogViewModel>();

                using (var connection = new SqlConnection(ConnectionString))
                {
                    await connection.OpenAsync();

                    string fetchParameterQuery = "SELECT AssetParameterID, ParameterName FROM AssetParameters WHERE AssetID = @AssetID";
                    var parametersDict = new Dictionary<int, string>();

                    using (var cmd = new SqlCommand(fetchParameterQuery, connection))
                    {
                        cmd.Parameters.AddWithValue("@AssetID", AssetID);
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                parametersDict[reader.GetInt32(0)] = reader.GetString(1);
                            }
                        }
                    }

                    using (var cmd = new SqlCommand(query, connection))
                    {
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string oldValueJson = reader["OldValue"] as string;
                                string newValueJson = reader["NewValue"] as string;

                                var oldValue = !string.IsNullOrEmpty(oldValueJson)
                                    ? JsonConvert.DeserializeObject<Dictionary<string, object>>(oldValueJson)
                                    : null;

                                var newValue = !string.IsNullOrEmpty(newValueJson)
                                    ? JsonConvert.DeserializeObject<Dictionary<string, object>>(newValueJson)
                                    : null;

                                if (newValue == null) continue;

                                if (newValue.ContainsKey("AssetID") && Convert.ToInt32(newValue["AssetID"]) == AssetID)
                                {
                                    string parameterName = string.Empty;
                                    if (newValue.ContainsKey("AssetParameterID"))
                                    {
                                        int assetParameterID = Convert.ToInt32(newValue["AssetParameterID"]);
                                        parametersDict.TryGetValue(assetParameterID, out parameterName);
                                    }
                                    else if (oldValue != null && oldValue.ContainsKey("AssetParameterID"))
                                    {
                                        int assetParameterID = Convert.ToInt32(oldValue["AssetParameterID"]);
                                        parametersDict.TryGetValue(assetParameterID, out parameterName);
                                    }

                                    result.Add(new AuditlogViewModel
                                    {
                                        AuditLogID = Convert.ToInt32(reader["AuditLogID"]),
                                        OperationType = reader["OperationType"] as string,
                                        ChangedBy = reader["ChangedBy"] as string,
                                        ChangedDate = Convert.ToDateTime(reader["ChangedDate"]),
                                        OldValue = oldValue,
                                        NewValue = newValue,
                                        ParameterName = parameterName
                                    });
                                }
                            }
                        }
                    }
                }

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Error: {ex.Message}");
            }
        }

        [HttpPost("AddOnProcessCapabiltyExcludedDateData")]
        public IActionResult AddOnProcessCapabiltyExcludedDateData(List<ProcessCapabiltyExcludedDatesViewModel> model)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    foreach (var item in model)
                    {
                        int? AssetID = item.AssetID;
                        DateTime FromDate = item.FromDate;
                        DateTime ToDate = item.ToDate;
                        string Reason = item.Reason;
                        int StatusID = item.StatusID;

                        string query = "INSERT INTO ProcessCapabiltyExcludedDates " +
                                       "(AssetID, FromDate, ToDate, Reason,TenantID,CreatedBy,CreatedDate,StatusID) " +
                                       "VALUES (@AssetID, @FromDate, @ToDate, @Reason, @TenantID, @CreatedBy, @CreatedDate, @StatusID)";

                        using (var cmd = new SqlCommand(query, connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.Parameters.AddWithValue("@AssetID", AssetID);
                            cmd.Parameters.AddWithValue("@FromDate", FromDate);
                            cmd.Parameters.AddWithValue("@ToDate", ToDate);
                            cmd.Parameters.AddWithValue("@Reason", Reason);
                            cmd.Parameters.AddWithValue("@TenantID", item.TenantID);
                            cmd.Parameters.AddWithValue("@CreatedBy", item.CreatedBy);
                            cmd.Parameters.AddWithValue("@CreatedDate", item.CreatedDate);
                            cmd.Parameters.AddWithValue("@StatusID", StatusID);

                            int rowsAffected = cmd.ExecuteNonQuery();

                            if (rowsAffected == 0)
                            {
                                return BadRequest("Failed to add process capability data.");
                            }
                        }
                    }

                    return Ok("Process capability data added successfully.");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }



        [HttpGet("GetSpecData")]
        public IActionResult GetSpecData(int ProcessCapabilityID, int TenantID)
        {
            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"SELECT LCL,UCL,Target,FromDate,ToDate
                             FROM ProcessCapabilitySpecifications WHERE ProcessCapabilityID = @ProcessCapabilityID AND TenantID = @TenantID AND StatusId = 1 ORDER BY CreatedDate DESC;";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@ProcessCapabilityID", ProcessCapabilityID);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);

                        _con.Open();
                        DataTable dt = new DataTable();
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(dt);
                        }
                        return Ok(dt);
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpPost("CheckDateExist")]
        public IActionResult CheckDateExist(int ProcessCapabilityID, List<DateRanges> dateRanges)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    foreach (var dateRange in dateRanges)
                    {
                        string query = @"SELECT COUNT(*)
FROM ProcessCapabilitySpecifications
WHERE ProcessCapabilityID = @ProcessCapabilityID
  AND (
        (@FromDate BETWEEN FromDate AND ToDate)
     OR (@ToDate BETWEEN FromDate AND ToDate)
     OR (FromDate BETWEEN @FromDate AND @ToDate)
     OR (ToDate BETWEEN @FromDate AND @ToDate)
      );";

                        using (var cmd = new SqlCommand(query, connection))
                        {
                            cmd.Parameters.AddWithValue("@ProcessCapabilityID", ProcessCapabilityID);
                            cmd.Parameters.AddWithValue("@FromDate", dateRange.FromDate);
                            cmd.Parameters.AddWithValue("@ToDate", dateRange.ToDate);

                            int count = (int)cmd.ExecuteScalar();

                            if (count > 0)
                            {
                                var response = new
                                {
                                    success = false,
                                    message = "Date range already exists."
                                };
                                return BadRequest(response);
                            }
                        }
                    }

                    var successResponse = new
                    {
                        success = true,
                        message = "Date check completed. No conflicts found."
                    };
                    return Ok(successResponse);
                }
            }
            catch (Exception ex)
            {
                var errorResponse = new
                {
                    success = false,
                    message = "An error occurred: " + ex.Message
                };
                return StatusCode(500, errorResponse);
            }
        }

        [HttpPost("SaveSpecficiationData")]
        public IActionResult SaveSpecficiationData(int ProcessCapabilityID, List<DateRanges> dateRanges, int LCL, int UCL, int Target, string CreatedBy, string createdDate, int TenantID)
        {
            try
            {
                var StatusID = 1;
                DateTime CreatedDate = DateTime.Parse(createdDate);
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    foreach (var dateRange in dateRanges)
                    {
                        string query = @"INSERT INTO ProcessCapabilitySpecifications 
                        (LCL, UCL, Target, FromDate, ToDate, ProcessCapabilityID, TenantID, StatusID, CreatedBy, CreatedDate)
                        VALUES 
                        (@LCL, @UCL, @Target, @FromDate, @ToDate, @ProcessCapabilityID, @TenantID, @StatusID, @CreatedBy, @CreatedDate)";

                        using (var cmd = new SqlCommand(query, connection))
                        {
                            cmd.Parameters.AddWithValue("@LCL", LCL);
                            cmd.Parameters.AddWithValue("@UCL", UCL);
                            cmd.Parameters.AddWithValue("@Target", Target);
                            cmd.Parameters.AddWithValue("@FromDate", dateRange.FromDate);
                            cmd.Parameters.AddWithValue("@ToDate", dateRange.ToDate);
                            cmd.Parameters.AddWithValue("@ProcessCapabilityID", ProcessCapabilityID);
                            cmd.Parameters.AddWithValue("@TenantID", TenantID);
                            cmd.Parameters.AddWithValue("@StatusID", StatusID);
                            cmd.Parameters.AddWithValue("@CreatedBy", CreatedBy);
                            cmd.Parameters.AddWithValue("@CreatedDate", CreatedDate);

                            int count = cmd.ExecuteNonQuery();

                            if (count > 0)
                            {
                                string query2 = @"SELECT TOP 1 LCL, UCL, Target 
                          FROM ProcessCapabilitySpecifications 
                          ORDER BY ToDate DESC";

                                DataTable dataTable = new DataTable();
                                using (var cmd2 = new SqlCommand(query2, connection))
                                {
                                    using (var adapter = new SqlDataAdapter(cmd2))
                                    {
                                        adapter.Fill(dataTable);
                                    }
                                }

                                if (dataTable.Rows.Count > 0)
                                {
                                    DataRow row = dataTable.Rows[0];
                                    int fetchedLCL = row.IsNull("LCL") ? 0 : Convert.ToInt32(row["LCL"]);
                                    int fetchedUCL = row.IsNull("UCL") ? 0 : Convert.ToInt32(row["UCL"]);
                                    int fetchedTarget = row.IsNull("Target") ? 0 : Convert.ToInt32(row["Target"]);

                                    var updateResponse = UpdateProcessCapabilityValuesData(
                                        fetchedLCL,
                                        fetchedUCL,
                                        fetchedTarget,
                                        ProcessCapabilityID);

                                    if (updateResponse)
                                    {
                                        var successResponse = new
                                        {
                                            success = true,
                                            message = "Data saved and configuration updated successfully."
                                        };
                                        return Ok(successResponse);
                                    }
                                }
                            }
                        }
                    }

                    var noConflictResponse = new
                    {
                        success = true,
                        message = "Date check completed. No conflicts found."
                    };
                    return Ok(noConflictResponse);
                }
            }
            catch (Exception ex)
            {
                var errorResponse = new
                {
                    success = false,
                    message = "An error occurred: " + ex.Message
                };
                return StatusCode(500, errorResponse);
            }
        }

        private bool UpdateProcessCapabilityValuesData(int LCL, int UCL, int Target, int ProcessCapabilityID)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    string query = @"UPDATE ProcessCapabilityConfiguration 
                     SET LCL = @LCL, UCL = @UCL, Target = @Target 
                     WHERE ProcessCapabilityID = @ProcessCapabilityID";

                    using (var cmd = new SqlCommand(query, connection))
                    {
                        cmd.Parameters.AddWithValue("@LCL", LCL);
                        cmd.Parameters.AddWithValue("@UCL", UCL);
                        cmd.Parameters.AddWithValue("@Target", Target);
                        cmd.Parameters.AddWithValue("@ProcessCapabilityID", ProcessCapabilityID);

                        int count = cmd.ExecuteNonQuery();

                        return count > 0;
                    }
                }
            }
            catch
            {
                return false;
            }
        }


        [HttpGet("DeleteProcessCapabilityData")]
        public IActionResult GetDeleteProcessCapabilityData(int ProcessCapabilityID, int TenantID, string UserId)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    // Set session context for auditing
                    var sessionContextQuery = "EXEC sp_set_session_context @key = N'UserId', @value = @userId";
                    using (var command = new SqlCommand(sessionContextQuery, connection))
                    {
                        command.Parameters.AddWithValue("@userId", UserId);
                        command.ExecuteNonQuery();
                    }

                    // Perform the delete operation
                    using (var cmd = new SqlCommand("UPDATE ProcessCapabilityConfiguration SET [StatusID] = 3 WHERE [ProcessCapabilityID] = @ProcessCapabilityID AND [TenantID] = @TenantID", connection))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@ProcessCapabilityID", ProcessCapabilityID);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);

                        int rowsAffected = cmd.ExecuteNonQuery();

                        if (rowsAffected == 0)
                        {
                            return BadRequest("Failed to delete Process Capability data.");
                        }
                    }

                    return Ok("Process Capability data deleted successfully.");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }

        [HttpGet("GetExcludeDateData")]
        public IActionResult GetExcludeDateData(int ProcessCapabilityID, int TenantID, int AssetID)
        {
            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"SELECT ProcessCapabiltyExcludedDatesID, FromDate, ToDate, Reason, CreatedBy, CreatedDate
                             FROM ProcessCapabiltyExcludedDates WHERE (@ProcessCapabilityID IS NULL OR ProcessCapabilityID = @ProcessCapabilityID) AND TenantID = @TenantID AND AssetID = @AssetID AND StatusId = 1 ORDER BY CreatedDate DESC;";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@ProcessCapabilityID", ProcessCapabilityID);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);
                        cmd.Parameters.AddWithValue("@AssetID", AssetID);
                        _con.Open();

                        DataTable dt = new DataTable();
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(dt);
                        }
                        return Ok(dt);
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpPost("AddProcessCapabiltyExcludedDateData")]
        public IActionResult AddProcessCapabiltyExcludedDateData(List<ProcessCapabiltyExcludedDatesViewModel> model)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    // Set session context for auditing
                    string sessionContextQuery = "EXEC sp_set_session_context @key = N'UserId', @value = @userId";
                    using (var command = new SqlCommand(sessionContextQuery, connection))
                    {
                        command.Parameters.AddWithValue("@userId", model.First().CreatedBy); // Assuming all entries have the same CreatedBy
                        command.ExecuteNonQuery();
                    }

                    foreach (var item in model)
                    {
                        int ProcessCapabilityID = item.ProcessCapabilityID;
                        DateTime FromDate = item.FromDate;
                        DateTime ToDate = item.ToDate;
                        string Reason = item.Reason;
                        int StatusID = item.StatusID;
                        int? AssetID = item.AssetID;

                        string query = @"
                INSERT INTO ProcessCapabiltyExcludedDates 
                (ProcessCapabilityID, FromDate, ToDate, Reason, TenantID, CreatedBy, CreatedDate, StatusID, AssetID) 
                VALUES 
                (@ProcessCapabilityID, @FromDate, @ToDate, @Reason, @TenantID, @CreatedBy, @CreatedDate, @StatusID, @AssetID)";

                        using (var cmd = new SqlCommand(query, connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.Parameters.AddWithValue("@ProcessCapabilityID", ProcessCapabilityID);
                            cmd.Parameters.AddWithValue("@FromDate", FromDate);
                            cmd.Parameters.AddWithValue("@ToDate", ToDate);
                            cmd.Parameters.AddWithValue("@Reason", Reason);
                            cmd.Parameters.AddWithValue("@TenantID", item.TenantID);
                            cmd.Parameters.AddWithValue("@CreatedBy", item.CreatedBy);
                            cmd.Parameters.AddWithValue("@CreatedDate", item.CreatedDate);
                            cmd.Parameters.AddWithValue("@StatusID", StatusID);
                            cmd.Parameters.AddWithValue("@AssetID", AssetID);

                            int rowsAffected = cmd.ExecuteNonQuery();

                            if (rowsAffected == 0)
                            {
                                return BadRequest("Failed to add process capability data.");
                            }
                        }
                    }

                    return Ok("Process capability data added successfully.");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }



        [HttpGet("DeleteProcessCapabilityExcludedData")]
        public IActionResult GetDeleteProcessCapabilityExcludedData(int ProcessCapabiltyExcludedDatesID, int TenantID, string UserId, string CurrentDateTime)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    // Set session context for auditing
                    var sessionContextQuery = "EXEC sp_set_session_context @key = N'UserId', @value = @userId";
                    using (var command = new SqlCommand(sessionContextQuery, connection))
                    {
                        command.Parameters.AddWithValue("@userId", UserId);
                        command.ExecuteNonQuery();
                    }

                    DateTime currentDateTime = DateTime.Parse(CurrentDateTime);

                    using (var cmd = new SqlCommand(
                        "Delete ProcessCapabiltyExcludedDates WHERE ProcessCapabiltyExcludedDatesID = @ProcessCapabiltyExcludedDatesID AND TenantID = @TenantID",
                        connection))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@ProcessCapabiltyExcludedDatesID", ProcessCapabiltyExcludedDatesID);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);

                        int rowsAffected = cmd.ExecuteNonQuery();

                        if (rowsAffected == 0)
                        {
                            return BadRequest("Failed to delete Process Capability Data.");
                        }
                    }

                    return Ok("Process Capability Data deleted successfully.");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }

        [HttpGet("GetFloorLevalAssetName")]
        public IActionResult GetFloorLevalAssetName(string tenantID, string userID)
        {
            SqlCommand cmd = new SqlCommand("[dbo].[USP_GetDeviceIdsForTreeView]");
            var ds = new DataSet();
            try
            {
                cmd.Connection = new SqlConnection(ConnectionString);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@userID", userID);
                cmd.Parameters.AddWithValue("@tenantId", tenantID);
                cmd.Parameters.AddWithValue("@deviceCategory", string.Empty);
                cmd.Parameters.AddWithValue("@customFilter", string.Empty);
                cmd.Parameters.AddWithValue("@showGroup", 0);

                var da1 = new SqlDataAdapter { SelectCommand = cmd };
                if (cmd.Connection != null && cmd.Connection.State == ConnectionState.Closed)
                    cmd.Connection.Open();
                da1.Fill(ds);
                if (ds.Tables.Count > 0 && ds.Tables[0] != null)
                {
                    var table = ds.Tables[0];
                    var distinctRows = table.AsEnumerable()
                                            .GroupBy(row => row.Field<int>("FloorId"))
                                            .Select(g => g.First())
                                            .CopyToDataTable();

                    ds.Tables.RemoveAt(0);
                    ds.Tables.Add(distinctRows);
                }
                return Ok(ds);
            }
            catch (Exception)
            {
                return Ok(ds);
            }
            finally
            {
                cmd.Connection.Close();
            }
        }

        [HttpGet("GetAssetName")]
        public IActionResult GetAssetName(string tenantID, string userID)
        {
            SqlCommand cmd = new SqlCommand("[dbo].[USP_GetDeviceIdsForTreeView]");
            var ds = new DataSet();
            try
            {
                cmd.Connection = new SqlConnection(ConnectionString);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@userID", userID);
                cmd.Parameters.AddWithValue("@tenantId", tenantID);
                cmd.Parameters.AddWithValue("@deviceCategory", string.Empty);
                cmd.Parameters.AddWithValue("@customFilter", string.Empty);
                cmd.Parameters.AddWithValue("@showGroup", 0);

                var da1 = new SqlDataAdapter { SelectCommand = cmd };
                if (cmd.Connection != null && cmd.Connection.State == ConnectionState.Closed)
                    cmd.Connection.Open();
                da1.Fill(ds);
                return Ok(ds);
            }
            catch (Exception)
            {
                return Ok(ds);
            }
            finally
            {
                cmd.Connection.Close();
            }
        }


        [HttpGet("GetParameterName")]
        public IActionResult GetParameterName(string tenantID, string assetID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = @"
                    SELECT pc.ProcessCapabilityID,pc.AssetParameterID,pc.ProcessID,pc.ProcessName, ap.ParameterName,pc.DecimalCount 
                    FROM ProcessCapabilityConfiguration pc 
                    JOIN AssetParameters ap ON pc.AssetParameterID = ap.AssetParameterId 
                    WHERE pc.AssetID = @AssetID AND pc.TenantID = @TenantID  
                        and pc.StatusId=1 and ap.StatusId=1
                    order by ap.sortorder";
                        cmd.Parameters.AddWithValue("@AssetID", assetID);
                        cmd.Parameters.AddWithValue("@TenantID", tenantID);
                        var da = new SqlDataAdapter(cmd);
                        connection.Open();
                        da.Fill(dt);
                        return Ok(dt);
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("getDashboardData")]
        public IActionResult getDashboardData(string tenantId, string assetId, string paramID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        DateTime toDate = DateTime.Now;
                        DateTime fromDate = toDate.AddDays(-30);

                        cmd.CommandText = @"
                  SELECT  pcr.SampleMean_Cp,pcr.SampleN_Cp,pcr.SampleMean_Pp,  pcr.SampleN_Pp,pcr.StDev_Overall,
    pcr.StDev_Between,  pcr.StDev_Within,pcr.StDev_B_W,pcr.Pp,pcr.Ppk,pcr.Ppl,   pcr.Ppu,pcr.Cpm,pcr.Cp,
    pcr.Cpk,pcr.Cpl,  pcr.Cpu,pcr.PPMTotal_Observed,pcr.PPMTotal_ExpectedOverall,
    pcr.PPMTotal_ExpectedB_W,pcr.Outliers_count,   pcr.Outliers_avg_value,
    pcr.Outliers_avg_residual, pcr.NumSubgroups,  pcr.MeanWithinStd,pcr.PPM_LSL_Observed,
    pcr.PPM_USL_Observed,  pcr.PPM_LSL_ExpectedOverall,pcr.PPM_USL_ExpectedOverall,
    pcr.PPM_LSL_ExpectedB_W,pcr.PPM_USL_ExpectedB_W
FROM ProcessCapabilityReadings pcr
JOIN ProcessCapabilityConfiguration pcc ON pcr.ProcessCapabilityID = pcc.ProcessCapabilityID
WHERE pcc.AssetID = @AssetID
  AND pcr.FromDate >= @FromDate 
  AND pcr.ToDate <= @ToDate 
  AND  pcc.AssetParameterID= @ParamID";

                        cmd.Parameters.AddWithValue("@AssetID", assetId);
                        cmd.Parameters.AddWithValue("@ParamID", paramID);
                        cmd.Parameters.AddWithValue("@FromDate", fromDate);
                        cmd.Parameters.AddWithValue("@ToDate", toDate);

                        var da = new SqlDataAdapter(cmd);
                        connection.Open();
                        da.Fill(dt);
                        return Ok(dt);
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpPost("GetProcessCapabilityHistoricalData")]
        public DataSet GetProcessCapabilityHistoricalData(int tenantId, int assetID, int processCapabilityID, string fromDate, string toDate)
        {
            SqlCommand cmd = new SqlCommand("[dbo].[USP_GetProcessCapabilityHistoricalData]");
            var ds = new DataSet();
            try
            {
                cmd.Connection = new SqlConnection(ConnectionString);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@assetID", assetID);
                cmd.Parameters.AddWithValue("@processCapabilityID", processCapabilityID);
                cmd.Parameters.AddWithValue("@FromDate", fromDate);
                cmd.Parameters.AddWithValue("@ToDate", toDate);
                cmd.Parameters.AddWithValue("@tenantId", tenantId);

                var da1 = new SqlDataAdapter { SelectCommand = cmd };
                if (cmd.Connection != null && cmd.Connection.State == ConnectionState.Closed)
                    cmd.Connection.Open();
                da1.Fill(ds);
                return ds;
            }
            catch (Exception)
            {
                return ds;
            }
            finally
            {
                cmd.Connection.Close();
            }
        }

        private DataSet GetAssetDataForProcessCapability(int assetID, string fromDate, string toDate, string tenantId)
        {
            SqlCommand cmd = new SqlCommand("[dbo].[USP_GetAssetDataProcessCapability]");
            var ds = new DataSet();
            try
            {
                cmd.Connection = new SqlConnection(ConnectionString);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@deviceid", assetID);
                cmd.Parameters.AddWithValue("@fromdt", fromDate);
                cmd.Parameters.AddWithValue("@todt", toDate);
                cmd.Parameters.AddWithValue("@tenantId", tenantId);

                var da1 = new SqlDataAdapter { SelectCommand = cmd };
                if (cmd.Connection != null && cmd.Connection.State == ConnectionState.Closed)
                    cmd.Connection.Open();
                da1.Fill(ds);
                return ds;
            }
            catch (Exception)
            {
                return ds;
            }
            finally
            {
                cmd.Connection.Close();
            }
        }

        [HttpGet("GetImage")]
        public IActionResult GetImage(string toDate, int processCapabilityID, int TenantID)
        {
            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"
                SELECT d.DeviceName, pc.ProcessID, pc.ProcessName,pc.ProcessCapabilityID, ap.ParameterName, pr.ToDate, pr.ImgSRC
                FROM ProcessCapabilityConfiguration pc
                JOIN ProcessCapabilityReadings pr ON pr.ProcessCapabilityID = pc.ProcessCapabilityID
                JOIN Devices d ON pc.AssetID = d.DeviceID
                JOIN AssetParameters ap ON pc.AssetParameterID = ap.AssetParameterId
                WHERE pr.ProcessCapabilityID = @ProcessCapabilityID 
                AND pc.TenantID = @TenantID 
                AND pr.ToDate = @ToDate";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@ToDate", toDate);
                        cmd.Parameters.AddWithValue("@ProcessCapabilityID", processCapabilityID);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);
                        _con.Open();

                        var imageResults = new List<object>();
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                imageResults.Add(new
                                {
                                    DeviceName = reader["DeviceName"].ToString(),
                                    ProcessID = reader["ProcessID"],
                                    ProcessName = reader["ProcessName"].ToString(),
                                    ParameterName = reader["ParameterName"].ToString(),
                                    ToDate = reader["ToDate"],
                                    ImgSRC = reader["ImgSRC"].ToString()
                                });
                            }
                        }
                        return Ok(imageResults);
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }


        [HttpPost("getListData")]
        public IActionResult getListData(string tenantID, string paramID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = @"
                  SELECT ProcessCapabilityReadingsID, 
						 FromDate,ToDate,Cp,Pp,Ppk FROM ProcessCapabilityReadings WHERE 
ProcessCapabilityID = @paramID
ORDER BY ToDate DESC, FromDate DESC;";

                        cmd.Parameters.AddWithValue("@paramID", paramID);

                        using (var da = new SqlDataAdapter(cmd))
                        {
                            connection.Open();
                            da.Fill(dt);
                        }
                    }
                }

                return Ok(dt);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }
        [HttpPost("saveComment")]
        public IActionResult SaveComment(List<ProcessCapabilityoutoflimitInfo> model)
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

                    return Ok("Comments saved successfully.");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, "An error occurred: " + ex.Message);
            }
        }

        [HttpGet("GetFloorData")]
        public IActionResult GetFloorData(string tenantID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = @"
                    SELECT DISTINCT 
            f.FloorID,  CONCAT(l.LocationName, ' - ', b.BuildingName, ' - ', f.FloorName) AS Name
            FROM Floor  f
            JOIN Building b ON f.BuildingID = b.BuildingID AND b.StatusID = 1 AND f.TenantID = b.TenantID
            JOIN Location l ON b.LocationID = l.LocationID 
        AND b.TenantID = l.TenantID WHERE f.TenantID = @tenantID; ";

                        cmd.Parameters.AddWithValue("@TenantID", tenantID);

                        var da = new SqlDataAdapter(cmd);

                        connection.Open();
                        da.Fill(dt);

                        return Ok(dt);
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }

        
        // PS START

        [HttpPost("LoadMetricsDataByPlant")]
        public IActionResult loadMetricsDataByPlant(string tenantID, string plantID, string fromDate, string toDate)
        {
            try
            {
                var dt = loadProcessCapabilityData_Plant(tenantID, plantID, fromDate, toDate);
                return Ok(dt);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }

        public async Task<string> loadProcessCapabilityData_Plant(string tenantID, string plantID, string fromDate, string toDate)
        {
            var paramDetails = getProcessCapabilityAssetParamsByPlant(tenantID, plantID);

            var assetList = paramDetails.AsEnumerable()
                                        .GroupBy(row => row.Field<int>("AssetID"))
                                        .Select(group => new { AssetID = group.Key })
                                        .ToList();

            var tasks = new List<Task>(); // List to hold all tasks

            foreach (var assetGroup in assetList)
            {
                int assetID = assetGroup.AssetID;
                var assetData = GetAssetDataForProcessCapability(assetID, fromDate, toDate, tenantID);
                if (assetData.Tables.Count > 3 && assetData.Tables[0].Rows.Count > 0)
                {
                    var pcSpecificationsData = assetData.Tables[3];
                    DataTable assetDataTable = assetData.Tables[0];
                    DataTable excludeDates = assetData.Tables[2];
                    foreach (DataColumn column in assetDataTable.Columns)
                    {
                        if (column.ColumnName == "Date")
                            continue;
                        var matchingRows = paramDetails.AsEnumerable()
                                                       .Where(row => row.Field<string>("ParameterName") == column.ColumnName && row.Field<int>("AssetID") == assetID)
                                                       .ToList();
                        if (matchingRows.Count == 0)
                            continue;

                        bool hasNonNullValues = assetDataTable.AsEnumerable()
                                                             .Any(row => !row.IsNull(column.ColumnName));
                        if (!hasNonNullValues)
                            continue;
                        DataTable filteredDataTable = new DataTable();
                        filteredDataTable.Columns.Add("D", typeof(DateTime));
                        filteredDataTable.Columns.Add("V", typeof(string));

                        foreach (var row in assetDataTable.AsEnumerable().Where(row => !row.IsNull(column.ColumnName)))
                        {
                            if (!row.IsNull(column.ColumnName) && !string.IsNullOrWhiteSpace(row[column.ColumnName]?.ToString()))
                            {
                                DataRow newRow = filteredDataTable.NewRow();
                                newRow["D"] = row.Field<DateTime>("Date");
                                newRow["V"] = row[column.ColumnName]?.ToString();
                                filteredDataTable.Rows.Add(newRow);
                            }
                        }

                        var ID = int.Parse(matchingRows[0]["ProcessCapabilityID"].ToString());
                        var paramID = int.Parse(matchingRows[0]["AssetParameterID"].ToString());
                        var lsl = double.Parse(matchingRows[0]["LCL"].ToString());
                        var usl = double.Parse(matchingRows[0]["UCL"].ToString());
                        var target = double.Parse(matchingRows[0]["Target"].ToString());
                        var tenantCode = matchingRows[0]["TenantCode"].ToString();
                        var subgroupSize = int.Parse(matchingRows[0]["SubgroupSize"].ToString());
                        var isFilter = int.Parse(matchingRows[0]["IsFilter"].ToString());
                        var datatypeID = int.Parse(matchingRows[0]["DatatypeID"].ToString());
                        var assetParameterCategoryId = int.Parse(matchingRows[0]["AssetParameterCategoryId"].ToString());
                        var is20DataPoints = 0;
                        // Start the task without awaiting
                        tasks.Add(Task.Run(async () =>
                        {
                            await pc.loadProcessCapabilityDataAsync(
                                filteredDataTable, pcSpecificationsData, excludeDates, tenantCode, assetID, paramID, int.Parse(tenantID), ID,
                                fromDate, toDate, datatypeID, lsl, usl, target, isFilter, subgroupSize, assetParameterCategoryId, is20DataPoints);
                            await Task.Delay(2000);
                        }));
                    }
                }
            }

            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false); // Prevent deadlock
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception while awaiting tasks: {ex.Message}");
            }
            await Task.Delay(10000);
            // Execute getmetricsDatafromDB_New after all tasks are done
            return "Sucess";
        }

        private DataTable getProcessCapabilityAssetParamsByPlant(string tenantID, string plantID)
        {
            var data = new DataTable();
            string query = @"
            SELECT pc.ProcessCapabilityID, 
       pc.AssetID,
       ap.AssetParameterCategoryId,
       pc.AssetParameterID,
       ref.ParameterName,
       pc.LCL,
       pc.UCL,
       pc.Target,
       pc.TimeInterval,
       pc.DatatypeID,
       pc.TenantID,
       pc.LastRunAt,
       t.TenantCode,
       pc.SubgroupSize,
       pc.IsFilter
FROM ProcessCapabilityConfiguration pc 
JOIN Devices d 
    ON d.DeviceID = pc.AssetID 
    AND pc.TenantID = d.TenantID 
	join Floor f on f.FloorID=d.FloorID and f.TenantID=d.TenantID
JOIN AssetParameters ap 
    ON ap.AssetParameterId = pc.AssetParameterID 
    AND ap.AssetId = pc.AssetID 
    AND ap.TenantId = pc.TenantID 
JOIN em.ReferenceParameters ref 
    ON ref.ReferenceParameterID = ap.ReferenceParameterId 
JOIN TenantDetails t 
    ON t.TenantDetailsID = pc.TenantID  -- Add the join for the Tenant table
WHERE f.BuildingID = " + plantID + "  AND pc.TenantID = " + tenantID + " AND pc.StatusId = 1 AND d.StatusID = 1 AND ap.StatusId = 1 and pc.LCL is not null and pc.UCL is not null and pc.Target is not null ORDER BY pc.AssetParameterID";

            using (var conn = new SqlConnection(ConnectionString))
            using (var cmd = new SqlCommand(query, conn))
            {
                cmd.CommandType = CommandType.Text;

                try
                {
                    var da1 = new SqlDataAdapter { SelectCommand = cmd };
                    if (cmd.Connection != null && cmd.Connection.State == ConnectionState.Closed)
                        cmd.Connection.Open();
                    da1.Fill(data);
                    return data;
                }
                catch (Exception ex)
                {
                    return data;
                    throw ex;
                }
                finally
                {
                    cmd.Connection.Close();
                }
            }
        }

        [HttpPost("GetMetricsData_Reload")]
        public IActionResult getMetricsData_Reload(string tenantID, string floorID, string fromDate, string toDate)
        {
            try
            {
                var parameterIds = "";
                var dt = loadProcessCapabilityData_Floor(parameterIds, tenantID, floorID, fromDate, toDate, 2);
                return Ok(dt);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }

        [HttpGet("GetConditionParameterData")]
        public DataSet GetConditionParameterData(string tenantID, string floorID, string fromDate, string toDate)
        {
            SqlCommand cmd = new SqlCommand("[dbo].[USP_GetConditionedParameters]");
            var ds = new DataSet();
            try
            {

                cmd.Connection = new SqlConnection(ConnectionString);

                cmd.Connection = new SqlConnection(ConnectionString);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@FromDate", fromDate);
                cmd.Parameters.AddWithValue("@ToDate ", toDate);
                cmd.Parameters.AddWithValue("@FloorID", floorID);
                cmd.Parameters.AddWithValue("@TenantID", tenantID);
                var da1 = new SqlDataAdapter { SelectCommand = cmd };
                if (cmd.Connection != null && cmd.Connection.State == ConnectionState.Closed)
                    cmd.Connection.Open();
                da1.Fill(ds);
                return ds;
            }
            catch (Exception)
            {
                return ds;
            }
            finally
            {
                cmd.Connection.Close();
            }
        }
        [HttpPost("getPieChartData")]
        public DataSet getPieChartData(string tenantID, string floorID, string fromDate, string toDate, string conditionText)
        {
            SqlCommand cmd = new SqlCommand("[dbo].[USP_GetPiechartData]");
            var ds = new DataSet();
            try
            {
                cmd.Connection = new SqlConnection(ConnectionString);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@FloorID", floorID);
                cmd.Parameters.AddWithValue("@FromDate", fromDate);
                cmd.Parameters.AddWithValue("@ToDate ", toDate);
                cmd.Parameters.AddWithValue("@TenantID", tenantID);
                cmd.Parameters.AddWithValue("@ConditionText", conditionText);
                var da1 = new SqlDataAdapter { SelectCommand = cmd };
                if (cmd.Connection != null && cmd.Connection.State == ConnectionState.Closed)
                    cmd.Connection.Open();
                da1.Fill(ds);
                return ds;
            }
            catch (Exception)
            {
                return ds;
            }
            finally
            {
                cmd.Connection.Close();
            }
        }
        [HttpPost("GetMultiSelectionData")]
        public IActionResult GetMultiSelectionData(string parameterIds, string tenantID, string floorID, string fromDate, string toDate)
        {
            try
            {
                var dt = getMultiSelectionDatafromDB(parameterIds, tenantID, floorID, fromDate, toDate);
                return Ok(dt);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }


        public DataTable getMultiSelectionDatafromDB(string parameterIds, string tenantID, string floorID, string fromDate, string toDate)
        {
            var dt = new DataTable();

            try
            {
                var parameterIdList = parameterIds.Split(',').Select(id => id.Trim()).ToList();
                string parameterIdParams = string.Join(",", parameterIdList.Select((id, index) => $"@ParameterId{index}"));

                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    string query = $@"SELECT
                                ap.AssetParameterId,
                                ap.ParameterName,
 ISNULL(cr.Cp, 0) AS Cp,
    ISNULL(cr.Ppk, 0) AS Ppk,
COALESCE(cr.LCL, pc.UpdatedLCL, pc.LCL) AS LSL,
COALESCE(cr.UCL, pc.UpdatedUCL, pc.UCL) AS USL,
    COALESCE(cr.Target, pc.UpdatedTarget, pc.Target) AS Target,
                                
                                cr.SampleN_Pp AS Data_Points,
                                cr.SampleMean_Pp AS Sample_Mean,
                                cr.StDev_Overall AS StDev_Overall,
                                cr.StDev_Within AS StDev_Within,
                                
                                cr.Target,
                                cr.ImgSRC
                             FROM ProcessCapabilityReadings cr
                             JOIN ProcessCapabilityConfiguration pc ON cr.ProcessCapabilityID = pc.ProcessCapabilityID
                             JOIN AssetParameters ap ON ap.AssetParameterId = pc.AssetParameterID
                             WHERE ap.AssetParameterId IN ({parameterIdParams})
                               AND ap.TenantId = @TenantId
                               AND cr.FromDate = @FromDate
                               AND cr.ToDate = @ToDate
                               AND pc.StatusId =1 and ap.StatusId=1;";

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        for (int i = 0; i < parameterIdList.Count; i++)
                        {
                            command.Parameters.AddWithValue($"@ParameterId{i}", parameterIdList[i]);
                        }

                        command.Parameters.AddWithValue("@TenantId", tenantID);
                        command.Parameters.AddWithValue("@FromDate", fromDate);
                        command.Parameters.AddWithValue("@ToDate", toDate);

                        using (SqlDataAdapter adapter = new SqlDataAdapter(command))
                        {
                            adapter.Fill(dt);
                        }
                    }
                }

                // If no data is found, fetch from another method
                if (dt.Rows.Count == 0)
                {
                    dt = loadProcessCapabilityData_Floor(parameterIds, tenantID, floorID, fromDate, toDate, 1).GetAwaiter().GetResult();
                }
            }
            catch (Exception ex)
            {
                // Log error or handle accordingly
                throw new Exception("Error fetching multi-selection data.", ex);
            }

            return dt;
        }


        public DataTable getMuliSelectionDatafromDB_New(string parameterIds, string tenantID, string floorID, string fromDate, string toDate)
        {
            var dt = new DataTable();

            try
            {
                var parameterIdList = parameterIds.Split(',').Select(id => id.Trim()).ToList();
                string parameterIdParams = string.Join(",", parameterIdList.Select((id, index) => $"@ParameterId{index}"));

                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    string query = $@"SELECT
                                ap.AssetParameterId,
                                ap.ParameterName,
 ISNULL(cr.Cp, 0) AS Cp,
    ISNULL(cr.Ppk, 0) AS Ppk,
 COALESCE(cr.LCL, pc.UpdatedLCL, pc.LCL) AS LSL,
COALESCE(cr.UCL, pc.UpdatedUCL, pc.UCL) AS USL,
    COALESCE(cr.Target, pc.UpdatedTarget, pc.Target) AS Target,
                                
                                cr.SampleN_Pp AS Data_Points,
                                cr.SampleMean_Pp AS Sample_Mean,
                                cr.StDev_Overall AS StDev_Overall,
                                cr.StDev_Within AS StDev_Within,
                                
                                cr.Target,
                                cr.ImgSRC
                             FROM ProcessCapabilityReadings cr
                             JOIN ProcessCapabilityConfiguration pc ON cr.ProcessCapabilityID = pc.ProcessCapabilityID
                             JOIN AssetParameters ap ON ap.AssetParameterId = pc.AssetParameterID
                             WHERE ap.AssetParameterId IN ({parameterIdParams})
                               AND ap.TenantId = @TenantId
                               AND cr.FromDate = @FromDate
                               AND cr.ToDate = @ToDate
                               AND pc.StatusId =1 and ap.StatusId=1;";

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        for (int i = 0; i < parameterIdList.Count; i++)
                        {
                            command.Parameters.AddWithValue($"@ParameterId{i}", parameterIdList[i]);
                        }

                        command.Parameters.AddWithValue("@TenantId", tenantID);
                        command.Parameters.AddWithValue("@FromDate", fromDate);
                        command.Parameters.AddWithValue("@ToDate", toDate);

                        using (SqlDataAdapter adapter = new SqlDataAdapter(command))
                        {
                            adapter.Fill(dt);
                        }
                    }
                }


            }
            catch (Exception ex)
            {
                // Log error or handle accordingly
                throw new Exception("Error fetching multi-selection data.", ex);
            }

            return dt;
        }

        [HttpPost("getMetricsData")]
        public IActionResult GetMetricsData(string tenantID, string floorID, string fromDate, string toDate)
        {
            try
            {
                var dt = getmetricsDatafromDB(tenantID, floorID, fromDate, toDate);
                return Ok(dt);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }

        public DataTable getmetricsDatafromDB(string tenantID, string floorID, string fromDate, string toDate)
        {
            var dt = new DataTable();
            var parameterIds = "";

            using (var connection = new SqlConnection(ConnectionString))
            {
                using (var cmd = new SqlCommand())
                {
                    cmd.Connection = connection;
                    cmd.CommandText = @"
SELECT 
    d.DeviceID AS AssetID, d.DeviceName AS AssetName,ap.AssetParameterId,apc.ParameterCategory AS [Category],pcr.StDev_Within,
    pcr.StDev_Overall,CONCAT(ap.ParameterName, ' (', ap.DisplayText, ')') AS DisplayText,
    ap.DisplayText,pcr.SampleN_Pp AS Size,ap.ParameterName, 
    pcr.Cpk,

CASE 
        WHEN pcr.SampleN_Pp IS NOT NULL THEN ISNULL(pcr.Cp, 0)
        ELSE NULL
    END AS Cp,
	CASE 
        WHEN pcr.SampleN_Pp IS NOT NULL THEN ISNULL(pcr.Ppk, 0)
        ELSE NULL
    END AS Ppk,
pcr.Pp,
    COALESCE(pcr.LCL, pcc.UpdatedLCL, pcc.LCL) AS LSL,COALESCE(pcr.UCL, pcc.UpdatedUCL, pcc.UCL) AS USL,
    COALESCE(pcr.Target, pcc.UpdatedTarget, pcc.Target) AS Target, 
pcr.SampleMean_Pp,
    pcr.SampleMean_Cp, COALESCE(pcr.UCL, pcc.UpdatedUCL, pcc.UCL) - COALESCE(pcr.LCL, pcc.UpdatedLCL, pcc.LCL) AS Tolerance,
    pcr.ProcessCapabilityID,pcr.ProcessCapabilityReadingsID,d.SortOrder,ap.SortOrder AS ParamSortOrder
FROM ProcessCapabilityConfiguration pcc 
JOIN AssetParameters ap ON pcc.AssetParameterID = ap.AssetParameterId
JOIN AssetParameterCategory apc  ON ap.AssetParameterCategoryId = apc.AssetParameterCategoryId
JOIN Devices d ON ap.AssetId = d.DeviceID
LEFT JOIN ProcessCapabilityReadings pcr ON pcr.ProcessCapabilityID = pcc.ProcessCapabilityID AND pcr.TenantID = @tenantID AND pcr.FromDate = @fromDate AND pcr.ToDate = @toDate
WHERE  d.FloorID = @floorID AND pcc.Statusid = 1 AND ap.StatusId = 1 AND d.StatusID = 1
ORDER BY d.SortOrder,ap.SortOrder;";
                    cmd.Parameters.AddWithValue("@tenantID", tenantID);
                    cmd.Parameters.AddWithValue("@fromDate", fromDate);
                    cmd.Parameters.AddWithValue("@toDate", toDate);
                    cmd.Parameters.AddWithValue("@floorID", floorID);
                    using (var da = new SqlDataAdapter(cmd))
                    {
                        connection.Open();
                        da.Fill(dt);
                        if (dt.Rows.Count > 0)
                        {
                            var nonNullSizeRowsEnum = dt.AsEnumerable()
                            .Where(row => !row.IsNull("Size"));

                            if (nonNullSizeRowsEnum.Any())
                            {
                                var nonNullSizeRows = nonNullSizeRowsEnum.CopyToDataTable();
                                return dt; // or return nonNullSizeRows based on your logic
                            }

                            
                        }

                        dt = loadProcessCapabilityData_Floor(parameterIds, tenantID, floorID, fromDate, toDate, 2).GetAwaiter().GetResult();
                        return dt;
                    
                    }
                }
            }
        }

        public async Task<DataTable> loadProcessCapabilityData_Floor(string parameterIds, string tenantID, string floorID, string fromDate, string toDate, int checkmethod)
        {
            try
            {
                var paramDetails = getProcessCapabilityParamsByFloor(tenantID, floorID);

                var assetList = paramDetails.AsEnumerable()
                                            .GroupBy(row => row.Field<int>("AssetID"))
                                            .Select(group => new { AssetID = group.Key })
                                            .ToList();

                var tasks = new List<Task>(); // List to hold all tasks
                var flag = 0;
                foreach (var assetGroup in assetList)
                {
                    int assetID = assetGroup.AssetID;
                    var assetData = GetAssetDataForProcessCapability(assetID, fromDate, toDate, tenantID);
                    if (assetData.Tables.Count > 3 && assetData.Tables[0].Rows.Count > 0)
                    {
                        flag = +1;
                        var pcSpecificationsData = assetData.Tables[3];
                        DataTable assetDataTable = assetData.Tables[0];
                        DataTable excludeDates = assetData.Tables[2];
                        foreach (DataColumn column in assetDataTable.Columns)
                        {
                            if (column.ColumnName == "Date")
                                continue;
                            var matchingRows = paramDetails.AsEnumerable()
                                                           .Where(row => row.Field<string>("ParameterName") == column.ColumnName && row.Field<int>("AssetID") == assetID)
                                                           .ToList();
                            if (matchingRows.Count == 0)
                                continue;

                            var matchRow = matchingRows[0];

                            int frequencytypeid = 0;
                            int.TryParse(matchRow["Value"]?.ToString(), out frequencytypeid);
                            var IsDisplayDatapoints = Convert.ToBoolean(matchRow["is20Datapointsenable"]);
                            int nonNullCount = assetDataTable.AsEnumerable()
                                                             .Count(row => !row.IsNull(column.ColumnName));

                            DataTable filteredDataTable = new DataTable();
                            filteredDataTable.Columns.Add("D", typeof(DateTime));
                            filteredDataTable.Columns.Add("V", typeof(string));

                            var is20DataPoints = 0;
                            if (nonNullCount < 20 && frequencytypeid > 24 && IsDisplayDatapoints == true)
                            {
                                is20DataPoints = 1;
                                string singleParameterId = matchRow["AssetParameterID"].ToString();
                                DataTable result = GetParameterreadings_Aggration(assetID.ToString(), singleParameterId, /*fromDate, toDate,*/ tenantID);


                                //if (result.Rows.Count < 20)
                                //    continue;

                                foreach (DataRow row in result.Rows)
                                {
                                    if (!row.IsNull("LogDateTime") && !row.IsNull("Value") && !string.IsNullOrWhiteSpace(row["Value"].ToString()))
                                    {
                                        DataRow newRow = filteredDataTable.NewRow();
                                        newRow["D"] = Convert.ToDateTime(row["LogDateTime"]);
                                        newRow["V"] = row["Value"].ToString();
                                        filteredDataTable.Rows.Add(newRow);
                                    }
                                }
                            }
                            else
                            {
                                bool hasNonNullValues = assetDataTable.AsEnumerable()
                                                                     .Any(row => !row.IsNull(column.ColumnName));
                                if (!hasNonNullValues)
                                    continue;

                                foreach (var row in assetDataTable.AsEnumerable().Where(row => !row.IsNull(column.ColumnName)))
                                {
                                    if (!string.IsNullOrWhiteSpace(row[column.ColumnName]?.ToString()))
                                    {
                                        DataRow newRow = filteredDataTable.NewRow();
                                        newRow["D"] = row.Field<DateTime>("Date");
                                        newRow["V"] = row[column.ColumnName]?.ToString();
                                        filteredDataTable.Rows.Add(newRow);
                                    }
                                }
                            }
                            var ID = int.Parse(matchingRows[0]["ProcessCapabilityID"].ToString());
                            var paramID = int.Parse(matchingRows[0]["AssetParameterID"].ToString());
                            var lsl = double.Parse(matchingRows[0]["LCL"].ToString());
                            var usl = double.Parse(matchingRows[0]["UCL"].ToString());
                            var target = double.Parse(matchingRows[0]["Target"].ToString());
                            var tenantCode = matchingRows[0]["TenantCode"].ToString();
                            var subgroupSize = int.Parse(matchingRows[0]["SubgroupSize"].ToString());
                            var isFilter = int.Parse(matchingRows[0]["IsFilter"].ToString());
                            var datatypeID = int.Parse(matchingRows[0]["DatatypeID"].ToString());
                            var assetParameterCategoryId = int.Parse(matchingRows[0]["AssetParameterCategoryId"].ToString());

                            // Start the task without awaiting
                            tasks.Add(Task.Run(async () =>
                            {
                                await pc.loadProcessCapabilityDataAsync(
                                    filteredDataTable, pcSpecificationsData, excludeDates, tenantCode, assetID, paramID, int.Parse(tenantID), ID,
                                    fromDate, toDate, datatypeID, lsl, usl, target, isFilter, subgroupSize, assetParameterCategoryId, is20DataPoints);
                                await Task.Delay(2000);
                            }));
                        }
                    }
                }
                if (flag > 0)
                {
                    try
                    {
                        await Task.WhenAll(tasks).ConfigureAwait(false); // Prevent deadlock
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Exception while awaiting tasks: {ex.Message}");
                    }
                    await Task.Delay(10000); // Optional delay

                    DataTable dt;
                    if (checkmethod == 1)
                    {
                        dt = getMuliSelectionDatafromDB_New(parameterIds, tenantID, floorID, fromDate, toDate);
                    }
                    else if (checkmethod == 2)  // checkmethod == 2
                    {
                        dt = getmetricsDatafromDB_New(tenantID, floorID, fromDate, toDate);
                    }
                    else
                    {
                        throw new ArgumentException("Invalid checkmethod. Allowed values: 1 or 2.");
                    }
                    return dt;
                }

                return new DataTable(); // if flag == 0

            }
            catch (Exception ex)
            {
                return new DataTable();
            }
        }



        public DataTable GetParameterreadings_Aggration(string assetID, string parameterId, string tenantID)
        {
            DataTable result = new DataTable();

            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(@"SELECT TOP (20) * 
                                                 FROM Parameterreadings_Aggration  
                                                 WHERE AssetID = @AssetID  
                                                   AND AssetParameterID = @ParameterID  
                                                   AND TenantID = @TenantID  
                                                 ORDER BY LogDateTime DESC", conn))
                {
                    cmd.Parameters.AddWithValue("@AssetID", assetID);
                    cmd.Parameters.AddWithValue("@ParameterID", parameterId); // Single ID
                    cmd.Parameters.AddWithValue("@TenantID", tenantID);

                    using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                    {
                        adapter.Fill(result);
                    }
                }
            }

            return result;
        }


        public DataTable getmetricsDatafromDB_New(string tenantID, string floorID, string fromDate, string toDate)
        {
            var dt = new DataTable();
            using (var connection = new SqlConnection(ConnectionString))
            {
                using (var cmd = new SqlCommand())
                {
                    cmd.Connection = connection;
                    cmd.CommandText = @"
SELECT 
    d.DeviceID AS AssetID, d.DeviceName AS AssetName,ap.AssetParameterId,apc.ParameterCategory AS [Category],pcr.StDev_Within,
    pcr.StDev_Overall,CONCAT(ap.ParameterName, ' (', ap.DisplayText, ')') AS DisplayText,
    ap.DisplayText,pcr.SampleN_Pp AS Size,ap.ParameterName, 
    pcr.Cpk,

CASE 
        WHEN pcr.SampleN_Pp IS NOT  NULL THEN ISNULL(pcr.Cp, 0)
        ELSE NULL
    END AS Cp,
	CASE 
        WHEN pcr.SampleN_Pp IS NOT NULL THEN ISNULL(pcr.Ppk, 0)
        ELSE NULL
    END AS Ppk,
pcr.Pp,
    COALESCE(pcr.LCL, pcc.UpdatedLCL, pcc.LCL) AS LSL,COALESCE(pcr.UCL, pcc.UpdatedUCL, pcc.UCL) AS USL,
    COALESCE(pcr.Target, pcc.UpdatedTarget, pcc.Target) AS Target, 
pcr.SampleMean_Pp,
    pcr.SampleMean_Cp, COALESCE(pcr.UCL, pcc.UpdatedUCL, pcc.UCL) - COALESCE(pcr.LCL, pcc.UpdatedLCL, pcc.LCL) AS Tolerance,
    pcr.ProcessCapabilityID,pcr.ProcessCapabilityReadingsID,d.SortOrder,ap.SortOrder AS ParamSortOrder
FROM ProcessCapabilityConfiguration pcc 
JOIN AssetParameters ap ON pcc.AssetParameterID = ap.AssetParameterId
JOIN AssetParameterCategory apc  ON ap.AssetParameterCategoryId = apc.AssetParameterCategoryId
JOIN Devices d ON ap.AssetId = d.DeviceID
LEFT JOIN ProcessCapabilityReadings pcr ON pcr.ProcessCapabilityID = pcc.ProcessCapabilityID AND pcr.TenantID = @tenantID AND pcr.FromDate = @fromDate AND pcr.ToDate = @toDate
WHERE  d.FloorID = @floorID AND pcc.Statusid = 1 AND ap.StatusId = 1 AND d.StatusID = 1
ORDER BY d.SortOrder,ap.SortOrder";
                    cmd.Parameters.AddWithValue("@tenantID", tenantID);
                    cmd.Parameters.AddWithValue("@fromDate", fromDate);
                    cmd.Parameters.AddWithValue("@toDate", toDate);
                    cmd.Parameters.AddWithValue("@floorID", floorID);

                    using (var da = new SqlDataAdapter(cmd))
                    {
                        connection.Open();
                        da.Fill(dt);
                        return dt;
                    }
                }
            }
        }

        private async Task<string> loadProcessCapabilityDataAsync(
    DataTable dt, string tenantCode, int assetID, int paramID, int tenantID, int ID,
    string fromDate, string toDate, int datatypeID, double LSL, double USL,
    double Target, int isFilter, int subgroupSize)
        {
            var body = new
            {
                Data = dt,
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
                CreatedDate = DateTime.UtcNow.ToString("yyyy-MM-dd"),
                ProcessCapabilityID = ID
            };

            var client = _httpClientFactory.CreateClient();
            var uri = _configuration["ProcessCapabilityUrl"];
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



        private DataSet getAssetParamData_Manual(int tenantId, int assetId, int paramID, string timeInterval, string from, string to)
        {
            SqlCommand cmd = new SqlCommand("[dbo].[USP_GetAssetData_ProcessCapability]");
            var ds = new DataSet();
            try
            {
                cmd.Connection = new SqlConnection(ConnectionString);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@deviceid", assetId);
                cmd.Parameters.AddWithValue("@fromdt", from);
                cmd.Parameters.AddWithValue("@todt", to);
                cmd.Parameters.AddWithValue("@tenantId", tenantId);
                cmd.Parameters.AddWithValue("@AssetParameterId", paramID);

                var da1 = new SqlDataAdapter { SelectCommand = cmd };
                if (cmd.Connection != null && cmd.Connection.State == ConnectionState.Closed)
                    cmd.Connection.Open();
                da1.Fill(ds);
                return ds;
            }
            catch (Exception ex)
            {
                return ds;
                throw ex;
            }
            finally
            {
                cmd.Connection.Close();
            }
        }


        private DataTable getProcessCapabilityParamsByFloor(string tenantID, string floorID)
        {
            var data = new DataTable();
            string query = @"
            SELECT pc.ProcessCapabilityID, 
       pc.AssetID,
       ap.AssetParameterCategoryId,
       pc.AssetParameterID,
       ref.ParameterName,
       pc.LCL,
       pc.UCL,
       pc.Target,
       pc.TimeInterval,
       pc.DatatypeID,
       pc.TenantID,
       pc.LastRunAt,
       t.TenantCode,
       pc.SubgroupSize,
       pc.IsFilter,
     pc.MeasurementFrequencyLookupID,
pmfl.Name,
pmfl.Value,
alpo.is20Datapointsenable
FROM ProcessCapabilityConfiguration pc 
JOIN Devices d 
    ON d.DeviceID = pc.AssetID 
    AND pc.TenantID = d.TenantID 
JOIN AssetParameters ap 
    ON ap.AssetParameterId = pc.AssetParameterID 
    AND ap.AssetId = pc.AssetID 
    AND ap.TenantId = pc.TenantID 
JOIN em.ReferenceParameters ref 
    ON ref.ReferenceParameterID = ap.ReferenceParameterId 
JOIN TenantDetails t 
    ON t.TenantDetailsID = pc.TenantID  -- Add the join for the Tenant table
 left Join  ParameterMeasurementFrequencyLookup pmfl
 on pc.MeasurementFrequencyLookupID = pmfl.MeasurementFrequencyLookupID
 join Floor f on  d.FloorID = f.FloorID
 join Building b on b.BuildingID=f.BuildingID and f.Tenantid=B.TenantID
LEFT JOIN AspenLoadingPlantOrder alpo 
   ON b.BuildingID = alpo.BuildingID
WHERE d.FloorID = " + floorID + "  AND pc.TenantID = " + tenantID + " AND pc.StatusId = 1 AND d.StatusID = 1 AND ap.StatusId = 1 and pc.LCL is not null and pc.UCL is not null and pc.Target is not null ORDER BY pc.AssetParameterID";
            using (var conn = new SqlConnection(ConnectionString))
            using (var cmd = new SqlCommand(query, conn))
            {
                cmd.CommandType = CommandType.Text;

                try
                {
                    var da1 = new SqlDataAdapter { SelectCommand = cmd };
                    if (cmd.Connection != null && cmd.Connection.State == ConnectionState.Closed)
                        cmd.Connection.Open();
                    da1.Fill(data);
                    return data;
                }
                catch (Exception ex)
                {
                    return data;
                    throw ex;
                }
                finally
                {
                    cmd.Connection.Close();
                }
            }
        }
        //PS END

        [HttpGet]

        public IActionResult GetMetricsImage(string tenantID, string pcrID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = @"
        SELECT ProcessCapabilityReadingsID,ProcessCapabilityID,LCL,UCL,Target,ImgSRC FROM ProcessCapabilityReadings WHERE ProcessCapabilityReadingsID = @pcrID  AND TenantID = @TenantID";

                        cmd.Parameters.AddWithValue("@pcrID", pcrID);
                        cmd.Parameters.AddWithValue("@TenantID", tenantID);

                        using (var da = new SqlDataAdapter(cmd))
                        {
                            connection.Open();
                            da.Fill(dt);
                        }
                    }
                }

                return Ok(dt);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }

        [HttpPost("getExcludeData")]
        public IActionResult getExcludeData(string tenantID, string paramID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = @"
                         SELECT  FromDate,ToDate,Reason FROM  ProcessCapabiltyExcludedDates  WHERE ProcessCapabilityID =  @paramID ";

                        cmd.Parameters.AddWithValue("@paramID", paramID);

                        using (var da = new SqlDataAdapter(cmd))
                        {
                            connection.Open();
                            da.Fill(dt);
                        }
                    }
                }

                return Ok(dt);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }
        [HttpPost("getDecimalCount")]
        public IActionResult getDecimalCount(string tenantID, string assetParameterID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = @"
select AssetParameterID,DecimalCount from ProcessCapabilityConfiguration where AssetParameterID=@AssetParameterID";


                        cmd.Parameters.AddWithValue("@AssetParameterID", assetParameterID);

                        using (var da = new SqlDataAdapter(cmd))
                        {
                            connection.Open();
                            da.Fill(dt);
                        }
                    }
                }

                return Ok(dt);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpPost("getReportExcludeData")]
        public IActionResult getReportExcludeData(string tenantID, string assetId, string paramID, string fromDate, string toDate)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                using (var cmd = new SqlCommand(@"
        SELECT ex.FromDate, ex.ToDate, ex.ProcessCapabilityID, ex.Reason,
               ex.AssetParameterCategoryID, ex.AssetID, ex.floorID
        FROM ProcessCapabiltyExcludedDates ex
        WHERE ex.FloorID = @floorID
          AND ex.StatusID = 1
          AND (ex.ProcessCapabilityID IS NULL OR ex.ProcessCapabilityID = @pID)
          AND (ex.AssetParameterCategoryID IS NULL OR ex.AssetParameterCategoryID = @apcID)
          AND (ex.AssetID IS NULL OR ex.AssetID = @assetId)
          AND ex.TenantId = @tenantID
          AND (
                (ex.FromDate BETWEEN @fromDate AND @toDate)
                OR (ex.ToDate BETWEEN @fromDate AND @toDate)
                OR (@fromDate BETWEEN ex.FromDate AND ex.ToDate)
                OR (@toDate BETWEEN ex.FromDate AND ex.ToDate)
              );", connection))
                {
                    cmd.Parameters.AddWithValue("@paramID", paramID);
                    cmd.Parameters.AddWithValue("@tenantID", tenantID);
                    cmd.Parameters.AddWithValue("@fromDate", fromDate);
                    cmd.Parameters.AddWithValue("@toDate", toDate);
                    cmd.Parameters.AddWithValue("@assetId", assetId);

                    using (var da = new SqlDataAdapter(cmd))
                    {
                        connection.Open();
                        da.Fill(dt);
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }

            return Ok(dt);
        }


        [HttpPost("getActiveItemData")]
        public IActionResult getActiveItemData(string tenantId, string id)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = @"
                  SELECT * FROM ProcessCapabilityReadings WHERE ProcessCapabilityReadingsID = @id ";

                        cmd.Parameters.AddWithValue("@id", id);

                        using (var da = new SqlDataAdapter(cmd))
                        {
                            connection.Open();
                            da.Fill(dt);
                        }
                    }
                }

                return Ok(dt);
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message); // Handle exceptions
            }
        }


    }
    public class ProcessCapability
    {
        public string parameter { get; set; }
        public object value { get; set; }
    }

    public class DataTableConverter
    {
        public Dictionary<string, string> ConvertDataTableToColumnWiseJson(DataTable dt)
        {
            var resultDictionary = new Dictionary<string, List<Dictionary<string, object>>>();

            foreach (DataColumn column in dt.Columns)
            {
                if (column.Ordinal == 0) continue; // Skip the first column (DateTime)

                // Initialize a list for each tag column
                resultDictionary[column.ColumnName] = new List<Dictionary<string, object>>();
            }

            // Loop through each row in the DataTable
            foreach (DataRow row in dt.Rows)
            {
                // Extract the DateTime from the first column
                string dateTime = row[0].ToString();

                // Loop through each column starting from the second column
                for (int i = 1; i < dt.Columns.Count; i++)
                {
                    string columnName = dt.Columns[i].ColumnName;

                    // Create an entry for the current date and tag value
                    var tagObject = new Dictionary<string, object>
                {
                    { "D", dateTime },   // DateTime value
                    { "V", row[i] }      // Tag value
                };

                    // Add the object to the corresponding column's list
                    resultDictionary[columnName].Add(tagObject);
                }
            }

            // Serialize each column list to JSON
            var jsonResult = new Dictionary<string, string>();
            foreach (var entry in resultDictionary)
            {
                jsonResult[entry.Key] = JsonConvert.SerializeObject(entry.Value, Formatting.Indented);
            }

            return jsonResult;
        }
        
    }
    
    public class Metric
    {
        public string Parameter { get; set; }
        public string Value { get; set; } // Use nullable double to handle null values
    }
    public class Response
    {
        public List<Metric> Metrics { get; set; }
        public string Status { get; set; }
    }
    public class PCParams
    {
        public string TenantID { get; set; }
        public string AssetID { get; set; }
        public string JSONData { get; set; }
    }
   
}