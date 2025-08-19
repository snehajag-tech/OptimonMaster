using Azure;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using Microsoft.Data.SqlClient;
using static SPCCoreMigration.Controllers.ProcessCapabilityController;
using SPCCoreMigration.Models;


namespace SPCCoreMigration.Controllers
{
    [ApiController]
    [Route("api/ProcessCapability")]
    public class ProcessCapabilityConfigurationController : Controller
    {
        // GET: ProcessCapabilityConfiguration
        private static TimeZoneInfo INDIAN_ZONE = TimeZoneInfo.FindSystemTimeZoneById("India Standard Time");
        private readonly IConfiguration _configuration;

        private string ConnectionString => _configuration.GetConnectionString("DefaultConnection") ?? throw new InvalidOperationException("Connection string 'DBConnStr' not found.");

        public ProcessCapabilityConfigurationController(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        [HttpPost]
        public IActionResult loadUploadedProcessCapabilityData(PCParams pcs)//int tenantId, int assetId
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
                        // Sort rows based on the first column (assumed to be a date) and get fromDate and toDate
                        var sortedRows = dt1.AsEnumerable()
                                                  .OrderBy(row => Convert.ToDateTime(row[0]))
                                                  .ToList();

                        DateTime fromDate = Convert.ToDateTime(sortedRows.First()[0]);
                        DateTime toDate = Convert.ToDateTime(sortedRows.Last()[0]);

                        string fromDt = fromDate.ToString("yyyy-MM-dd");
                        string toDt = toDate.ToString("yyyy-MM-dd");
                        string parameterName = kvp.Key;
                        int tenantId = Convert.ToInt32(pcs.TenantID);

                        // Fetch process capability data
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

                            // Process filtered data
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
               
                return base.Ok("Sucess");
            }
            catch (Exception ex)
            {
                throw ex;
            }
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

                        var checkExistenceCommand = @"
                   select *from ProcessCapabilityReadings  WHERE ProcessCapabilityID = @ProcessCapabilityID ";

                        using (var checkCmd = new SqlCommand(checkExistenceCommand, sqlConnection))
                        {
                            checkCmd.Parameters.AddWithValue("@ProcessCapabilityID", ID);
                            var exists = Convert.ToInt32(checkCmd.ExecuteScalar()) > 0;

                            if (exists)
                            {
                                var updateCommand = @"
                            UPDATE ProcessCapabilityReadings
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
                                var insertCommand = @"
                            INSERT INTO ProcessCapabilityReadings
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
            catch (Exception ex)
            {

                return new DataSet();
                throw ex;
            }

            return ds;
        }
        [HttpGet]
        public IActionResult GetProcessCapabilityData(int AssetID, int TenantID)
        {
            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"SELECT 
    PCC.ProcessCapabilityID,
    AP.ParameterName,
    CAST(
        CASE 
            WHEN PCC.UpdatedLCL IS NOT NULL AND LTRIM(RTRIM(PCC.UpdatedLCL)) <> '' 
                THEN PCC.UpdatedLCL 
            ELSE PCC.LCL 
        END AS DECIMAL(18, 6)
    ) AS LCL,
    CAST(
        CASE 
            WHEN PCC.UpdatedLCL IS NOT NULL AND LTRIM(RTRIM(PCC.UpdatedLCL)) <> '' 
                THEN PCC.UpdatedUCL 
            ELSE PCC.UCL 
        END AS DECIMAL(18, 6)
    ) AS UCL,
    CAST(
        CASE 
            WHEN PCC.UpdatedLCL IS NOT NULL AND LTRIM(RTRIM(PCC.UpdatedLCL)) <> '' 
                THEN PCC.UpdatedTarget 
            ELSE PCC.Target 
        END AS DECIMAL(18, 6)
    ) AS Target,
    CAST(
        CASE 
            WHEN PCC.UpdatedLCL IS NOT NULL AND LTRIM(RTRIM(PCC.UpdatedLCL)) <> '' 
                THEN PCC.UpdatedTolerance 
            ELSE PCC.Tolerance 
        END AS DECIMAL(18, 6)
    ) AS Tolerance,
    PCDT.Type,
    PCC.StatusId,
    PCC.IsJobRun,
    PCC.JobInterval,
    PCC.TimeInterval,
    PCC.ProcessName,
    PCC.ProcessID,
    PCC.SubgroupSize,
    PCC.DecimalCount,
    PCC.IsFilter,
    PCC.CreatedBy,
    PCC.Comment,
    PCC.CreatedDate,
    PCC.ModifiedBy,
    PCC.ModifiedDate,
    PCC.Min,
    PCC.Max,
    PCC.UpdatedLCL,
    PCC.UpdatedUCL,
    PCC.UpdatedTarget,
    PCC.UpdatedTolerance,
    PMF.Name,
    AP.DisplayText,
    AP.SortOrder,
    AP.AssetParameterID,
    AP.AssetParameterCategoryID,
    AP.Units,
    APC.ParameterCategory
FROM ProcessCapabilityConfiguration PCC
JOIN AssetParameters AP 
    ON PCC.AssetParameterId = AP.AssetParameterId
    AND PCC.AssetID = AP.AssetID
    AND PCC.TenantID = AP.TenantID
JOIN ProcessCapabilityDataTypeLookup PCDT 
    ON PCC.DataTypeID = PCDT.DataTypeID
JOIN AssetParameterCategory APC 
    ON AP.AssetParameterCategoryId = APC.AssetParameterCategoryID
LEFT JOIN ParameterMeasurementFrequencyLookup PMF
    ON PCC.MeasurementFrequencyLookupID = PMF.MeasurementFrequencyLookupID
WHERE AP.AssetID = @AssetID 
  AND AP.TenantID = @TenantID 
  AND AP.StatusID IN (1, 2)
  AND PCC.StatusId IN (1, 2)
ORDER BY AP.SortOrder;";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@AssetID", AssetID);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);
                        _con.Open();

                        DataTable dt = new DataTable();
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(dt);
                        }

                        // Return the DataTable as JSON response
                        return Ok(dt);
                    }
                }
            }
            catch (Exception ex)
            {
                // Log or handle the exception
                return StatusCode(500, ex.Message);
            }
        }
        [HttpGet]
        public IActionResult GetProcessCapabilityExcludeData(int AssetID, int TenantID)
        {
            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"
                SELECT apc.ParameterCategory, pce.ProcessCapabiltyExcludedDatesID, pce.FromDate, pce.ToDate, pce.Reason, pce.CreatedBy, 
                       pce.CreatedDate, pce.StatusID 
                FROM ProcessCapabiltyExcludedDates pce 
                LEFT JOIN AssetParameterCategory apc ON apc.AssetParameterCategoryID = pce.AssetParameterCategoryID 
                WHERE pce.TenantID = @TenantID 
                  AND pce.AssetID = @AssetID 
                  AND pce.ProcessCapabilityID IS NULL 
                  AND pce.StatusID = 1 
                ORDER BY pce.FromDate DESC;";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@AssetID", AssetID);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);
                        _con.Open();

                        DataTable dt = new DataTable();
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(dt);
                        }

                        // Return the DataTable as JSON response
                        return Ok(dt);
                    }
                }
            }
            catch (Exception ex)
            {
                // Log or handle the exception
                return StatusCode(500, ex.Message);
            }
        }
        [HttpGet]
        public IActionResult GetParameterData(int AssetID, int TenantID)
        {
            try
            {
                var parameterData = new List<KeyValuePair<int, string>>();
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"
                SELECT AP.AssetParameterID, AP.ParameterName 
                FROM AssetParameters AP 
                WHERE AP.AssetId = @AssetID 
                  AND AP.TenantID = @TenantID
                  AND AP.StatusId = 1 
                  AND AP.ParameterName NOT IN (
                      SELECT AP2.ParameterName 
                      FROM AssetParameters AP2 
                      JOIN ProcessCapabilityConfiguration PCC 
                      ON AP2.AssetParameterID = PCC.AssetParameterID where PCC.AssetId = @AssetID 
                  AND PCC.TenantID = @TenantID AND  PCC.statusId in (1,2)
                  );";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@AssetID", AssetID);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);
                        _con.Open();

                        // Create a DataSet to hold the results
                        DataSet dt = new DataSet();

                        // Use SqlDataAdapter to execute the command and fill the DataSet
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(dt);
                        }

                        // Check the DataTable has rows before processing
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

                    // Return the parameter data as a response
                    return Ok(parameterData);
                }
            }
            catch (Exception ex)
            {
                // Return a bad request response in case of an exception
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet]
        public IActionResult GetDisplayTextByAssetParameter(int AssetID, int AssetParameterID, int TenantID)
        {
            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"SELECT DisplayText, ParameterCategory, ap.SortOrder, apc.AssetParameterCategoryId, Units 
                             FROM AssetParameters ap
                             LEFT JOIN Assetparametercategory apc ON ap.AssetParameterCategoryID = apc.AssetParameterCategoryID
                             WHERE AssetID = @AssetID AND AssetParameterID = @AssetParameterID AND TenantID = @TenantID";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@AssetID", AssetID);
                        cmd.Parameters.AddWithValue("@AssetParameterID", AssetParameterID);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);
                        _con.Open();

                        // Create a DataTable to hold the results
                        DataTable dt = new DataTable();

                        // Use SqlDataAdapter to execute the command and fill the DataTable
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(dt);
                        }

                        // Return the DataTable as JSON response
                        return Ok(dt);
                    }
                }
            }
            catch (Exception ex)
            {
                // Log or handle the exception
                return StatusCode(500, ex.Message);
            }
        }
        [HttpGet]
        [Route("GetDataType")]
        public IActionResult GetDataType()
        {
            try
            {
                using (SqlConnection con = new SqlConnection(ConnectionString))
                {
                    con.Open();
                    var dataTypeList = new List<object>(); // Create a list to hold the data

                    string query = @"
                SELECT 
                    DataTypeID,
                    Type 
                FROM ProcessCapabilityDataTypeLookup
                WHERE StatusID IN (1);";

                    using (SqlCommand cmd = new SqlCommand(query, con))
                    {
                        using (SqlDataReader reader = cmd.ExecuteReader())
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
                    }

                    // Create a JSON response with success flag and data
                    var response = new
                    {
                        success = true,
                        data = dataTypeList
                    };

                    // Return the list as a JSON-formatted response
                    return Ok(response);
                }
            }
            catch (Exception ex)
            {
                // Create error response in case of an exception
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet]
        [Route("GetMeasurementFrequencyLookup")]
        public IActionResult GetMeasurementFrequencyLookup()
        {
            try
            {
                using (SqlConnection con = new SqlConnection(ConnectionString))
                {
                    con.Open();
                    var dataTypeList = new List<object>(); // Create a list to hold the data

                    string query = @"
                SELECT 
                    Name,
                    MeasurementFrequencyLookupID 
                FROM ParameterMeasurementFrequencyLookup
                WHERE StatusID = 1;";

                    using (SqlCommand cmd = new SqlCommand(query, con))
                    {
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                dataTypeList.Add(new
                                {
                                    MeasurementFrequencyLookupID = reader["MeasurementFrequencyLookupID"],
                                    Name = reader["Name"]
                                });
                            }
                        }
                    }

                    // Create a JSON response with success flag and data
                    var response = new
                    {
                        success = true,
                        data = dataTypeList
                    };

                    // Return the list as a JSON-formatted response
                    return Ok(response);
                }
            }
            catch (Exception ex)
            {
                // Create error response in case of an exception
                return StatusCode(500, ex.Message);
            }
        }

        [HttpPost]
        [Route("ValidateDisplayText")]
        public IActionResult ValidateDisplayText(List<ProcessCapabilityConfigurationViewModel> model)
        {
            var results = new List<object>();

            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    foreach (var item in model)
                    {
                        using (var cmd = new SqlCommand("CheckDisplayTextExistence", connection))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;

                            cmd.Parameters.AddWithValue("@AssetId", item.AssetID);
                            cmd.Parameters.AddWithValue("@ParameterName", item.ParameterName);
                            cmd.Parameters.AddWithValue("@DisplayText", item.DisplayText);
                            cmd.Parameters.AddWithValue("@TenantId", item.TenantID);

                            using (var reader = cmd.ExecuteReader())
                            {
                                if (reader.Read())
                                {
                                    results.Add(new
                                    {
                                        DisplayText = item.DisplayText,
                                        ParameterName = item.ParameterName,
                                        CountDisplayText = reader.GetInt32(reader.GetOrdinal("CountDisplayText")),
                                        CountParameterName = reader.GetInt32(reader.GetOrdinal("CountParameterName")),
                                        CountBothMatch = reader.GetInt32(reader.GetOrdinal("CountBothMatch"))
                                    });
                                }
                            }
                        }
                    }
                }

                return Ok(new { success = true, data = results });
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Server Error: {ex.Message}");
            }
        }


        [HttpPost]
        [Route("AddProcessCapabilityData")]
        public IActionResult AddProcessCapabilityData(List<ProcessCapabilityConfigurationViewModel> model, string userId)
        {
            var responseList = new List<object>();
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
                        using (var cmd = new SqlCommand("SaveProcessCapabilityData", connection))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;

                            cmd.Parameters.AddWithValue("@AssetID", item.AssetID);
                            cmd.Parameters.AddWithValue("@AssetParameterID", item.AssetParameterID);
                            cmd.Parameters.AddWithValue("@ParameterName", item.ParameterName);
                            cmd.Parameters.AddWithValue("@DisplayText", item.DisplayText);
                            cmd.Parameters.AddWithValue("@AssetParameterCategoryId", item.AssetParameterCategoryId);
                            cmd.Parameters.AddWithValue("@Units", item.Units);
                            cmd.Parameters.AddWithValue("@SortOrder", item.SortOrder);
                            cmd.Parameters.AddWithValue("@LCL", item.LCL);
                            cmd.Parameters.AddWithValue("@UCL", item.UCL);
                            cmd.Parameters.AddWithValue("@Target", item.Target);
                            cmd.Parameters.AddWithValue("@Tolerance", item.Tolerance);
                            cmd.Parameters.AddWithValue("@MeasurementFrequencyLookupID", item.MeasurementFrequencyLookupID);
                            cmd.Parameters.AddWithValue("@DataTypeID", item.DatatypeID);
                            cmd.Parameters.AddWithValue("@TenantID", item.TenantID);
                            cmd.Parameters.AddWithValue("@TimeInterval", 1);
                            cmd.Parameters.AddWithValue("@ProcessID", item.ProcessID ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("@ProcessName", item.ProcessName ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("@IsJobRun", item.IsJobRun ?? 1);
                            cmd.Parameters.AddWithValue("@JobInterval", item.JobInterval ?? 1);
                            cmd.Parameters.AddWithValue("@SubgroupSize", item.SubgroupSize);
                            cmd.Parameters.AddWithValue("@DecimalCount", item.DecimalCount);
                            cmd.Parameters.AddWithValue("@IsFilter", item.IsFilter ?? 0);
                            cmd.Parameters.AddWithValue("@CreatedBy", item.CreatedBy);
                            cmd.Parameters.AddWithValue("@CreatedDate", item.CreatedDate);
                            cmd.Parameters.AddWithValue("@StatusID", item.StatusID);
                            cmd.Parameters.AddWithValue("@Comment", string.IsNullOrEmpty(item.Comment) ? (object)DBNull.Value : item.Comment);

                            using (var reader = cmd.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    responseList.Add("Process Capability Data Added Successfully");
                                }
                            }
                        }
                    }
                }

                return Ok(new { success = true, message = "Process Capability Data Added Successfully", data = responseList });
            }
            catch (SqlException sqlEx)
            {
                return Ok(new
                {
                    success = false,
                    message = sqlEx.Message,
                    data = responseList
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Server Error: {ex.Message}");
            }
        }


        [HttpPost]
        [Route("UpdateProcessCapabilityData")]
        public IActionResult UpdateProcessCapabilityData(List<ProcessCapabilityConfigurationViewModel> model, string userId)
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
                        string updateProcessCapabilityQuery = "UPDATE ProcessCapabilityConfiguration SET [LCL] = @LCL, [UCL] = @UCL, [Target] = @Target,[Tolerance] = @Tolerance,[DatatypeID] = @DatatypeID, " +
                                                              "[SubgroupSize] = @SubgroupSize,[DecimalCount] = @DecimalCount, [Comment] = @Comment, [ModifiedBy] = @ModifiedBy, [ModifiedDate] = @ModifiedDate, [StatusID] = @StatusID,[Min] = @Min,[Max] = @Max,[MeasurementFrequencyLookupID] = @MeasurementFrequencyLookupID " +
                                                              "WHERE [ProcessCapabilityID] = @ProcessCapabilityID AND [TenantID] = @TenantID";

                        using (var cmd = new SqlCommand(updateProcessCapabilityQuery, connection))
                        {
                            cmd.Parameters.AddWithValue("@ProcessCapabilityID", item.ProcessCapabilityID);
                            cmd.Parameters.AddWithValue("@LCL", item.LCL);
                            cmd.Parameters.AddWithValue("@UCL", item.UCL);
                            cmd.Parameters.AddWithValue("@Target", item.Target);
                            cmd.Parameters.AddWithValue("@Tolerance", item.Tolerance);
                            cmd.Parameters.AddWithValue("@DatatypeID", item.DatatypeID);
                            cmd.Parameters.AddWithValue("@StatusID", item.StatusID);
                            cmd.Parameters.AddWithValue("@SubgroupSize", item.SubgroupSize ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("@DecimalCount", item.DecimalCount ?? (object)DBNull.Value);
                            //cmd.Parameters.AddWithValue("@IsFilter", item.IsFilter);
                            cmd.Parameters.AddWithValue("@TenantID", item.TenantID);
                            cmd.Parameters.AddWithValue("@ModifiedBy", item.ModifiedBy);
                            cmd.Parameters.AddWithValue("@ModifiedDate", item.ModifiedDate);
                            cmd.Parameters.AddWithValue("@Comment", item.Comment ?? string.Empty);
                            cmd.Parameters.AddWithValue("@Min", item.Min ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("@Max", item.Max ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("@MeasurementFrequencyLookupID", item.MeasurementFrequencyLookupID ?? (object)DBNull.Value);

                            int rowsAffected = cmd.ExecuteNonQuery();
                            if (rowsAffected == 0)
                            {
                                return BadRequest("Failed to Update Process Capability Data.");
                            }
                        }

                        string sortOrderQuery = @"SELECT @SortOrder = ISNULL(MAX(SortOrder), 0) + 1 
                                          FROM AssetParameters WHERE AssetId = @AssetId and TenantId = @TenantID";

                        if (item.SortOrder == null)
                        {
                            using (var sortOrderCmd = new SqlCommand(sortOrderQuery, connection))
                            {
                                sortOrderCmd.Parameters.AddWithValue("@AssetId", item.AssetID);
                                sortOrderCmd.Parameters.AddWithValue("@TenantID", item.TenantID);

                                SqlParameter outputParam = new SqlParameter("@SortOrder", SqlDbType.Int);
                                outputParam.Direction = ParameterDirection.Output;
                                sortOrderCmd.Parameters.Add(outputParam);

                                sortOrderCmd.ExecuteNonQuery();

                                item.SortOrder = (int)outputParam.Value;
                            }
                        }
                        string updateAssetParametersQuery = @"
                                        UPDATE AssetParameters
                                            SET ParameterName = @ParameterName,
                                            ParameterCode = @ParameterCode,
                                            DisplayText = @DisplayText,
                                            SortOrder = @SortOrder,
                                            AssetParameterCategoryID = @AssetParameterCategoryID,
                                            Units = @Units
                                            WHERE AssetParameterID = @AssetParameterID";

                        using (var assetCmd = new SqlCommand(updateAssetParametersQuery, connection))
                        {
                            assetCmd.Parameters.AddWithValue("@ParameterName", item.ParameterName);
                            assetCmd.Parameters.AddWithValue("@ParameterCode", item.ParameterName);
                            assetCmd.Parameters.AddWithValue("@DisplayText", item.DisplayText);
                            assetCmd.Parameters.AddWithValue("@AssetParameterID", item.AssetParameterID);
                            assetCmd.Parameters.AddWithValue("@AssetParameterCategoryID", item.AssetParameterCategoryId);
                            assetCmd.Parameters.AddWithValue("@Units", item.Units ?? (object)DBNull.Value);
                            assetCmd.Parameters.AddWithValue("@SortOrder", item.SortOrder);

                            assetCmd.ExecuteNonQuery();
                        }
                    }

                    return Ok("Process Capability Data Updated Successfully.");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }


        [HttpGet]
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
                // Handle exceptions and log if necessary
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }



        [HttpGet]
        public IActionResult GetAssetParameterCategory(int AssetID, int TenantID)
        {
            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"SELECT ParameterCategory, AssetParameterCategoryID  
                             FROM AssetParameterCategory 
                             WHERE AssetCategoryID in 
                             (SELECT DeviceCategoryID from Devices where DeviceID=@AssetID and TenantID=@TenantID)";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@AssetID", AssetID);
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

        [HttpGet]
        public IActionResult GetExcludeDateData(int ProcessCapabilityID, int TenantID, int AssetID)
        {
            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"SELECT ProcessCapabiltyExcludedDatesID, FromDate, ToDate, Reason, CreatedBy, CreatedDate,StatusID
                             FROM ProcessCapabiltyExcludedDates 
                             WHERE (@ProcessCapabilityID IS NULL OR ProcessCapabilityID = @ProcessCapabilityID) 
                             AND TenantID = @TenantID 
                             AND AssetID = @AssetID 
                             AND StatusId = 1 
                             ORDER BY CreatedDate DESC;";

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


        [HttpPost]
        [Route("AddOnProcessCapabiltyExcludedDateData")]
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
                        int? AssetParameterCategoryId = item.AssetParameterCategoryId;

                        string query = "INSERT INTO ProcessCapabiltyExcludedDates " +
                                       "(AssetID, FromDate, ToDate, Reason, TenantID, CreatedBy, CreatedDate, StatusID, FloorID, BuildingID, LocationID, AssetParameterCategoryId) " +
                                       "VALUES (@AssetID, @FromDate, @ToDate, @Reason, @TenantID, @CreatedBy, @CreatedDate, @StatusID, @FloorID, @BuildingID, @LocationID, @AssetParameterCategoryId)";

                        using (var cmd = new SqlCommand(query, connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.Parameters.AddWithValue("@AssetID", AssetID);
                            cmd.Parameters.AddWithValue("@FromDate", FromDate);
                            cmd.Parameters.AddWithValue("@ToDate", ToDate);
                            cmd.Parameters.AddWithValue("@Reason", (object)Reason ?? DBNull.Value);
                            cmd.Parameters.AddWithValue("@TenantID", item.TenantID);
                            cmd.Parameters.AddWithValue("@AssetParameterCategoryId", (object)AssetParameterCategoryId ?? DBNull.Value);
                            cmd.Parameters.AddWithValue("@CreatedBy", item.CreatedBy);
                            cmd.Parameters.AddWithValue("@CreatedDate", item.CreatedDate);
                            cmd.Parameters.AddWithValue("@StatusID", StatusID);
                            cmd.Parameters.AddWithValue("@FloorID", item.FloorID);
                            cmd.Parameters.AddWithValue("@BuildingID", item.BuildingID);
                            cmd.Parameters.AddWithValue("@LocationID", item.LocationID);

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

        [HttpGet]
        public IActionResult GetDeleteProcessCapabilityExcludedData(int ProcessCapabilityExcludedDatesID, int TenantID, string UserId)
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

                    // Perform the delete operation (soft delete)
                    using (var cmd = new SqlCommand("UPDATE ProcessCapabiltyExcludedDates SET [StatusID] = 3 WHERE [ProcessCapabilityExcludedDatesID] = @ProcessCapabilityExcludedDatesID AND [TenantID] = @TenantID", connection))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@ProcessCapabilityExcludedDatesID", ProcessCapabilityExcludedDatesID);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);

                        int rowsAffected = cmd.ExecuteNonQuery();

                        if (rowsAffected == 0)
                        {
                            return BadRequest("Failed to delete Process Capability data.");
                        }
                    }

                    return Ok("Process Capability Excluded data deleted successfully.");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }

        [HttpPost]
        [Route("AddProcessCapabiltyExcludedDateData")]
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
                    (ProcessCapabilityID, FromDate, ToDate, Reason, TenantID, CreatedBy, CreatedDate, StatusID, AssetID, FloorID, BuildingID, LocationID) 
                    VALUES 
                    (@ProcessCapabilityID, @FromDate, @ToDate, @Reason, @TenantID, @CreatedBy, @CreatedDate, @StatusID, @AssetID, @FloorID, @BuildingID, @LocationID)";

                        using (var cmd = new SqlCommand(query, connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.Parameters.AddWithValue("@ProcessCapabilityID", ProcessCapabilityID);
                            cmd.Parameters.AddWithValue("@FromDate", FromDate);
                            cmd.Parameters.AddWithValue("@ToDate", ToDate);
                            cmd.Parameters.AddWithValue("@Reason", (object)Reason ?? DBNull.Value);
                            cmd.Parameters.AddWithValue("@TenantID", item.TenantID);
                            cmd.Parameters.AddWithValue("@CreatedBy", item.CreatedBy);
                            cmd.Parameters.AddWithValue("@CreatedDate", item.CreatedDate);
                            cmd.Parameters.AddWithValue("@StatusID", StatusID);
                            cmd.Parameters.AddWithValue("@AssetID", AssetID);
                            cmd.Parameters.AddWithValue("@FloorID", item.FloorID);
                            cmd.Parameters.AddWithValue("@BuildingID", item.BuildingID);
                            cmd.Parameters.AddWithValue("@LocationID", item.LocationID);

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

        [HttpGet]
        [Route("GetAuditLogData")]
        public async Task<IActionResult> GetAuditLogData(int TenantID, int AssetID, int ProcessCapabilityID)
        {
            try
            {
                if (TenantID == 0 || ProcessCapabilityID <= 0)
                {
                    return BadRequest("Invalid input parameters.");
                }

                string query = @"SELECT AuditLogID, TenantID, TableName, OperationType, RecordID, OldValue, NewValue, ChangedBy, ChangedDate 
                         FROM AuditLog WHERE TenantID = @TenantID";

                List<AuditlogViewModel> result = new List<AuditlogViewModel>();

                using (var connection = new SqlConnection(ConnectionString))
                {
                    await connection.OpenAsync();

                    List<dynamic> cachedRows = new List<dynamic>();
                    using (var cmd = new SqlCommand(query, connection))
                    {
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);

                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                cachedRows.Add(new
                                {
                                    AuditLogID = Convert.ToInt32(reader["AuditLogID"]),
                                    TableName = reader["TableName"] as string,
                                    OperationType = reader["OperationType"] as string,
                                    ChangedBy = reader["ChangedBy"] as string,
                                    ChangedDate = Convert.ToDateTime(reader["ChangedDate"]),
                                    OldValue = reader["OldValue"] as string,
                                    NewValue = reader["NewValue"] as string
                                });
                            }
                        }
                    }

                    foreach (var row in cachedRows)
                    {
                        var oldValue = !string.IsNullOrEmpty(row.OldValue)
                            ? JsonConvert.DeserializeObject<Dictionary<string, object>>(row.OldValue)
                            : null;

                        var newValue = !string.IsNullOrEmpty(row.NewValue)
                            ? JsonConvert.DeserializeObject<Dictionary<string, object>>(row.NewValue)
                            : null;

                        bool matchFound = false;

                        if (newValue != null && newValue.ContainsKey("ProcessCapabilityID") &&
                            Convert.ToInt32(newValue["ProcessCapabilityID"]) == ProcessCapabilityID)
                        {
                            matchFound = true;
                        }
                        else if (oldValue != null && oldValue.ContainsKey("ProcessCapabilityID") &&
                                 Convert.ToInt32(oldValue["ProcessCapabilityID"]) == ProcessCapabilityID)
                        {
                            matchFound = true;
                        }

                        if (matchFound)
                        {
                            string parameterName = await GetParameterNameByProcessCapabilityID(ProcessCapabilityID, connection);

                            result.Add(new AuditlogViewModel
                            {
                                AuditLogID = row.AuditLogID,
                                TableName = row.TableName,
                                OperationType = row.OperationType,
                                ChangedBy = row.ChangedBy,
                                ChangedDate = row.ChangedDate,
                                OldValue = oldValue,
                                NewValue = newValue,
                                ParameterName = parameterName
                            });
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

        public async Task<string> GetParameterNameByProcessCapabilityID(int processCapabilityID, SqlConnection connection)
        {
            string query = @"SELECT TOP 1 ParameterName 
                     FROM AssetParameters ap
                     JOIN ProcessCapabilityConfiguration pcc ON ap.AssetParameterId = pcc.AssetParameterID
                     WHERE pcc.ProcessCapabilityID = @ProcessCapabilityID";

            using (var cmd = new SqlCommand(query, connection))
            {
                cmd.Parameters.AddWithValue("@ProcessCapabilityID", processCapabilityID);

                var result = await cmd.ExecuteScalarAsync();
                return result != null ? result.ToString() : null;
            }
        }


        [HttpGet]
        [Route("GetExcludedAuditLogData")]
        public async Task<IActionResult> GetExcludedAuditLogData(int TenantID, int AssetID)
        {
            try
            {
                if (TenantID == 0 || AssetID <= 0)
                {
                    return BadRequest("Invalid input parameters.");
                }

                string query = "SELECT AuditLogID, TenantID, TableName, OperationType, RecordID, OldValue, NewValue, ChangedBy, ChangedDate " +
                               "FROM AuditLog WHERE TenantID = @TenantID AND TableName = 'ProcessCapabiltyExcludedDates'";

                List<AuditlogViewModel> result = new List<AuditlogViewModel>();

                using (var connection = new SqlConnection(ConnectionString))
                {
                    await connection.OpenAsync();

                    List<dynamic> cachedRows = new List<dynamic>();
                    using (var cmd = new SqlCommand(query, connection))
                    {
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);

                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                cachedRows.Add(new
                                {
                                    AuditLogID = Convert.ToInt32(reader["AuditLogID"]),
                                    TableName = reader["TableName"] as string,
                                    OperationType = reader["OperationType"] as string,
                                    ChangedBy = reader["ChangedBy"] as string,
                                    ChangedDate = Convert.ToDateTime(reader["ChangedDate"]),
                                    OldValue = reader["OldValue"] as string,
                                    NewValue = reader["NewValue"] as string
                                });
                            }
                        }
                    }

                    foreach (var row in cachedRows)
                    {
                        var oldValue = !string.IsNullOrEmpty(row.OldValue)
                            ? JsonConvert.DeserializeObject<Dictionary<string, object>>(row.OldValue)
                            : null;

                        var newValue = !string.IsNullOrEmpty(row.NewValue)
                            ? JsonConvert.DeserializeObject<Dictionary<string, object>>(row.NewValue)
                            : null;

                        int? processCapabilityID = newValue?.ContainsKey("ProcessCapabilityID") == true
                            ? Convert.ToInt32(newValue["ProcessCapabilityID"])
                            : oldValue?.ContainsKey("ProcessCapabilityID") == true
                                ? Convert.ToInt32(oldValue["ProcessCapabilityID"])
                                : (int?)null;

                        int? assetID = newValue?.ContainsKey("AssetID") == true
                            ? Convert.ToInt32(newValue["AssetID"])
                            : oldValue?.ContainsKey("AssetID") == true
                                ? Convert.ToInt32(oldValue["AssetID"])
                                : (int?)null;

                        AuditlogViewModel viewModel = new AuditlogViewModel
                        {
                            AuditLogID = row.AuditLogID,
                            TableName = row.TableName,
                            OperationType = row.OperationType,
                            ChangedBy = row.ChangedBy,
                            ChangedDate = row.ChangedDate,
                            OldValue = oldValue,
                            NewValue = newValue
                        };

                        if (processCapabilityID.HasValue)
                        {
                            string parameterNameQuery = "SELECT ParameterName " +
                                                        "FROM AssetParameters ap " +
                                                        "JOIN ProcessCapabilityConfiguration pcc ON ap.AssetParameterId = pcc.AssetParameterID " +
                                                        "WHERE pcc.ProcessCapabilityID = @ProcessCapabilityID";

                            using (var cmd = new SqlCommand(parameterNameQuery, connection))
                            {
                                cmd.Parameters.AddWithValue("@ProcessCapabilityID", processCapabilityID.Value);
                                var parameterName = await cmd.ExecuteScalarAsync() as string;
                                viewModel.ParameterName = parameterName;
                            }
                        }

                        if (string.IsNullOrEmpty(viewModel.ParameterName) && assetID.HasValue)
                        {
                            string assetNameQuery = "SELECT DeviceName FROM Devices WHERE DeviceID = @AssetID";

                            using (var cmd = new SqlCommand(assetNameQuery, connection))
                            {
                                cmd.Parameters.AddWithValue("@AssetID", assetID.Value);
                                var assetName = await cmd.ExecuteScalarAsync() as string;
                                viewModel.AssetName = assetName;
                            }
                        }

                        if (!string.IsNullOrEmpty(viewModel.ParameterName) || !string.IsNullOrEmpty(viewModel.AssetName))
                        {
                            result.Add(viewModel);
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

        [HttpPost]
        [Route("SaveSpecficiationData")]
        public IActionResult SaveSpecficiationData(List<ProcessCapabilitySpecifications> specificationdata, string userId)
        {
            try
            {
                var StatusID = 1;

                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();
                    var sessionContextQuery = "EXEC sp_set_session_context @key = N'UserId', @value = @userId";
                    using (var command = new SqlCommand(sessionContextQuery, connection))
                    {
                        command.Parameters.AddWithValue("@userId", userId);
                        command.ExecuteNonQuery();
                    }

                    foreach (var specification in specificationdata)
                    {
                        string checkQuery = @"SELECT COUNT(*) 
                                      FROM ProcessCapabilitySpecifications 
                                      WHERE ProcessCapabilityID = @ProcessCapabilityID AND FromDate = @FromDate AND ToDate = @ToDate";

                        using (var checkCmd = new SqlCommand(checkQuery, connection))
                        {
                            checkCmd.Parameters.AddWithValue("@ProcessCapabilityID", specification.ProcessCapabilityID);
                            checkCmd.Parameters.AddWithValue("@FromDate", specification.FromDate);
                            checkCmd.Parameters.AddWithValue("@ToDate", specification.ToDate);

                            var existingCount = (int)checkCmd.ExecuteScalar();

                            if (existingCount == 0)
                            {
                                string insertQuery = @"INSERT INTO ProcessCapabilitySpecifications 
                                                (LCL, UCL, Target, FromDate, ToDate, ProcessCapabilityID, TenantID, StatusID, CreatedBy, CreatedDate,Comment)
                                                VALUES 
                                                (@LCL, @UCL, @Target, @FromDate, @ToDate, @ProcessCapabilityID, @TenantID, @StatusID, @CreatedBy, @CreatedDate,@Comment)";

                                using (var insertCmd = new SqlCommand(insertQuery, connection))
                                {
                                    insertCmd.Parameters.AddWithValue("@LCL", specification.LCL);
                                    insertCmd.Parameters.AddWithValue("@UCL", specification.UCL);
                                    insertCmd.Parameters.AddWithValue("@Target", specification.Target);
                                    insertCmd.Parameters.AddWithValue("@FromDate", specification.FromDate);
                                    insertCmd.Parameters.AddWithValue("@ToDate", specification.ToDate);
                                    insertCmd.Parameters.AddWithValue("@ProcessCapabilityID", specification.ProcessCapabilityID);
                                    insertCmd.Parameters.AddWithValue("@TenantID", specification.TenantID);
                                    insertCmd.Parameters.AddWithValue("@StatusID", StatusID);
                                    insertCmd.Parameters.AddWithValue("@CreatedBy", specification.CreatedBy);
                                    insertCmd.Parameters.AddWithValue("@CreatedDate", specification.CreatedDate);
                                    insertCmd.Parameters.AddWithValue("@Comment", specification.Comment ?? string.Empty);

                                    insertCmd.ExecuteNonQuery();
                                }
                            }
                            else
                            {
                                var existingResponse = new
                                {
                                    success = false,
                                    message = "Data Already Exists."
                                };
                                return Conflict(existingResponse);
                            }
                        }
                    }

                    var successResponse = new
                    {
                        success = true,
                        message = "Process Capability Specifications Data Updated Successfully"
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

        [HttpPost]
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
                                       )";

                        using (var cmd = new SqlCommand(query, connection))
                        {
                            cmd.Parameters.AddWithValue("@ProcessCapabilityID", ProcessCapabilityID);
                            cmd.Parameters.AddWithValue("@FromDate", dateRange.FromDate);
                            cmd.Parameters.AddWithValue("@ToDate", dateRange.ToDate != null ? (object)dateRange.ToDate : DBNull.Value);

                            int count = (int)cmd.ExecuteScalar();

                            if (count > 0)
                            {
                                var response = new
                                {
                                    success = false,
                                    count = count,
                                    message = $"Date range from {dateRange.FromDate:yyyy-MM-dd} to {dateRange.ToDate?.ToString("yyyy-MM-dd") ?? "N/A"} already exists.",
                                    fromDate = dateRange.FromDate,
                                    toDate = dateRange.ToDate
                                };
                                return BadRequest(response);
                            }
                        }
                    }

                    var successResponse = new
                    {
                        success = true,
                        count = 0,
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

        [HttpPost]
        public IActionResult CheckExcludedDatesExists(List<ProcessCapabiltyExcludedDatesViewModel> modal)
        {
            var responseList = new List<object>();

            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    string query = @"
                SELECT 
                    COUNT(*) AS ConflictCount,
                    apc.ParameterCategory
                FROM ProcessCapabiltyExcludedDates pced
                JOIN AssetParameterCategory apc 
                    ON pced.AssetParameterCategoryID = apc.AssetParameterCategoryID
                WHERE pced.AssetID = @AssetID 
                  AND pced.TenantID = @TenantID
                  AND (
                        (@FromDate BETWEEN pced.FromDate AND pced.ToDate)
                        OR (@ToDate BETWEEN pced.FromDate AND pced.ToDate)
                        OR (pced.FromDate BETWEEN @FromDate AND @ToDate)
                        OR (pced.ToDate BETWEEN @FromDate AND @ToDate)
                  )
                  AND pced.StatusID = 1
                  AND (@AssetParameterCategoryID IS NULL OR pced.AssetParameterCategoryID = @AssetParameterCategoryID)
                GROUP BY apc.ParameterCategory";

                    foreach (var item in modal)
                    {
                        using (var cmd = new SqlCommand(query, connection))
                        {
                            cmd.Parameters.AddWithValue("@AssetID", item.AssetID);
                            cmd.Parameters.AddWithValue("@TenantID", item.TenantID);
                            cmd.Parameters.AddWithValue("@FromDate", item.FromDate);
                            cmd.Parameters.AddWithValue("@ToDate", (object)item.ToDate ?? DBNull.Value);
                            cmd.Parameters.AddWithValue("@AssetParameterCategoryID", (object)item.AssetParameterCategoryId ?? DBNull.Value);

                            using (var reader = cmd.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        int count = Convert.ToInt32(reader["ConflictCount"]);
                                        string parameterCategory = reader["ParameterCategory"].ToString();

                                        string message = count > 0
                                            ? $"Given {item.FromDate} and {item.ToDate} are already exists with Given {parameterCategory} parameterCategory"
                                            : "No conflicts found.";

                                        responseList.Add(new
                                        {
                                            FromDate = item.FromDate,
                                            ToDate = item.ToDate,
                                            Count = count,
                                            AssetParameterCategory = parameterCategory,
                                            Message = message
                                        });
                                    }
                                }
                                else
                                {
                                    responseList.Add(new
                                    {
                                        FromDate = item.FromDate,
                                        ToDate = item.ToDate,
                                        Count = 0,
                                        AssetParameterCategory = string.Empty,
                                        Message = "No conflicts found."
                                    });
                                }
                            }
                        }
                    }
                }

                return Ok(responseList);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "An error occurred: " + ex.Message });
            }
        }





        [HttpPost]
        public IActionResult CheckExcludedDatesParamLevelExists(int AssetID, int ProcessCapabilityID, int TenantID, List<DateRanges> dateRanges)
        {
            var responseList = new List<object>();

            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    string query = @"
                SELECT COUNT(*)
                FROM ProcessCapabiltyExcludedDates
                WHERE AssetID = @AssetID AND TenantID = @TenantID and ProcessCapabilityID = @ProcessCapabilityID
                AND (
                    (@FromDate BETWEEN FromDate AND ToDate)
                    OR (@ToDate BETWEEN FromDate AND ToDate)
                    OR (FromDate BETWEEN @FromDate AND @ToDate)
                    OR (ToDate BETWEEN @FromDate AND @ToDate)
                ) and Statusid = 1";

                    foreach (var dateRange in dateRanges)
                    {
                        int count;
                        using (var cmd = new SqlCommand(query, connection))
                        {
                            cmd.Parameters.AddWithValue("@AssetID", AssetID);
                            cmd.Parameters.AddWithValue("@ProcessCapabilityID", ProcessCapabilityID);
                            cmd.Parameters.AddWithValue("@TenantID", TenantID);
                            cmd.Parameters.AddWithValue("@FromDate", dateRange.FromDate);
                            cmd.Parameters.AddWithValue("@ToDate", (object)dateRange.ToDate ?? DBNull.Value);

                            count = (int)cmd.ExecuteScalar();
                        }

                        responseList.Add(new
                        {
                            FromDate = dateRange.FromDate,
                            ToDate = dateRange.ToDate,
                            Count = count,
                            Message = "Given Dates are already Exists!"
                        });
                    }
                }

                return Ok(responseList);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { Message = "An error occurred: " + ex.Message });
            }
        }

        [HttpPost]
        public IActionResult GetSpecDataGrid(int ProcessCapabilityID, int TenantID, string fromDate, string toDate)
        {
            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"
                SELECT 
                    pccs.ProcessCapabilitySpecificationsID,
                    pcc.ProcessCapabilityID,
                    ap.ParameterName,
                    ap.DisplayText, 
                    pccs.LCL,
                    pccs.UCL,
                    pccs.Target,
                    pccs.Tolerance,
                    pccs.FromDate,
                    pccs.ToDate,
                    pccs.Comment,
                    pcc.DecimalCount
                FROM  
                    ProcessCapabilitySpecifications pccs 
                JOIN 
                    ProcessCapabilityConfiguration pcc 
                    ON pccs.ProcessCapabilityID = pcc.ProcessCapabilityID 
                JOIN 
                    assetparameters ap 
                    ON pcc.AssetParameterID = ap.AssetParameterId 
                    AND pcc.AssetID = ap.AssetID 
                WHERE 
                    pccs.ProcessCapabilityID = @ProcessCapabilityID 
                    AND pccs.TenantID = @TenantID 
                    AND pccs.StatusID = 1 
                    AND (CAST(pccs.FromDate AS DATE) BETWEEN @FromDate AND @ToDate)
                    AND (pccs.ToDate IS NULL OR CAST(pccs.ToDate AS DATE) BETWEEN @FromDate AND @ToDate)
                ORDER BY 
                    pccs.FromDate DESC;";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@ProcessCapabilityID", ProcessCapabilityID);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);
                        cmd.Parameters.AddWithValue("@FromDate", DateTime.Parse(fromDate).Date);
                        cmd.Parameters.AddWithValue("@ToDate", DateTime.Parse(toDate).Date);

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

        [HttpPost]
        public IActionResult UpdateSpecificationData(List<ProcessCapabilitySpecifications> specificationData, string userId)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    var sessionContextQuery = "EXEC sp_set_session_context @key = N'UserId', @value = @userId";
                    using (var command = new SqlCommand(sessionContextQuery, connection))
                    {
                        command.Parameters.AddWithValue("@userId", userId);
                        command.ExecuteNonQuery();
                    }

                    string updateSpecQuery = @"UPDATE ProcessCapabilitySpecifications 
                                       SET LCL = @LCL, UCL = @UCL, Target = @Target 
                                       WHERE ProcessCapabilitySpecificationsID = @ProcessCapabilitySpecificationsID 
                                       AND ProcessCapabilityID = @ProcessCapabilityID AND TenantID = @TenantID";

                    foreach (var spec in specificationData)
                    {
                        using (var cmd = new SqlCommand(updateSpecQuery, connection))
                        {
                            cmd.Parameters.AddWithValue("@LCL", spec.LCL);
                            cmd.Parameters.AddWithValue("@UCL", spec.UCL);
                            cmd.Parameters.AddWithValue("@Target", spec.Target);
                            cmd.Parameters.AddWithValue("@ProcessCapabilitySpecificationsID", spec.ProcessCapabilitySpecificationsID);
                            cmd.Parameters.AddWithValue("@ProcessCapabilityID", spec.ProcessCapabilityID);
                            cmd.Parameters.AddWithValue("@TenantID", spec.TenantID);

                            cmd.ExecuteNonQuery();
                        }
                    }
                }

                return Ok("Process Capability Specifications data updated successfully.");
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"An error occurred: {ex.Message}");
            }
        }

        [HttpGet]
        public IActionResult GetDeleteProcessCapabilitySpecificationsData(int ProcessCapabilitySpecificationsID, int TenantID, string UserId)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    var sessionContextQuery = "EXEC sp_set_session_context @key = N'UserId', @value = @userId";
                    using (var command = new SqlCommand(sessionContextQuery, connection))
                    {
                        command.Parameters.AddWithValue("@userId", UserId);
                        command.ExecuteNonQuery();
                    }

                    using (var cmd = new SqlCommand("UPDATE ProcessCapabilitySpecifications SET [StatusID] = 3 WHERE [ProcessCapabilitySpecificationsID] = @ProcessCapabilitySpecificationsID AND [TenantID] = @TenantID", connection))
                    {
                        cmd.CommandType = CommandType.Text;

                        cmd.Parameters.AddWithValue("@ProcessCapabilitySpecificationsID", ProcessCapabilitySpecificationsID);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);

                        int rowsAffected = cmd.ExecuteNonQuery();

                        if (rowsAffected == 0)
                        {
                            return BadRequest("Failed to delete Process Capability data.");
                        }
                    }

                    return Ok("Process Capability Specification Data Deleted successfully.");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }

        [HttpGet]
        [Route("GetProcessCapabilityExcludedDates")]
        public IActionResult GetProcessCapabilityExcludedDates(int? LocationID, int? BuildingID, int? FloorID, int? AssetID, int TenantID)
        {
            try
            {
                DataTable dataTable = new DataTable();

                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    SqlCommand cmd = new SqlCommand();
                    cmd.Connection = connection;
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = "USP_GetProcessCapabilityExcludedDates";
                    cmd.Parameters.AddWithValue("@LocationID", (object)LocationID ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@BuildingID", (object)BuildingID ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@FloorID", (object)FloorID ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@AssetID", (object)AssetID ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@TenantID", (object)TenantID ?? DBNull.Value);

                    cmd.Connection.Open();
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        dataTable.Load(reader);
                    }
                    cmd.Connection.Close();
                }

                if (dataTable.Rows.Count > 0)
                {
                    return Ok(dataTable);
                }
                else
                {
                    return NotFound("No data found for the given parameters.");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { success = false, message = ex.Message });
            }
        }


        [HttpPost]
        [Route("AddProcessCapabilityExcludedDates")]
        public IActionResult AddProcessCapabilityExcludedDates(List<ProcessCapabiltyExcludedDatesViewModel> model)
        {
            try
            {
                string query = @"
            INSERT INTO ProcessCapabiltyExcludedDates 
            (LocationID, BuildingID, FloorID, AssetID, FromDate, ToDate, Reason, StatusID, TenantID, CreatedBy, CreatedDate) 
            VALUES 
            (@LocationID, @BuildingID, @FloorID, @AssetID, @FromDate, @ToDate, @Reason, @StatusID, @TenantID, @CreatedBy, @CreatedDate)";

                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    foreach (var item in model)
                    {
                        using (SqlCommand command = new SqlCommand(query, connection))
                        {
                            command.Parameters.AddWithValue("@LocationID", item.LocationID ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@BuildingID", item.BuildingID ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@FloorID", item.FloorID ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@AssetID", item.AssetID ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@FromDate", item.FromDate);
                            command.Parameters.AddWithValue("@ToDate", item.ToDate);
                            command.Parameters.AddWithValue("@Reason", item.Reason ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@StatusID", item.StatusID);
                            command.Parameters.AddWithValue("@TenantID", item.TenantID);
                            command.Parameters.AddWithValue("@CreatedBy", item.CreatedBy);
                            command.Parameters.AddWithValue("@CreatedDate", item.CreatedDate);

                            command.ExecuteNonQuery();
                        }
                    }
                }

                return Ok("Data added successfully!");
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"An error occurred: {ex.Message}");
            }
        }

        [HttpGet]
        [Route("getGroupUsersData")]
        public IActionResult GetGroupUsersData(string tenantID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    string query = @"
                select sum.SSOGroupUsersID,su.Name UserName ,su.ssoUsersID,su.UserEmailID,sag.Name  AS GroupName,sag.GroupID,su.StatusID from 
                SSousers su 
                join SSOGroupUsersMap as sum on su.ssoUsersID=sum.ssoUsersID
                join SSOApplicationGroups sag on sum.GroupID= sag.Groupid
                WHERE sag.TenantID = @tenantID AND su.StatusID != 3
                order by sum.SSOGroupUsersID desc ";

                    using (var cmd = new SqlCommand(query, connection))
                    {
                        cmd.Parameters.AddWithValue("@tenantID", tenantID);

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
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }

        [HttpGet]
        [Route("getGroupNames")]
        public IActionResult getGroupNames(string tenantID, string applicationID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    string query = @"
                select * from  SSOApplicationGroups WHERE TenantID = @tenantID  and ApplicationID=@applicationID";

                    using (var cmd = new SqlCommand(query, connection))
                    {
                        cmd.Parameters.AddWithValue("@tenantID", tenantID);
                        cmd.Parameters.AddWithValue("@applicationID", applicationID);

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
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }

        [HttpGet]
        [Route("getApplicationNames")]
        public IActionResult getApplicationNames(string tenantID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    string query = @"select * from  SSOApplications WHERE TenantID = @tenantID";

                    using (var cmd = new SqlCommand(query, connection))
                    {
                        cmd.Parameters.AddWithValue("@tenantID", tenantID);

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
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }

        [HttpGet]
        [Route("getExistingUsers")]
        public IActionResult getExistingUsers(string tenantID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    string query = @"
                SELECT su.UserEmailID, su.Name
                FROM SSOUsers su
                WHERE su.TenantID = @tenantID 
                AND su.SSOUsersID NOT IN (
                    SELECT DISTINCT sgum.SSOUsersID 
                    FROM SSOGroupUsersMap sgum
                    JOIN SSOApplicationGroups sag ON sgum.GroupID = sag.GroupID
                ); ";

                    using (var cmd = new SqlCommand(query, connection))
                    {
                        cmd.Parameters.AddWithValue("@tenantID", tenantID);

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
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }

        [HttpPost]
        [Route("UpdateUserGroup")]
        public IActionResult UpdateUserGroup(string tenantId, int ssoUsersID)
        {
            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.CommandText = @"
                    UPDATE SSOUsers
                        SET StatusID = 3
                        WHERE SSOUsersID = @ssoUsersID;
                ";

                        cmd.Parameters.AddWithValue("@ssoUsersID", ssoUsersID);

                        conn.Open();
                        cmd.ExecuteNonQuery();

                        return Ok("Deleted and status updated successfully.");
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpPost]
        [Route("getGroupUsersEditData")]
        public IActionResult getGroupUsersEditData(string tenantID, string SSOUsersID)
        {
            var dt = new DataTable();
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    using (var cmd = new SqlCommand(@"
                select sum.SSOGroupUsersID,su.Name  ,su.SSOUsersID,su.UserEmailID,sag.Name  AS GroupName,sag.GroupID,sum.StatusID from 
                SSousers su  
                join SSOGroupUsersMap as sum on su.ssoUsersID=sum.ssoUsersID
                join SSOApplicationGroups sag on sum.GroupID= sag.Groupid
                WHERE su.SSOUsersID =@SSOUsersID and sag.TenantID = @tenantID", connection))
                    {
                        cmd.Parameters.AddWithValue("@tenantID", tenantID);
                        cmd.Parameters.AddWithValue("@SSOUsersID", SSOUsersID);

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
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }

        [HttpPost]
        [Route("UpdataGroupUserData")]
        public IActionResult UpdataGroupUserData(string tenantId, string SSOUsersID, string GroupID, string Name, string UserEmailID, string StatusID, string ModifiedBy, string ModifiedDate)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    using (var transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            using (var cmd = new SqlCommand(@"
                        UPDATE SSOUsers 
                        SET Name = @Name, UserEmailID = @UserEmailID, StatusID = @StatusID, 
                            ModifiedBy = @ModifiedBy, ModifiedDate = @ModifiedDate
                        WHERE SSOUsersID = @SSOUsersID;
                        
                        UPDATE SSOGroupUsersMap 
                        SET GroupID = @GroupID, StatusID = @StatusID, 
                            ModifiedBy = @ModifiedBy, ModifiedDate = @ModifiedDate
                        WHERE SSOUsersID = @SSOUsersID;", connection, transaction))
                            {
                                cmd.Parameters.AddWithValue("@GroupID", GroupID);
                                cmd.Parameters.AddWithValue("@SSOUsersID", SSOUsersID);
                                cmd.Parameters.AddWithValue("@Name", Name);
                                cmd.Parameters.AddWithValue("@UserEmailID", UserEmailID);
                                cmd.Parameters.AddWithValue("@StatusID", StatusID);
                                cmd.Parameters.AddWithValue("@ModifiedBy", ModifiedBy);
                                cmd.Parameters.AddWithValue("@ModifiedDate", ModifiedDate);

                                int rowsAffected = cmd.ExecuteNonQuery();

                                if (rowsAffected > 0)
                                {
                                    transaction.Commit();
                                    return Ok("Update successful");
                                }
                                else
                                {
                                    transaction.Rollback();
                                    return NotFound("No matching records found");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            return StatusCode(500, "An error occurred: " + ex.Message);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }



        [HttpPost]
        [Route("GetGroupData")]
        public IActionResult GetGroupData(string TenantID, string groupId, string name, string email, string status, string createdBy, string createdDate, string ssoUserId)
        {
            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    using (SqlTransaction transaction = conn.BeginTransaction())
                    {
                        try
                        {
                            int ssoUsersID;

                            if (string.IsNullOrEmpty(ssoUserId) || ssoUserId == "null")
                            {
                                string insertUserQuery = @"
                            INSERT INTO SSOUsers (Name, UserEmailID, TenantID, StatusID, CreatedBY, CreatedDate)
                            VALUES (@name, @email, @TenantID, @status, @createdBy, @createdDate);
                            SELECT SCOPE_IDENTITY();";

                                using (SqlCommand cmd = new SqlCommand(insertUserQuery, conn, transaction))
                                {
                                    cmd.Parameters.AddWithValue("@name", name);
                                    cmd.Parameters.AddWithValue("@email", email);
                                    cmd.Parameters.AddWithValue("@TenantID", TenantID);
                                    cmd.Parameters.AddWithValue("@status", status);
                                    cmd.Parameters.AddWithValue("@createdBy", createdBy);
                                    cmd.Parameters.AddWithValue("@createdDate", createdDate);

                                    ssoUsersID = Convert.ToInt32(cmd.ExecuteScalar());
                                }
                            }
                            else
                            {
                                ssoUsersID = Convert.ToInt32(ssoUserId);
                            }

                            string insertGroupQuery = @"
                        INSERT INTO SSOGroupUsersMap (GroupID, SSOUsersID, TenantID, StatusID, CreatedBY, CreatedDate)
                        VALUES (@groupId, @ssoUsersID, @TenantID, @status, @createdBy, @createdDate);";

                            using (SqlCommand cmdGroup = new SqlCommand(insertGroupQuery, conn, transaction))
                            {
                                cmdGroup.Parameters.AddWithValue("@groupId", groupId);
                                cmdGroup.Parameters.AddWithValue("@ssoUsersID", ssoUsersID);
                                cmdGroup.Parameters.AddWithValue("@TenantID", TenantID);
                                cmdGroup.Parameters.AddWithValue("@status", status);
                                cmdGroup.Parameters.AddWithValue("@createdBy", createdBy);
                                cmdGroup.Parameters.AddWithValue("@createdDate", createdDate);

                                cmdGroup.ExecuteNonQuery();
                            }

                            transaction.Commit();
                            return Ok("Data inserted successfully.");
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            return StatusCode(500, "Transaction failed: " + ex.Message);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet]
        public IActionResult GetDeleteProcessCapabilityExcludeData(int ProcessCapabilityExcludedDatesID, int TenantID, string UserId)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    var sessionContextQuery = "EXEC sp_set_session_context @key = N'UserId', @value = @userId";
                    using (var command = new SqlCommand(sessionContextQuery, connection))
                    {
                        command.Parameters.AddWithValue("@userId", UserId);
                        command.ExecuteNonQuery();
                    }

                    using (var cmd = new SqlCommand("UPDATE ProcessCapabiltyExcludedDates SET [StatusID] = 3 WHERE [ProcessCapabilityExcludedDatesID] = @ProcessCapabilityExcludedDatesID AND [TenantID] = @TenantID", connection))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@ProcessCapabilityExcludedDatesID", ProcessCapabilityExcludedDatesID);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);

                        int rowsAffected = cmd.ExecuteNonQuery();

                        if (rowsAffected == 0)
                        {
                            return BadRequest("Failed to delete Process Capability data.");
                        }
                    }

                    return Ok("Process Capability Excluded data deleted successfully.");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }

        [HttpGet]
        public IActionResult GetFetchandSaveSpecificationsData(int AssetID, int TenantID)
        {
            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"SELECT pcc.ProcessCapabilityID,
                                    ap.AssetParameterId,
                                    ap.ParameterName,
                                    ap.DisplayText,
                                    pcc.DecimalCount,
                                    pcc.MeasurementFrequencyLookupID,
                                    CASE 
                                        WHEN pcc.UpdatedLCL IS NOT NULL AND LTRIM(RTRIM(pcc.UpdatedLCL)) <> '' 
                                            THEN pcc.UpdatedLCL 
                                        ELSE pcc.LCL 
                                    END AS LCL,
                                    CASE 
                                       WHEN pcc.UpdatedLCL IS NOT NULL AND LTRIM(RTRIM(pcc.UpdatedLCL)) <> '' 
                                            THEN pcc.UpdatedUCL 
                                        ELSE pcc.UCL 
                                    END AS UCL,
                                    CASE 
                                        WHEN pcc.UpdatedLCL IS NOT NULL AND LTRIM(RTRIM(pcc.UpdatedLCL)) <> '' 
                                            THEN pcc.UpdatedTarget 
                                        ELSE pcc.Target 
                                    END AS Target,
                                    CASE 
                                        WHEN pcc.UpdatedLCL IS NOT NULL AND LTRIM(RTRIM(pcc.UpdatedLCL)) <> ''
                                            THEN pcc.UpdatedTolerance 
                                        ELSE pcc.Tolerance 
                                    END AS Tolerance
                             FROM ProcessCapabilityConfiguration pcc
                             JOIN assetparameters ap ON pcc.AssetParameterID = ap.AssetParameterId
                             WHERE ap.StatusId = 1 
                               AND pcc.StatusId = 1 
                               AND ap.AssetId = @AssetID 
                               AND pcc.TenantID = @TenantID
                               ORDER BY ap.SortOrder;";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@AssetID", AssetID);
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

        [HttpGet]
        public IActionResult GetAssetParameterCategoryforSpecData(int AssetID, int TenantID)
        {
            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"select * from assetparametercategory where assetparametercategoryid in 
                             (select assetparametercategoryid from assetparameters where assetid=@AssetID
                             and tenantid=@TenantID and statusid=1);";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@AssetID", AssetID);
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



        [HttpPost]
        [Route("SaveMultipleSpecificationData")]
        public IActionResult SaveMultipleSpecificationData(List<ProcessCapabilitySpecifications> specificationdata, string userId)
        {
            if (specificationdata == null || !specificationdata.Any())
            {
                return BadRequest(new
                {
                    success = false,
                    message = "No specification data provided."
                });
            }

            int operationType = specificationdata.First().Case;

            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    var sessionContextQuery = "EXEC sp_set_session_context @key = N'UserId', @value = @userId";
                    using (var command = new SqlCommand(sessionContextQuery, connection))
                    {
                        command.Parameters.AddWithValue("@userId", userId);
                        command.ExecuteNonQuery();
                    }

                    foreach (var specification in specificationdata)
                    {
                        using (var spCmd = new SqlCommand("ManageProcessCapabilitySpecifications", connection))
                        {
                            spCmd.CommandType = CommandType.StoredProcedure;

                            spCmd.Parameters.AddWithValue("@FromDate", specification.FromDate);
                            spCmd.Parameters.AddWithValue("@ToDate", specification.ToDate != null ? (object)specification.ToDate : DBNull.Value);
                            spCmd.Parameters.AddWithValue("@ProcessCapabilityID", specification.ProcessCapabilityID);
                            spCmd.Parameters.AddWithValue("@ProcessCapabilitySpecificationID", specification.ProcessCapabilitySpecificationsID);
                            spCmd.Parameters.AddWithValue("@LCL", specification.LCL);
                            spCmd.Parameters.AddWithValue("@UCL", specification.UCL);
                            spCmd.Parameters.AddWithValue("@Target", specification.Target);
                            spCmd.Parameters.AddWithValue("@Tolerance", specification.Tolerance);
                            spCmd.Parameters.AddWithValue("@TenantID", specification.TenantID);
                            spCmd.Parameters.AddWithValue("@CreatedBy", specification.CreatedBy);
                            spCmd.Parameters.AddWithValue("@CreatedDate", specification.CreatedDate);
                            spCmd.Parameters.AddWithValue("@Comment", string.IsNullOrEmpty(specification.Comment) ? string.Empty : specification.Comment);
                            spCmd.Parameters.AddWithValue("@Case", specification.Case);
                            spCmd.Parameters.AddWithValue("@AssetParameterCategoryID", specification.AssetParameterCategoryID != 0
                                ? specification.AssetParameterCategoryID
                                : (object)DBNull.Value);

                            spCmd.ExecuteNonQuery();
                        }
                    }
                }

                string successMessage = operationType == 1
                    ? "Process Capability Specifications Data updated successfully"
                    : "Process Capability Specifications Data Deleted successfully";

                return Ok(new
                {
                    success = true,
                    message = successMessage
                });
            }
            catch (Exception ex)
            {
                string errorMessage = operationType == 1
                    ? "An unexpected error occurred while Updating data: " + ex.Message
                    : "An unexpected error occurred while Delete data: " + ex.Message;

                return StatusCode(500, new
                {
                    success = false,
                    message = errorMessage
                });
            }
        }

        [HttpGet]
        public IActionResult GetAssetParameterCategorySpecData(int AssetID, int AssetParameterCategoryId, int TenantID)
        {
            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"SELECT apc.assetparametercategoryid,pcc.ProcessCapabilityID,ap.AssetParameterId,pcc.DecimalCount,pcc.MeasurementFrequencyLookupID,
                             ap.ParameterName,ap.DisplayText,COALESCE(pcc.UpdatedLCL, pcc.LCL) AS LCL,COALESCE(pcc.UpdatedUCL, pcc.UCL) AS UCL,
                             COALESCE(pcc.UpdatedTarget, pcc.Target) AS Target,COALESCE(pcc.UpdatedTolerance, pcc.Tolerance) AS Tolerance
                             FROM ProcessCapabilityConfiguration pcc JOIN assetparameters ap ON pcc.AssetParameterID = ap.AssetParameterId
                             JOIN AssetparameterCategory apc ON apc.assetparametercategoryid = ap.assetparametercategoryid WHERE ap.StatusId = 1 
                             AND pcc.statusid = 1 AND ap.assetId = @AssetID AND apc.assetparametercategoryid = @assetparametercategoryid
                             AND pcc.TenantID = @TenantID;";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@AssetID", AssetID);
                        cmd.Parameters.AddWithValue("@assetparametercategoryid", AssetParameterCategoryId);
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

        [HttpGet]
        public IActionResult GetSpecData(int ProcessCapabilityID, int TenantID)
        {
            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"SELECT 
pcc.ProcessCapabilityID, 
ap.AssetParameterId,
ap.ParameterName,
ap.DisplayText,
CASE 
    WHEN pcc.UpdatedLCL IS NOT NULL AND LTRIM(RTRIM(pcc.UpdatedLCL)) <> '' 
         THEN pcc.UpdatedLCL 
    ELSE pcc.LCL 
END AS LCL,
CASE 
    WHEN pcc.UpdatedLCL IS NOT NULL AND LTRIM(RTRIM(pcc.UpdatedLCL)) <> '' 
         THEN pcc.UpdatedUCL 
    ELSE pcc.UCL 
END AS UCL,
CASE 
   WHEN pcc.UpdatedLCL IS NOT NULL AND LTRIM(RTRIM(pcc.UpdatedLCL)) <> '' 
         THEN pcc.UpdatedTarget 
    ELSE pcc.Target 
END AS Target,
CASE 
    WHEN pcc.UpdatedLCL IS NOT NULL AND LTRIM(RTRIM(pcc.UpdatedLCL)) <> '' 
         THEN pcc.UpdatedTolerance 
    ELSE pcc.Tolerance 
END AS Tolerance
FROM ProcessCapabilityConfiguration pcc
JOIN assetparameters ap ON pcc.AssetParameterID = ap.AssetParameterId
WHERE ap.StatusId = 1 
  AND pcc.StatusId = 1 
  AND pcc.ProcessCapabilityID= @ProcessCapabilityID
  AND pcc.TenantID = @TenantID;";

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

        [HttpPost]
        public IActionResult CheckFromDateExists(string fromdate, int processCapabilityID, int processCapabilitySpecificationsID, int tenantID)
        {
            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    string query = @"SELECT TOP 1 FromDate FROM ProcessCapabilitySpecifications 
                      WHERE ProcessCapabilitySpecificationsID = @ProcessCapabilitySpecificationsID 
                      AND TenantID = @TenantID AND StatusID = 1 
                      ORDER BY FromDate DESC";

                    SqlCommand cmd = new SqlCommand(query, conn);
                    cmd.Parameters.AddWithValue("@ProcessCapabilitySpecificationsID", processCapabilitySpecificationsID);
                    cmd.Parameters.AddWithValue("@TenantID", tenantID);
                    object result = cmd.ExecuteScalar();

                    if (result != null)
                    {
                        DateTime outputFromDate = Convert.ToDateTime(result);
                        DateTime parameterFromDate = Convert.ToDateTime(fromdate);

                        string conditionQuery = @"SELECT COUNT(*) FROM ProcessCapabilitySpecifications 
                                  WHERE FromDate BETWEEN @StartDate AND @EndDate 
                                  AND ProcessCapabilityID = @ProcessCapabilityID 
                                  AND TenantID = @TenantID 
                                  AND StatusID = 1
                                  AND ProcessCapabilitySpecificationsID!= @processCapabilitySpecificationsID  ";

                        DateTime startDate = parameterFromDate < outputFromDate ? parameterFromDate : outputFromDate;
                        DateTime endDate = parameterFromDate > outputFromDate ? parameterFromDate : outputFromDate;

                        SqlCommand conditionCmd = new SqlCommand(conditionQuery, conn);
                        conditionCmd.Parameters.AddWithValue("@StartDate", startDate);
                        conditionCmd.Parameters.AddWithValue("@EndDate", endDate);
                        conditionCmd.Parameters.AddWithValue("@ProcessCapabilityID", processCapabilityID);
                        conditionCmd.Parameters.AddWithValue("@TenantID", tenantID);
                        conditionCmd.Parameters.AddWithValue("@processCapabilitySpecificationsID", processCapabilitySpecificationsID);

                        int count = (int)conditionCmd.ExecuteScalar();

                        var successResponse = new
                        {
                            success = true,
                            count = count > 0 ? 1 : 0,
                            message = count > 0 ? "The updated FromDate lies between the original FromDate" : "Date is valid."
                        };
                        return Ok(successResponse);
                    }
                    else
                    {
                        var successResponse = new
                        {
                            success = true,
                            count = 0,
                            message = "Date does not exist."
                        };

                        return Ok(successResponse);
                    }
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

        [HttpGet]
        public IActionResult GetDateforEditSortorder(int AssetID, int TenantID)
        {
            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"SELECT 
AP.AssetParameterID, 
AP.ParameterName, 
APC.ParameterCategory, 
AP.DisplayText, 
AP.SortOrder
FROM 
    AssetParameters AP
JOIN 
    ProcessCapabilityConfiguration PCC 
    ON AP.AssetID = PCC.AssetID 
    AND AP.AssetParameterID = PCC.AssetParameterID 
    AND AP.TenantID = PCC.TenantID
 JOIN 
    AssetParameterCategory APC 
    ON AP.AssetParameterCategoryID = APC.AssetParameterCategoryID
WHERE 
    AP.AssetID = @AssetID 
    AND AP.TenantID = @TenantID 
    AND AP.StatusID IN (1, 2)
	AND PCC.StatusID IN (1,2)
ORDER BY 
    AP.SortOrder;";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@AssetID", AssetID);
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

        [HttpPost]
        [Route("SaveMultipleSortorderData")]
        public IActionResult SaveMultipleSortorderData(List<ProcessCapabilityConfigurationViewModel> sortorderdata)
        {
            if (sortorderdata == null || !sortorderdata.Any())
            {
                return BadRequest(new
                {
                    success = false,
                    message = "No data provided."
                });
            }

            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    foreach (var specification in sortorderdata)
                    {
                        string updateQuery = @"
                UPDATE AssetParameters
                SET SortOrder = @SortOrder
                WHERE AssetParameterID = @AssetParameterID AND AssetID = @AssetID AND TenantID = @TenantID";

                        using (SqlCommand spCmd = new SqlCommand(updateQuery, connection))
                        {
                            spCmd.CommandType = CommandType.Text;
                            spCmd.Parameters.AddWithValue("@SortOrder", specification.SortOrder);
                            spCmd.Parameters.AddWithValue("@AssetParameterID", specification.AssetParameterID);
                            spCmd.Parameters.AddWithValue("@AssetID", specification.AssetID);
                            spCmd.Parameters.AddWithValue("@TenantID", specification.TenantID);

                            spCmd.ExecuteNonQuery();
                        }
                    }
                }

                return Ok(new
                {
                    success = true,
                    message = "Process Capability Specifications updated successfully"
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new
                {
                    success = false,
                    message = "An unexpected error occurred: " + ex.Message
                });
            }
        }

        [HttpPost]
        [Route("AddAssetParameterCategory")]
        public IActionResult AddAssetParameterCategory(int AssetID, string ParameterCategory, int TenantID)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();
                    DateTime indianTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, INDIAN_ZONE);
                    int assetCategoryId = 0;

                    string selectQuery = "SELECT DeviceCategoryID FROM Devices WHERE DeviceID = @DeviceID AND TenantID = @TenantID";
                    using (var selectCmd = new SqlCommand(selectQuery, connection))
                    {
                        selectCmd.Parameters.AddWithValue("@DeviceID", AssetID);
                        selectCmd.Parameters.AddWithValue("@TenantID", TenantID);

                        object result = selectCmd.ExecuteScalar();
                        if (result == null)
                        {
                            return NotFound("Asset category not found.");
                        }

                        assetCategoryId = Convert.ToInt32(result);
                    }

                    string insertQuery = @"
            INSERT INTO AssetParameterCategory (ParameterCategory, AssetCategoryID, CreatedDate, StatusID, ParameterCode)
            OUTPUT INSERTED.AssetParameterCategoryID, INSERTED.ParameterCategory
            VALUES (@ParameterCategory, @AssetCategoryID, @CreatedDate, @StatusID, @ParameterCode)";

                    using (var insertCmd = new SqlCommand(insertQuery, connection))
                    {
                        insertCmd.Parameters.AddWithValue("@ParameterCategory", ParameterCategory);
                        insertCmd.Parameters.AddWithValue("@AssetCategoryID", assetCategoryId);
                        insertCmd.Parameters.AddWithValue("@CreatedDate", indianTime);
                        insertCmd.Parameters.AddWithValue("@StatusID", 1);
                        insertCmd.Parameters.AddWithValue("@ParameterCode", ParameterCategory);

                        using (var reader = insertCmd.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                var insertedId = reader["AssetParameterCategoryID"];
                                var insertedCategory = reader["ParameterCategory"];

                                var response = new
                                {
                                    AssetParmeterCategoryID = insertedId,
                                    ParameterCategory = insertedCategory
                                };

                                return Ok(response);
                            }
                            else
                            {
                                return StatusCode(500, "Insert failed.");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }



        // For Excel Related Data


        [HttpGet]
        [Route("GetAssetParameterCategoryForExcel")]
        public IActionResult GetAssetParameterCategoryForExcel(int AssetID, int TenantID)
        {
            var result = new List<string>();

            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"
                SELECT ParameterCategory 
                FROM AssetParameterCategory 
                WHERE AssetCategoryID IN 
                (
                    SELECT DeviceCategoryID 
                    FROM Devices 
                    WHERE DeviceID = @AssetID AND TenantID = @TenantID
                )";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@AssetID", AssetID);
                        cmd.Parameters.AddWithValue("@TenantID", TenantID);

                        _con.Open();

                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                result.Add(reader["ParameterCategory"].ToString());
                            }
                        }
                    }
                }

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }

        [HttpGet]
        [Route("GetMeasurementFrequencyLookupForExcel")]
        public IActionResult GetMeasurementFrequencyLookupForExcel()
        {
            var result = new List<string>();

            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"
                SELECT Name, MeasurementFrequencyLookupID 
                FROM ParameterMeasurementFrequencyLookup
                WHERE StatusID = 1;";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        _con.Open();

                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                result.Add(reader["Name"].ToString());
                            }
                        }
                    }
                }

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }

        [HttpGet]
        [Route("GetDataTypeForExcel")]
        public IActionResult GetDataTypeForExcel()
        {
            var result = new List<string>();

            try
            {
                using (SqlConnection _con = new SqlConnection(ConnectionString))
                {
                    string query = @"
                SELECT 
                    DataTypeID,
                    Type 
                FROM ProcessCapabilityDataTypeLookup
                WHERE StatusID IN (1);";

                    using (SqlCommand cmd = new SqlCommand(query, _con))
                    {
                        _con.Open();

                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                result.Add(reader["Type"].ToString());
                            }
                        }
                    }
                }

                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An error occurred: " + ex.Message);
            }
        }

        [HttpPost]
        public IActionResult UploadExceltoDB(List<ExcelToDatabaseViewModel> modelList)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    foreach (var model in modelList)
                    {
                        using (var command = new SqlCommand("ImportExcelDataToDataBase", connection))
                        {
                            command.CommandType = CommandType.StoredProcedure;

                            command.Parameters.AddWithValue("@AssetId", model.AssetID);
                            command.Parameters.AddWithValue("@ParameterName", model.ParameterName ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@ParameterCode", model.ParameterName ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@DisplayText", model.DisplayText ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@ParameterCategory", model.AssetParameterCategory ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@Units", model.Units ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@SortOrder", model.SortOrder);
                            command.Parameters.AddWithValue("@LCL", (object)model.LCL ?? DBNull.Value);
                            command.Parameters.AddWithValue("@UCL", (object)model.UCL ?? DBNull.Value);
                            command.Parameters.AddWithValue("@Target", (object)model.Target ?? DBNull.Value);
                            command.Parameters.AddWithValue("@Tolerance", (object)model.Tolerance ?? DBNull.Value);
                            command.Parameters.AddWithValue("@TagType", model.TagType ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@MeasurementFrequency", model.MeasurementFrequency ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@DecimalCount", model.DecimalCount);
                            command.Parameters.AddWithValue("@Comment", model.Comment ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@CreatedBy", model.CreatedBy);
                            command.Parameters.AddWithValue("@ModifiedBy", model.ModifiedBy);
                            command.Parameters.AddWithValue("@TenantID", model.TenantID);

                            using (var reader = command.ExecuteReader())
                            {
                                // Optional: You can log or verify results from the reader if the SP returns something
                                while (reader.Read())
                                {
                                    // Optional reading logic
                                }
                            }
                        }
                    }

                    return Ok("Excel data successfully imported.");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, "Error: " + ex.Message);
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
    }
    

}
