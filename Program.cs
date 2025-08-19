using System.Text.Json;
using System.Text.Json.Serialization;

var builder = WebApplication.CreateBuilder(args);

// Add CORS policy to allow requests from the React development server
builder.Services.AddCors(options =>
{
    options.AddPolicy("ReactAppPolicy",
        policyBuilder =>
        {
            policyBuilder.WithOrigins("http://localhost:5173")
                         .AllowAnyHeader()
                         .AllowAnyMethod();
        });
});

builder.Services.AddControllers()
    .AddJsonOptions(options =>
    {
        options.JsonSerializerOptions.ReferenceHandler = ReferenceHandler.IgnoreCycles;
        options.JsonSerializerOptions.WriteIndented = true;
        options.JsonSerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase; 
    });

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
}

app.UseStaticFiles();
app.UseRouting();

// Apply the CORS policy
app.UseCors("ReactAppPolicy");

app.UseAuthorization();

app.MapControllers();

app.Run();
