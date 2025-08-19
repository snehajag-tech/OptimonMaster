var builder = WebApplication.CreateBuilder(args);
builder.Services.AddControllers();
// Add services to the container.

var app = builder.Build();


// Configure the HTTP request pipeline.
app.UseCors(x => x.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader());

app.UseHttpsRedirection();

app.MapControllers();

app.Run();

//             }