using EventProcessor.Application.Interfaces;
using EventProcessor.Infrastructure.Persistence;
using EventProcessor.Infrastructure.Settings;
using EventProcessor.Worker;
using Npgsql;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);

// Serilog
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();

builder.Services.AddSerilog();

// Settings
builder.Services.Configure<RabbitMQSettings>(builder.Configuration.GetSection("RabbitMQ"));
builder.Services.Configure<PostgreSQLSettings>(builder.Configuration.GetSection("PostgreSQL"));

// Connection pooling
var pgConnectionString = builder.Configuration.GetSection("PostgreSQL")["ConnectionString"]!;
var dataSource = new NpgsqlDataSourceBuilder(pgConnectionString).Build();
builder.Services.AddSingleton(dataSource);

// Services
builder.Services.AddScoped<IEventRepository, PostgreSQLEventRepository>();

// Worker
builder.Services.AddHostedService<EventConsumerWorker>();

var host = builder.Build();
host.Run();
