using System;
using System.Threading.RateLimiting;
using EventProcessor.Api.BackgroundServices;
using EventProcessor.Application.Interfaces;
using EventProcessor.Application.Validators;
using EventProcessor.Infrastructure.Messaging;
using EventProcessor.Infrastructure.Persistence;
using EventProcessor.Infrastructure.Settings;
using FluentValidation;
using FluentValidation.AspNetCore;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

// Serilog
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();

builder.Host.UseSerilog();

// Settings
builder.Services.Configure<RabbitMQSettings>(builder.Configuration.GetSection("RabbitMQ"));
builder.Services.Configure<PostgreSQLSettings>(builder.Configuration.GetSection("PostgreSQL"));

// Connection pooling
var pgConnectionString = builder.Configuration.GetSection("PostgreSQL")["ConnectionString"]!;
var dataSource = new NpgsqlDataSourceBuilder(pgConnectionString).Build();
builder.Services.AddSingleton(dataSource);

// Services
builder.Services.AddSingleton<IEventPublisher, RabbitMQPublisher>();
builder.Services.AddSingleton<OutboxQueue>();
builder.Services.AddScoped<IEventRepository, PostgreSQLEventRepository>();
builder.Services.AddScoped<IOutboxRepository, PostgreSQLOutboxRepository>();

// Validation
builder.Services.AddFluentValidationAutoValidation();
builder.Services.AddValidatorsFromAssemblyContaining<EventValidator>();

// Background Services
builder.Services.AddHostedService<OutboxWriterService>();
builder.Services.AddHostedService<OutboxProcessor>();
builder.Services.AddHostedService<OutboxCleanupService>();

// Rate Limiting
builder.Services.AddRateLimiter(options =>
{
    options.AddFixedWindowLimiter("ingestion", opt =>
    {
        opt.PermitLimit = 3000;
        opt.Window = TimeSpan.FromSeconds(1);
        opt.QueueLimit = 500;
        opt.QueueProcessingOrder = QueueProcessingOrder.OldestFirst;
    });
    options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
});

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseSerilogRequestLogging();
app.UseRateLimiter();
app.MapControllers();

app.Run();
