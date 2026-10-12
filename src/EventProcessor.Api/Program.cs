using System;
using System.Threading.RateLimiting;
using EventProcessor.Api.BackgroundServices;
using EventProcessor.Api.Ingestion;
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
builder.Services.Configure<MessagingSettings>(builder.Configuration.GetSection("Messaging"));
builder.Services.Configure<RabbitMQSettings>(builder.Configuration.GetSection("RabbitMQ"));
builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("Kafka"));
builder.Services.Configure<IngestSettings>(builder.Configuration.GetSection("Ingest"));
builder.Services.Configure<PostgreSQLSettings>(builder.Configuration.GetSection("PostgreSQL"));

// Services
var messaging = builder.Configuration.GetSection("Messaging").Get<MessagingSettings>() ?? new MessagingSettings();
var ingest = builder.Configuration.GetSection("Ingest").Get<IngestSettings>() ?? new IngestSettings();
if (ingest.IsDirect && !messaging.UseKafka)
    throw new InvalidOperationException("Ingest:Mode=Direct requires Messaging:Transport=Kafka");

if (messaging.UseKafka)
    builder.Services.AddSingleton<IEventPublisher, KafkaPublisher>();
else
    builder.Services.AddSingleton<IEventPublisher, RabbitMQPublisher>();

if (ingest.IsDirect)
{
    builder.Services.AddSingleton<IEventIngestor, DirectKafkaIngestor>();
}
else
{
    // Connection pooling
    var pgConnectionString = builder.Configuration.GetSection("PostgreSQL")["ConnectionString"]!;
    var dataSource = new NpgsqlDataSourceBuilder(pgConnectionString).Build();
    builder.Services.AddSingleton(dataSource);

    builder.Services.AddSingleton<OutboxQueue>();
    builder.Services.AddSingleton<IEventIngestor, OutboxIngestor>();
    builder.Services.AddScoped<IEventRepository, PostgreSQLEventRepository>();
    builder.Services.AddScoped<IOutboxRepository, PostgreSQLOutboxRepository>();
}

// Validation
builder.Services.AddFluentValidationAutoValidation();
builder.Services.AddValidatorsFromAssemblyContaining<EventValidator>();

// Background Services (outbox mode only)
if (!ingest.IsDirect)
{
    builder.Services.AddHostedService<OutboxWriterService>();
    builder.Services.AddHostedService<OutboxProcessor>();
    builder.Services.AddHostedService<OutboxCleanupService>();
}

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

builder.Services.AddHealthChecks();
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Create topics with the configured partition count before any producer can auto-create them
if (messaging.UseKafka)
{
    var kafkaSettings = builder.Configuration.GetSection("Kafka").Get<KafkaSettings>() ?? new KafkaSettings();
    await KafkaTopicInitializer.EnsureTopicsAsync(kafkaSettings, app.Logger);
}

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseSerilogRequestLogging();
app.UseRateLimiter();
app.MapControllers();
app.MapHealthChecks("/healthz");

app.Run();
