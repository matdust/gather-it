using JobReadApi.Application.Enums;
using JobReadApi.Application.Interfaces;
using JobReadApi.Infrastructure.Data;
using JobReadApi.Infrastructure.Repositories;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace JobReadApi.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddReadApiInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString("Postgres")
            ?? throw new InvalidOperationException("ConnectionStrings:Postgres is not configured.");

        services.AddDbContext<ReadApiDbContext>(options =>
            options.UseNpgsql(connectionString, npgsql => npgsql
                    .MapEnum<JobSite>()
                    .MapEnum<ContractType>()
                    .MapEnum<WorkplaceType>()
                    .MapEnum<ExperienceLevel>()
                    .MapEnum<OfferStatus>())
                .UseSnakeCaseNamingConvention());

        services.AddScoped<IJobAdReadRepository, JobAdReadRepository>();

        return services;
    }
}

