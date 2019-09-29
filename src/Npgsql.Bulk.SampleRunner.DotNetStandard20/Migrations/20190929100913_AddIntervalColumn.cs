using System;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Npgsql.Bulk.SampleRunner.DotNetStandard20.Migrations
{
    public partial class AddIntervalColumn : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<TimeSpan>(
                name: "interval",
                schema: "public",
                table: "addresses",
                nullable: false,
                defaultValue: new TimeSpan(0, 0, 0, 0, 0));
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "interval",
                schema: "public",
                table: "addresses");
        }
    }
}
