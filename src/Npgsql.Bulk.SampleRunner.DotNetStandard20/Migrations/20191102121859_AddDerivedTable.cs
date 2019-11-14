using Microsoft.EntityFrameworkCore.Migrations;

namespace Npgsql.Bulk.SampleRunner.DotNetStandard20.Migrations
{
    public partial class AddDerivedTable : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "Discriminator",
                schema: "public",
                table: "addresses",
                nullable: false,
                defaultValue: "");

            migrationBuilder.AddColumn<int>(
                name: "index2",
                schema: "public",
                table: "addresses",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "localized_name",
                schema: "public",
                table: "addresses",
                nullable: true);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "Discriminator",
                schema: "public",
                table: "addresses");

            migrationBuilder.DropColumn(
                name: "index2",
                schema: "public",
                table: "addresses");

            migrationBuilder.DropColumn(
                name: "localized_name",
                schema: "public",
                table: "addresses");
        }
    }
}
