using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.Bulk.SampleRunner.DotNetStandard20.DAL;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;
using NpgsqlTypes;

namespace Npgsql.Bulk.SampleRunner.DotNetStandard20.Migrations
{
    public partial class Initial : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterDatabase()
                .Annotation("Npgsql:Enum:address_type", "type1,type2")
                .Annotation("Npgsql:Enum:address_type_int", "first,second");

            migrationBuilder.EnsureSchema(
                name: "public");

            migrationBuilder.CreateTable(
                name: "addresses",
                schema: "public",
                columns: table => new
                {
                    address_id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    street_name = table.Column<string>(nullable: false),
                    house_number = table.Column<int>(nullable: false),
                    extra_house_number = table.Column<int>(nullable: true),
                    postal_code = table.Column<string>(nullable: false),
                    range = table.Column<NpgsqlRange<DateTime>>(nullable: false),
                    created_at = table.Column<DateTime>(nullable: false),
                    address_type = table.Column<AddressType>(nullable: false),
                    address_type_int = table.Column<AddressTypeInt>(nullable: false),
                    unmapped_enum = table.Column<int>(nullable: false),
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_addresses", x => x.address_id);
                });
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "addresses",
                schema: "public");
        }
    }
}
