using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.Bulk.SampleRunner.DotNetStandard20.DAL;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

namespace Npgsql.Bulk.SampleRunner.DotNetStandard20.Migrations
{
    public partial class AddAddressType : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterDatabase()
                .Annotation("Npgsql:Enum:address_type", "type1,type2")
                .Annotation("Npgsql:Enum:address_type_int", "first,second");

            migrationBuilder.AddColumn<AddressType>(
                name: "address_type",
                schema: "public",
                table: "addresses",
                nullable: false,
                defaultValue: AddressType.Type1);

            migrationBuilder.AddColumn<AddressTypeInt>(
                name: "address_type_int",
                schema: "public",
                table: "addresses",
                nullable: false,
                defaultValue: AddressTypeInt.First);

            migrationBuilder.AddColumn<int>(
                name: "unmapped_enum",
                schema: "public",
                table: "addresses",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<uint>(
                name: "xmin",
                schema: "public",
                table: "addresses",
                type: "xid",
                nullable: false,
                defaultValue: 0u);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "address_type",
                schema: "public",
                table: "addresses");

            migrationBuilder.DropColumn(
                name: "address_type_int",
                schema: "public",
                table: "addresses");

            migrationBuilder.DropColumn(
                name: "unmapped_enum",
                schema: "public",
                table: "addresses");

            migrationBuilder.DropColumn(
                name: "xmin",
                schema: "public",
                table: "addresses");

            migrationBuilder.AlterDatabase()
                .OldAnnotation("Npgsql:Enum:address_type", "type1,type2")
                .OldAnnotation("Npgsql:Enum:address_type_int", "first,second");

        }
    }
}
