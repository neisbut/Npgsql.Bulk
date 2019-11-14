using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Npgsql.Bulk.DAL;
using NpgsqlTypes;

namespace Npgsql.Bulk.SampleRunner.DotNetStandard20.DAL
{
    [Table("addresses2ef", Schema = "public")]
    public class Address2EF : Address
    {
        [Column("localized_name"), Required()]
        public string LocalizedName { get; set; }

        [Column("index2")]
        public int Index2 { get; set; }
    }
}
