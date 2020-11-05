using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Npgsql.Bulk.DAL;
using NpgsqlTypes;

namespace Npgsql.Bulk.SampleRunner.DotNetStandard20.DAL
{
    public enum AddressType
    {
        Type1,
        Type2,
    }
    public enum AddressTypeInt
    {
        First = 0,
        Second = 1,
    }

    public enum UnmappedEnum
    {
        A = 0,
        B = 1,
    }

    [Table("addresses", Schema = "public")]
    public class Address: IHasId, IHasId2
    {
        [Column("address_id"), Key(), DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int AddressId { get; set; }

        [NotMapped]
        int IHasId2.AddressId { get; set; }

        [Column("street_name"), Required()]
        public string StreetName { get; set; }

        [Column("house_number")]
        public int HouseNumber { get; set; }

        [Column("extra_house_number")]
        public int? ExtraHouseNumber { get; set; }

        [Column("postal_code"), Required()]
        public string PostalCode { get; set; }

        [Column("range")]
        public NpgsqlRange<DateTime> Duration { get; set; }

        [Column("interval")]
        public TimeSpan Interval { get; set; }

        [Column("created_at")]
        public DateTime CreatedAt { get; set; }

        [Column("address_type")]
        public AddressType AddressType { get; set; }

        [Column("address_type_int")]
        public AddressTypeInt AddressTypeInt { get; set; }

        [Column("unmapped_enum")]
        public UnmappedEnum UnmappedEnum { get; set; }

        //[Column("hi_lo")]
        //public int HiLo { get; set; }

    }
}
