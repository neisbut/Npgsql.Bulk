using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Npgsql.Bulk.DAL
{
    [Table("addresses", Schema = "public")]
    public class Address
    {
        [Column("address_id"), Key(), DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int AddressId { get; set; }

        [Column("street_name"), Required()]
        public string StreetName { get; set; }

        [Column("house_number")]
        public int HouseNumber { get; set; }

        [Column("extra_house_number")]
        public int? ExtraHouseNumber { get; set; }

        [Column("postal_code"), Required()]
        public string PostalCode { get; set; }

        [Column("created_at"), DatabaseGenerated(DatabaseGeneratedOption.Computed)]
        public DateTime CreatedAt { get; set; }
    }
}
