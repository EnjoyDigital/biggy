using System;

namespace Biggy.Elasticsearch.Tests
{
    public class Widget
    {
        public long Id { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }

        public int Size { get; set; }

        public DateTime Expiration { get; set; }

        public decimal Price { get; set; }
    }
}