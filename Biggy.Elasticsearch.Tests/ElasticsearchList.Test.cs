using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;

namespace Biggy.Elasticsearch.Tests
{
    [Trait("Biggy list with Elasticsearch", "")]
    public class ElasticsearchList_Test
    {
        public ElasticsearchList_Test()
        {
            esClient.DeleteByQuery<Widget>(a => a.Index(Index).Query(q => q.MatchAll()));
        }

        private IBiggy<Widget> CreateBiggyList()
        {

            var store = new ElasticsearchStore<Widget>(Host, Index);
            var list = new BiggyList<Widget>(store);
            return list;
        }

        [Fact(DisplayName = "Elasticsearch: loads a single document into memory")]
        public void LoadSingleDocument()
        {
            var widget = new Widget()
            {
                Description = "A widget",
                Expiration = DateTime.Now.AddYears(1),
                Name = "Widget",
                Price = 9.99m,
                Size = 700
            };

            esClient.Index(widget);

            Thread.Sleep(1000);

            var widgets = CreateBiggyList();
            Assert.Equal(1, widgets.Count());

            var found = widgets.First();
            Assert.Equal(widget.Description, found.Description);
            Assert.Equal(widget.Expiration.Year, found.Expiration.Year);
            Assert.Equal(widget.Name, found.Name);
            Assert.Equal(widget.Price, found.Price);
            Assert.Equal(widget.Size, found.Size);
        }

        [Fact(DisplayName = "Elasticsearch: writes 12 metric crap-loads of records into memory and db")]
        public void WriteALot()
        {
            var data = new List<Widget>();
            for (var i = 0; i < 10000; i++)
            {
                data.Add(new Widget
                {
                    Id = i+1,
                    Description = "A widget",
                    Expiration = DateTime.Now.AddYears(1),
                    Name = "Widget",
                    Price = 9.99m,
                    Size = i
                });
            }

            var widgets = CreateBiggyList();
            widgets.Add(data);

            Thread.Sleep(1000);

            Assert.Equal(data.Count, widgets.Count());
            Assert.Equal(data.Count, esClient.Count<Widget>(a => a.Index(Index)).Count);
        }

        [Fact(DisplayName = "Elasticsearch: queries a range of records from memory")]
        public void Query()
        {
            var data = new List<Widget>();
            for (var i = 0; i < 10; i++)
            {
                data.Add(new Widget
                {
                    Description = "A widget",
                    Expiration = DateTime.Now.AddYears(1),
                    Name = "Widget",
                    Price = 9.99m,
                    Size = i
                });
            }

            var widgets = CreateBiggyList();
            widgets.Add(data);

            var query = from w in widgets
                        where w.Size > 5 && w.Size < 8
                        select w;

            Assert.Equal(2, query.Count());
        }

        [Fact(DisplayName = "Elasticsearch: Update syncs item to mongo")]
        public void Update()
        {
            var widget = new Widget()
            {
                Description = "A widget",
                Expiration = DateTime.Now.AddYears(1),
                Name = "Widget",
                Price = 9.99m,
                Size = 2
            };

            var widgets = CreateBiggyList();
            widgets.Add(widget);

            widget.Name = "I updated this!!";
            widgets.Update(widget);

            var updatedWidget = esClient.Get<Widget>(widget.Id);
            Assert.Equal(widget.Name, updatedWidget.Source.Name);

        }

        [Fact(DisplayName = "Elasticsearch: deletes a single record in memory and db")]
        public void Delete()
        {
            var widget = new Widget()
            {
                Description = "A widget",
                Expiration = DateTime.Now.AddYears(1),
                Name = "Widget",
                Price = 9.99m,
                Size = 2
            };

            var widgets = CreateBiggyList();
            widgets.Add(widget);
            widgets.Remove(widget);

            Assert.Equal(0, esClient.Count<Widget>(a => a.Index(Index)).Count);
        }

        private const string Host = "http://localhost:9200/";

        private const string Index = "biggytest";

        private readonly IElasticClient esClient = new ElasticClient(new ConnectionSettings(new Uri(Host), Index));
    }
}