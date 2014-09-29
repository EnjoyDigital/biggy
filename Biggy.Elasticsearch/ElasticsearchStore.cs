using System;
using System.Collections.Generic;
using System.Linq;
using Biggy.Extensions;
using Elasticsearch.Net;
using Nest;

namespace Biggy.Elasticsearch
{
    public class ElasticsearchStore<T> : IBiggyStore<T> where T : class
    {
        private readonly ElasticClient esClient;

        public ElasticsearchStore(string host, string index)
        {
            esClient = new ElasticClient(new ConnectionSettings(new Uri(host), index));
        }

        public List<T> Load()
        {
            var scanResults = esClient.Search<T>(s => s
                .From(0)
                .Size(1)
                .MatchAll()
                .SearchType(SearchType.Scan)
                .Scroll("2s"));

            var list = new List<T>();

            var results = esClient.Scroll<T>("4s", scanResults.ScrollId);

            while (results.Documents.Any())
            {
                list.AddRange(results.Documents);
                results = esClient.Scroll<T>("4s", results.ScrollId);
            }

            return list;
        }

        public void Clear()
        {
            esClient.DeleteByQuery<T>(a => a.Type<T>());
        }

        public T Add(T item)
        {
            esClient.Index(item);
            return item;
        }

        public IList<T> Add(List<T> items)
        {
            //TODO: Benchmark this it doesn't seem any quicker than IndexMany
            //if (items.Count > 1000)
            //{
            //    var batches = items.Batch(1000);

            //    foreach (var batch in batches)
            //    {
            //        var descriptor = new BulkDescriptor();

            //        foreach (var item in batch)
            //        {
            //            descriptor.Index<T>(op => op.Document(item));
            //        }

            //        var result = esClient.Bulk(descriptor);                    
            //    }
            //}
            //else
            //{
                esClient.IndexMany(items);                
            //}

            return items;
        }

        public T Update(T item)
        {
            esClient.Index(item);
            return item;
        }

        public T Remove(T item)
        {
            esClient.Delete(item);

            return item;
        }

        public IList<T> Remove(List<T> items)
        {
            esClient.DeleteMany(items);
            return items;
        }
    }
}