using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using LogParser.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace LogParser {
    
    //--- Classes ---
    public class Function {
    
        //--- Fields ---
        private readonly IAmazonS3 _s3Client;
        private readonly string _bucket;
        private readonly Random _random = new Random();
        
        //--- Constructors ---
        public Function() {
            _s3Client = new AmazonS3Client();
            _bucket = System.Environment.GetEnvironmentVariable("bucket");
        }

        //--- Methods ---
        [LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]
        public void Handler(CloudWatchLogsEvent cloudWatchLogsEvent, ILambdaContext context) {
            
            // Level 1: decode and decompress data
            var logsData = cloudWatchLogsEvent.AwsLogs.Data;
            Console.WriteLine($"THIS IS THE DATA: {logsData}");
            var decompressedData = DecompressLogData(logsData);
            Console.WriteLine($"THIS IS THE DECODED, UNCOMPRESSED DATA: {decompressedData}");
            
            // Level 2: Parse log records
            var athenaFriendlyJson = ParseLog(decompressedData);

            // Level 3: Save data to S3
            PutObject(athenaFriendlyJson);

            // Level 4: Create athena schema to query data
        }
        
        public string DecompressLogData(string value) {
            var bytes = Convert.FromBase64String(value);
            var source = new MemoryStream(bytes);
            var destination = new MemoryStream();
            using(var compressedStream = new GZipStream(source, CompressionMode.Decompress)) {
                compressedStream.CopyTo(destination);
            }
            var data = Encoding.UTF8.GetString(destination.ToArray());
            LambdaLogger.Log("*** DATA: " + data + "\n");
            return data;
        }

        private IEnumerable<string> ParseLog(string data) {
            var json = JsonConvert.DeserializeObject<JObject>(data);
            foreach(var entry in json["logEvents"]) {
                yield return (string)entry["message"];
            }
        }

        public void PutObject(IEnumerable<string> values) {
            var payload = new StringBuilder();
            foreach(var value in values) {
                var matches = Regex.Matches(value, "\\(([^\\)]*)\\)").Cast<Match>().Select(match => match.Value).ToArray();
                var json = new {
                    user_name = Extract(matches[0]),
                    friends = Extract(matches[4]),
                    date_created = Extract(matches[9])
                };
                var serialized = JsonConvert.SerializeObject(json, Formatting.None);
                // LambdaLogger.Log(serialized + "\n");
                payload.AppendLine(serialized);
            }
            _s3Client.PutObjectAsync(new PutObjectRequest {
                BucketName = _bucket,
                Key = "data_" + _random.Next() + ".json",
                InputStream = new MemoryStream(Encoding.UTF8.GetBytes(payload.ToString()))
            }).Wait();

            // local functions
            string Extract(string v) => v.Substring(1, v.Length - 2);
        }
    }
}
