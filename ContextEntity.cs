using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using TestEntities.Models;

namespace TestEntities
{
    [JsonObject(MemberSerialization.OptIn)]
    public class ContextEntity
    {


        [JsonProperty]
        public string ClusterId { get; set; }

        [JsonProperty]
        public string ClusterState { get; set; }

        [JsonProperty]
        public string ClusterName { get; set; }

        [JsonProperty]
        public string ContextId { get; set; }

        [JsonProperty]
        public string CommandId { get; set; }

        [JsonProperty]
        public string CommandState { get; set; }

        private readonly ILogger logger;
        private readonly IDurableEntityContext context;

        private static readonly HttpClient client = new HttpClient();



        [FunctionName(nameof(ContextEntity))]
        public static Task Run([EntityTrigger] IDurableEntityContext ctx) => ctx.DispatchAsync<ContextEntity>();


        public void SetCluster(string clusterId, string ClusterName) {
            this.ClusterId = clusterId;
            this.ClusterName = ClusterName;
            this.ClusterState = "RUNNING";
        }



        public async Task CreateContext()
        {

            var url = Constants.DatabricksBaseUrl + "api/1.2/contexts/create";

            var createContextRequest = new CreateContextRequest
            {
                ClusterId = this.ClusterId,
                Language = Language.scala.ToString()
            };

            Console.WriteLine("Santos " + createContextRequest.ToString());

            var response = await SubmitPost(url, createContextRequest);
            Console.WriteLine("Santosh " + response);

            // if (response.Id != null)
            // {
            //     this.ContextId = response.Id;
            // }

            this.ClusterId = "";
            this.ClusterName = "";
        }



        private async Task<String> SubmitPost(string url, object content)
        {

            var contentAsString = new StringContent(JsonConvert.SerializeObject(content), Encoding.UTF8, "text/html");

            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Add("Authorization", $"Bearer {Constants.Token}");

            var response = await client.PostAsync(url, contentAsString);
            response.EnsureSuccessStatusCode();
            if (response.IsSuccessStatusCode)
            {
                Console.WriteLine("Success");
            }

            string responseBody = await response.Content.ReadAsStringAsync();
            Console.WriteLine(responseBody);
            return responseBody;
        }
    }

 

}
