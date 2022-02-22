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


        public ContextEntity(string clusterId, string clusterState, string clusterName, string contextId, string commandId, string commandState, string contextState) 
        {
            this.ClusterId = clusterId;
    this.ClusterState = clusterState;
    this.ClusterName = clusterName;
    this.ContextId = contextId;
    this.CommandId = commandId;
    this.CommandState = commandState;
    this.ContextState = contextState;
   
        }
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

        [JsonProperty]
        public string ContextState { get; set; }


        private readonly ILogger logger;
        private readonly IDurableEntityContext context;

        private static readonly HttpClient client = new HttpClient();



        [FunctionName(nameof(ContextEntity))]
        public static Task Run([EntityTrigger] IDurableEntityContext ctx) => ctx.DispatchAsync<ContextEntity>();


        public void SetCluster(string clusterId, string ClusterName, string clusterState) {
            this.ClusterId = clusterId;
            this.ClusterName = ClusterName;
            this.ClusterState = clusterState;
        }

        public async void RunCommand(String payload, String userToken)
        {
            string url = Constants.DatabricksBaseUrl + "/1.2/commands/execute";

            //Call Databricks Rest API to run command using ClusterID and ContextId;
           // values for the command would be "com.deloitte.cortex.sparkdataingestion.ProdApplicationContext(env).main.runActionJobResultString("job1", jobPayload, tokenString)";

            //wait for 5 seoncds chagne the state
            //change the status to avaialble

            Object requestPaylod = null;
            String responseBody = await SubmitPost(url, requestPaylod);

            CommandId = "<parse response and get the command Id";
        }


        public async void UpdateCommandStatus()
        {
            //Call Databricks Rest API to getCommandStatus and set the commandState accordingly
            string url = Constants.DatabricksBaseUrl + "/1.2/commands/status";
            
            //TODO: Create Request Payload
            Object requestPayload = null;
            String responseBody = await SubmitPost(url, requestPayload);

            //TODO Parse responseBody and set the commandStatus
            CommandState = "Parse response to get sattus";

        }

        public void UpdateClusterState()
        {
            //Call Databricks Rest API to getClusterStatus and set the ClusterState accordingly

        }

        public async void UpdateContextState()
        {
            //call databricks Rest API to getContextState for the given clusterId and contextId and set the state accordingly

            string url = Constants.DatabricksBaseUrl + "/1.2/contexts/status";
            
            //TODO: Create Request Payload
            Object requestPayload = null;
            String responseBody = await SubmitPost(url, requestPayload);

            //TODO Parse responseBody and set the commandStatus
            ContextState = "Parse response to get context state";
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

            this.ContextId = "Returnd for Response";
            this.ContextState = "<<RUNNING>>";
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
