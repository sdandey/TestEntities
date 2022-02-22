using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace JobManagement.ClientFunctions
{
    public static class HttpSample
    {
        [FunctionName("HttpSample")]
        public static async Task<IActionResult> Run(
            [Queue("outqueue"),StorageAccount("AzureWebJobsStorage")] ICollector<string> msg,
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string name = req.Query["name"];

            

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;

            if (!string.IsNullOrEmpty(name))
            {
                // Add a message to the output collection.
                msg.Add(string.Format("Name passed to the function: {0}", name));
            }

            return name != null
                ? (ActionResult)new OkObjectResult($"Hello, {name}")
                 : new BadRequestObjectResult("Please pass a name on the query string or in the request body");
        }
    }
}
