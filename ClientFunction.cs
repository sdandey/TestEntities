using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using TestEntities;
using System.Threading;

namespace Company.Function
{
    public static class ClientFunction
    {
       



        /**
        
            The purpose of this function is for MVP purpose only,
            For MVP,  we configure a pre-defined static cluster and use the pre-defined context to submit jobs.
            Post MVP, Context and Cluster should be auto-created on-demand based on the request load.
        */    

        [FunctionName(nameof(DatabricksClusterStartup))]
        public static async Task DatabricksClusterStartup(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            [DurableClient] IDurableEntityClient durableEntityClient,
            ILogger log)
        {


            /**
                use the static variable for MVP purpose,
                this implement would change to create on-demand cluster and context based on job load.
            */    
            var clusterId = req.Query["clusterId"];
            var clusterName = req.Query["clusterName"];
            
            
            /*
            read number of pre-created context from Rest API get Parameter
            */
            var numberofContexts = Int32.Parse(req.Query["numberOfContexts"]);


            /*
                1. check the status of clusterId
                2. if clusterId is other than RUNNING, TERMINATING, RESIZING
                    2.1 - use Databricks 2.0 API to start CLuster
                    2.2 - poll-in every 10 seconds for the status
                    2.3 if (STATUS is RUNNING | RESIZING )
                    2.4  Break and set the clusterState = "RUNNING"
                3. 
            */

            string clusterState = "<<clusterState Returned from Databricks API Call>>";


            /*
            The purpose of this loop is to create n number of context entity with static pre-defined clusterId, clusterName, clusterState set
            for each contexts.

            This should be call-in wait for all the objects to be created
            */

            var entityKey = "";

            for (int i = 0; i < numberofContexts; i++)
            {
                entityKey = "clusterId:" + clusterId + "-" + "contextId:" + i;
                var entity = new EntityId(nameof(ContextEntity), );
                await durableEntityClient.SignalEntityAsync(entity, nameof(ContextEntity.SetCluster), entityKey);
            }


            /**
               signal createContext() method for every entity Ids.
            */
            for (int i = 0; i < numberofContexts; i++)
            {
                entityKey = "clusterId:" + clusterId + "-" + "contextId:" + i;
                var entity = new EntityId(nameof(ContextEntity), entityKey);
                durableEntityClient.SignalEntityAsync(entity, nameof(ContextEntity.CreateContext));
            }
            

        }


        /*
            The purpose of the jobprocessor is to read message payload from Httptrigger.

            Note: for MVP, make sure to run the DatabricksClusterStartup HTTPTrigger function before submitting jobs.
            Prereqs:  Predefined N number of context Entities should be available before running this query.
        */

        [FunctionName(nameof(JobProcessor))]
        public static async Task<IActionResult> JobProcessor(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req, CancellationToken cancellationToken,
            [DurableClient] IDurableEntityClient durableEntityClient,
            ILogger log)
        {

           //query all the contexts created
            //filter it based on CommandState = 'FINISHED' and ContextState for 'AVAILABLE'

            //leverage EntityQuery and fetch the results
            // EntityQuery query = new EntityQuery();
            // var entityQueryResult = await durableEntityClient.ListEntitiesAsync(new EntityQuery(), cancellationToken);    
 

 
            String payload = "payload from Rest API POst";
            String userToken ="<PAT TOken Here>";
            var entity = new EntityId(nameof(ContextEntity), "<ClusterID>-<ContextID>");
            await durableEntityClient.SignalEntityAsync(entity, nameof(ContextEntity.RunCommand), payload,userToken);

            var name = "JobProcessor";
            string responseMessage = string.IsNullOrEmpty(name)
               ? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
               : $"Hello, {name}. This HTTP triggered function executed successfully.";
            return new OkObjectResult(responseMessage);

        }

        [FunctionName(nameof(MonitorContext))]
        public static IActionResult MonitorContext(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            [DurableClient] IDurableEntityClient durableEntityClient,
            ILogger log)
        {
            var name = "MonitorContext";
            string responseMessage = string.IsNullOrEmpty(name)
               ? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
               : $"Hello, {name}. This HTTP triggered function executed successfully.";
            return new OkObjectResult(responseMessage);

        }

    }

}





