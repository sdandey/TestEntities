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
using System.Collections.Generic;

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
        public static async Task<IActionResult> DatabricksClusterStartup(
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


            for (int i = 0; i < numberofContexts; i++)
            {
                var entityKey = "clusterId:" + clusterId + "|" + "contextId:" + i;
                var entity = new EntityId(nameof(ContextEntity), entityKey);
                await durableEntityClient.SignalEntityAsync(entity, nameof(ContextEntity.SetCluster), entityKey);
            }


            /**
               signal createContext() method for every entity Ids.
            */
            for (int i = 0; i < numberofContexts; i++)
            {
                var entityKey = "clusterId:" + clusterId + "|" + "contextId:" + i;
                var entity = new EntityId(nameof(ContextEntity), entityKey);
                durableEntityClient.SignalEntityAsync(entity, nameof(ContextEntity.CreateContext));
            }
            
            var name = "DatabricksClusterStartup";
            string responseMessage = $"Http Trigger executed successfully the function {name}";
            return new OkObjectResult(responseMessage);

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
 
            var entityId = "<<Avaialble Context Entity Id value for e.g.,  clusterId:0125-171110|contextId:1>>";

            var availableContextEntityId = new EntityId(nameof(ContextEntity), entityId);
 

            /**
                for quick testing use the following payload
                Println("Hello");

                Actual Job Submission would look like below.
                com.deloitte.cortex.sparkdataingestion.ProdApplicationContext(env).main.runActionJobResultString("job1", jobPayload, tokenString)


            **/
            String payload = "payload from Rest API POst";
            String userToken ="<Databricks PAT TOken Here>";
            await durableEntityClient.SignalEntityAsync(availableContextEntityId, nameof(ContextEntity.RunCommand), payload,userToken);

            var name = "JobProcessor";
            string responseMessage = $"Http Trigger executed successfully the function {name}";
            return new OkObjectResult(responseMessage);

        }


        /**
            Monitor context would periodically every 5 minutes call databricks API and sync the ContextState and CommandState for all the active Job Running
        */
        [FunctionName(nameof(MonitorContext))]
        public static IActionResult MonitorContext(
            [TimerTrigger("0 */5 * * * *")]TimerInfo monitorTimer,     
            [DurableClient] IDurableEntityClient durableEntityClient,
            ILogger log)
        {

            
            List<string> contextEntityIds = new List<string>();
            //use entity query model and query all the context Entity Ids.


            /*
                Update Command State will call databricks API and set the commandState as per the response 
            */
            contextEntityIds.ForEach(entityId => {
                Console.WriteLine($"Signal commandState for {entityId}");
                var entity = new EntityId(nameof(ContextEntity), entityId);
                durableEntityClient.SignalEntityAsync(entity, nameof(ContextEntity.UpdateCommandStatus));
            });

            /*
                Update Context State will call databricks API and set the ContextState as per the response 
            */

             contextEntityIds.ForEach(entityId => {
                Console.WriteLine($"Signal commandState for {entityId}");
                var entity = new EntityId(nameof(ContextEntity), entityId);
                durableEntityClient.SignalEntityAsync(entity, nameof(ContextEntity.UpdateContextState));
            });


            var name = "MonitorContext";
            string responseMessage = $"Http Trigger executed successfully the function {name}";
            return new OkObjectResult(responseMessage);

        }

    }

}





