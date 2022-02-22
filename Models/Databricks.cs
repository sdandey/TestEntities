using Newtonsoft.Json;
namespace TestEntities.Models
{


    class Constants 
    {
        public const string Token = "dapiab6a39735333c23cc6c5d83064346865-2";  
        public const string DatabricksBaseUrl = "https://adb-3129414294545315.15.azuredatabricks.net/";   
    }
    public class CreateContextRequest
    {
        [JsonProperty(PropertyName = "clusterId")]
        public string ClusterId { get; set; }

        [JsonProperty(PropertyName = "language")]
        public string Language { get; set; }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }


    }


    public class CreateContextResponse
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }
    }
    public enum Language{
        sql,
        python,
        scala
    }

    /**
	 * State value received from the Databricks API /api/1.2/command/status
	 */
    public enum CommandState
    {
        Cancelled,
        Cancelling,
        Error,
        Finished,
        Queued,
        Running
    }

    /**
	 * State value received from the Databricks API /api/1.2/contexts/status
	 */
    public enum ContextState
    {
        Error,
        Pending,
        Running
    }

    /**
	 * JobState stored the state of the Job submitted from the /jobs API.
	 * The enum consists of combination of values from Databricks Command Status Reponse as well
	 * Cortex specific state to capture Submitted and Queued to represent the request was received and
	 * queue internally for execution with Durable Functions
	 */
    public enum JobState
    {
        CtxSubmitted,
        CtxQueued,
        Running,
        Finished,
        Error,
        Queued,
        Cancelled,
        Cancelling
    }
    /**
	 * Databricks Cluster State received from Databricks API 2.0/clusters/get
	 */
    public enum ClusterState
    {
        PENDING,
        RUNNING,
        RESTARTING,
        RESIZING,
        TERMINATING,
        TERMINATED,
        ERROR,
        UNKNOWN
    }


}