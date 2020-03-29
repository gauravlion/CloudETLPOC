using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace CloudPOC.Function
{
    public static class HttpEndPointTrigger
    {
        [FunctionName("HttpEndPointTrigger")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            string responseMessage = "This HTTP triggered function executed successfully.";

            log.LogInformation("C# HTTP trigger function processed a request.");
            try
            {
                var obj = new ETLHelper.TableStorageUtility().MainFlow(req);

                log.LogInformation("C# HTTP trigger function executed successfully.");

                return obj;
            }
            catch(Exception ex)
            {
                responseMessage = ex.InnerException?.ToString();
            }

            return new OkObjectResult(responseMessage);
        }
    }
}
