using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Data.Tables;
using Azure.Storage.Blobs.Models;
using System.Linq;

namespace Azure9_Functions_19._05
{
    public class Function1
    {
        private ILogger<Function1> logger;
        public Function1(ILogger<Function1> logger)
        {
            this.logger = logger;
        }
        [FunctionName("Set")]
        public  async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            [Table("shortUrl")] TableClient tableClient,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string href = req.Query["href"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            href = href ?? data?.href;
            if (string.IsNullOrEmpty(href)) return new OkObjectResult(new
            {
                message = $"Please enter href parameter like" +
                $"http://localhost:7032/api/set?href=https://logbook.itstep.org/#/presents"
            }) ;
            UrlKey key; 
            var res = await tableClient.GetEntityIfExistsAsync<UrlKey>("1", "Key");
            if(!res.HasValue)       
            {
                key = new UrlKey() { Id=1, PartitionKey="1", RowKey= "Key" };
                await   tableClient.UpsertEntityAsync(key);
            }
            else
            {
                key = res.Value;
            }
            int index = key.Id;
            string code="";
            string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            while(index > 0)
            {
                code += alphabet[ (index % alphabet.Length)];
                index /= alphabet.Length;
                

            }
            code = string.Join(string.Empty, code.Reverse());
            UrlData urlData = new UrlData()
            {
                RowKey = code,
                PartitionKey = code[0].ToString(),
                Url = href,
                Count = 1,
                Id= code
            };
            key.Id++;
            await tableClient.UpsertEntityAsync(urlData);
            await tableClient.UpsertEntityAsync(key);
            return new OkObjectResult(new {href,shortUrl=urlData.RowKey});
        }
        [FunctionName("Go")]
        public async Task<IActionResult> Go([HttpTrigger(AuthorizationLevel.Anonymous,"get",Route ="go/{shortUrl}")]HttpRequest req,
            string shortUrl,
            [Table("shortUrl")]TableClient tableClient,
            [Queue("counts")] IAsyncCollector<string> queue,
            ILogger logger )
        {
            if(string.IsNullOrEmpty(shortUrl))
            {
                return new BadRequestResult();
            }
            shortUrl= shortUrl.ToUpper();
            var result = await tableClient.GetEntityIfExistsAsync<UrlData>(shortUrl[0].ToString(), shortUrl);
            if (!result.HasValue)
            {
                return new BadRequestObjectResult(new {message = "there is no such shoty URL!"});
            }
            await queue.AddAsync(result.Value.RowKey);
            return new RedirectResult(result.Value.Url);
        }
        [FunctionName("ProcessQueue")]
        public async Task ProcessQueue(
            [QueueTrigger("counts")] string shortCode,
            [Table("shortUrl")] TableClient tableClient,
            ILogger loger
            )
        {
            var result = await tableClient.GetEntityIfExistsAsync<UrlData>(
                partitionKey: shortCode[0].ToString(), shortCode);
            result.Value.Count++;
            loger.LogInformation($"From Short URL: {result.Value.RowKey} redirected {result.Value.Count} times");
            await tableClient.UpsertEntityAsync(result.Value);
        }
    
    }   
}
