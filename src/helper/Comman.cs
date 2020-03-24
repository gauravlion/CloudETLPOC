using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure; //Namespace for CloudConfigurationManager
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Extensions.Configuration;
using System.Text;  
using ETLModel;


namespace ETLHelper
{
    public class TableStorageUtility
    {
        private static readonly string _rowID = "rowID";
        private static readonly string _data = "data";
        private static readonly string _PartitionKey = "1";
        private string _configName = "AzureWebJobsStorage";
        private static readonly string _tableName = "ETLSchema";
        private static readonly string _fileName = "Data.csv";

        public FileContentResult MainFlow(HttpRequest req)
        {
            var storageAccount = GetCloudStorageAccountInfoThroughConfig(_configName);
            
            var table  = GetCloudTable(_tableName, storageAccount );

            var tableValue = GetTableInfo(table.Result, req);

            if(tableValue == null || string.IsNullOrWhiteSpace(tableValue?.Result?.RowKey))
            {
                tableValue = InsertInTableInfo(table.Result, req);
            }

            return GenerateCsvFile(tableValue);
        }

        public CloudStorageAccount GetCloudStorageAccountInfoThroughConfig(string configName)
        {
            try
            {
               return CloudStorageAccount.Parse(Environment.GetEnvironmentVariable(configName)??string.Empty);
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                throw;
            }
        }

        public async Task<CloudTable> GetCloudTable(string tableName, CloudStorageAccount cloudStorageAccount)
        {

            // Create a table client for interacting with the table service
            CloudTableClient tableClient = cloudStorageAccount.CreateCloudTableClient();

            // Create a table client for interacting with the table service 
            CloudTable table = tableClient.GetTableReference(tableName);
            try
            {
                if (await table.CreateIfNotExistsAsync())
                {
                    Console.WriteLine("Created Table named: {0}", tableName);
                }
                else
                {
                    Console.WriteLine("Table {0} already exists", tableName);
                }
            }
            catch (StorageException)
            {
                Console.WriteLine("If you are running with the default configuration please make sure you have started the storage emulator. Press the Windows key and type Azure Storage to select and run it from the list of applications - then restart the sample.");
                throw;
            }

            return table;
        }
  
        public async Task<ETLSchema> GetTableInfo(CloudTable table, HttpRequest req)
        {
            // Read operation: query an entity.
            ETLSchema customerRead = null;
            try
            {
                req.GetQueryParameterDictionary().TryGetValue(_rowID,out var rowID);

                if(string.IsNullOrWhiteSpace(rowID))
                    return null;

                var customer = new ETLSchema()
                { PartitionKey = _PartitionKey, RowKey = rowID};

                var retrieveOperation = TableOperation.Retrieve<ETLSchema>(customer.PartitionKey,
                customer.RowKey);//,
                //data);

                TableResult result = await table.ExecuteAsync(retrieveOperation);
                customerRead = result.Result as ETLSchema;

                Console.WriteLine("Read operation succeeded");
                Console.WriteLine();
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 403)
                {
                    Console.WriteLine("Read operation failed for SAS");
                    Console.WriteLine("Additional error information: " + e.Message);
                }
                else
                {
                    Console.WriteLine(e.Message);
                    throw;
                }
            }

            return customerRead;
        }

        public async Task<ETLSchema> InsertInTableInfo(CloudTable table, HttpRequest req)
        {
            // Read operation: query an entity.
            ETLSchema customerRead = null;
            try
            {
                //string rowID = req.Query[_rowID];
                req.GetQueryParameterDictionary().TryGetValue(_rowID,out var rowID);

                if(string.IsNullOrWhiteSpace(rowID))
                    return null;

                req.GetQueryParameterDictionary().TryGetValue(_data,out var data);

                var customer = new ETLSchema()
                { PartitionKey = _PartitionKey, RowKey = rowID, Data = data};
               
                var retrieveOperation = TableOperation.Insert(customer);

                TableResult result = await table.ExecuteAsync(retrieveOperation);
                customerRead = result.Result as ETLSchema;

                Console.WriteLine("Read operation succeeded");
                Console.WriteLine();
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 403)
                {
                    Console.WriteLine("Read operation failed for SAS");
                    Console.WriteLine("Additional error information: " + e.Message);
                }
                else
                {
                    Console.WriteLine(e.Message);
                    throw;
                }
            }

            return customerRead;
        }

        public FileContentResult GenerateCsvFile(Task<ETLSchema> tableValue)
        {
            var getResult = tableValue.Result;
            StringBuilder csv = new  StringBuilder();

            csv.Append("Row ID,Time Stamp,Data");
            csv.Append($"{Environment.NewLine}{getResult?.RowKey},{getResult?.Timestamp},{getResult?.Data}");

            byte[] filebytes = Encoding.UTF8.GetBytes(csv.ToString());

            return new FileContentResult(filebytes, "application/octet-stream") {
                FileDownloadName = _fileName
            };
        }
    }
}