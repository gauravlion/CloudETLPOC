using Microsoft.WindowsAzure.Storage.Table;

namespace ETLModel
{
    public class ETLSchema : TableEntity
    {
        public string Data{ get; set;}

        public ETLSchema(){}
 
        public ETLSchema(string rowKey, string publisher)
        {
            this.RowKey = RowKey;
            this.PartitionKey = publisher;
        }
    }
}