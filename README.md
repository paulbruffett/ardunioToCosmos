The Azure Function that connects the IoT Hub blob storage to Cosmos DB.

Every 5 minutes looks for files in blob, reads to temp storage, parses JSON and commits it to cosmosDB, deletes files from blob when done.