using Microsoft.Azure.Cosmos;
using System.Text;
using System.Text.Json;

Console.WriteLine("Starting Cosmos DB seed import...");

string? endpoint =
    Environment.GetEnvironmentVariable("COSMOS_ENDPOINT");

string? key =
    Environment.GetEnvironmentVariable("COSMOS_KEY");

if (string.IsNullOrWhiteSpace(endpoint))
{
    Console.WriteLine("ERROR: COSMOS_ENDPOINT is missing.");
    return;
}

if (string.IsNullOrWhiteSpace(key))
{
    Console.WriteLine("ERROR: COSMOS_KEY is missing.");
    return;
}

const string databaseName = "VcoDstDb";
const string containerName = "PlanningData";
const string jsonFileName =
    "vco-dst-cosmos-screen-aligned.json";

string jsonPath = Path.Combine(
    Directory.GetCurrentDirectory(),
    jsonFileName);

if (!File.Exists(jsonPath))
{
    jsonPath = Path.Combine(
        AppContext.BaseDirectory,
        jsonFileName);
}

if (!File.Exists(jsonPath))
{
    Console.WriteLine(
        $"ERROR: JSON file not found: {jsonFileName}");

    Console.WriteLine(
        $"Current folder: {Directory.GetCurrentDirectory()}");

    return;
}

try
{
    using CosmosClient client = new(
        endpoint,
        key,
        new CosmosClientOptions
        {
            ConnectionMode = ConnectionMode.Gateway
        });

    Container container = client.GetContainer(
        databaseName,
        containerName);

    ContainerResponse containerResponse =
        await container.ReadContainerAsync();

    string partitionKeyPath =
        containerResponse.Resource.PartitionKeyPath;

    Console.WriteLine(
        $"Connected to database : {databaseName}");

    Console.WriteLine(
        $"Connected to container: {containerName}");

    Console.WriteLine(
        $"Partition key path    : {partitionKeyPath}");

    if (partitionKeyPath != "/planningCycleId")
    {
        Console.WriteLine(
            "ERROR: Expected partition key /planningCycleId.");

        return;
    }

    string json =
        await File.ReadAllTextAsync(jsonPath);

    using JsonDocument document =
        JsonDocument.Parse(json);

    if (document.RootElement.ValueKind !=
        JsonValueKind.Array)
    {
        Console.WriteLine(
            "ERROR: JSON root must be an array.");

        return;
    }

    int totalCount =
        document.RootElement.GetArrayLength();

    int successCount = 0;
    int failureCount = 0;

    Console.WriteLine($"Documents found: {totalCount}");
    Console.WriteLine();

    foreach (JsonElement element
             in document.RootElement.EnumerateArray())
    {
        string id = "UNKNOWN";
        string planningCycleId = "UNKNOWN";

        try
        {
            if (!element.TryGetProperty(
                    "id",
                    out JsonElement idElement) ||
                idElement.ValueKind != JsonValueKind.String)
            {
                throw new InvalidOperationException(
                    "Document does not have a valid string id.");
            }

            if (!element.TryGetProperty(
                    "planningCycleId",
                    out JsonElement partitionElement) ||
                partitionElement.ValueKind !=
                    JsonValueKind.String)
            {
                throw new InvalidOperationException(
                    "Document does not have a valid " +
                    "planningCycleId.");
            }

            id = idElement.GetString()!;
            planningCycleId =
                partitionElement.GetString()!;

            string rawJson =
                element.GetRawText();

            byte[] jsonBytes =
                Encoding.UTF8.GetBytes(rawJson);

            using MemoryStream payload =
                new(jsonBytes);

            using ResponseMessage response =
                await container.UpsertItemStreamAsync(
                    payload,
                    new PartitionKey(planningCycleId));

            if (response.IsSuccessStatusCode)
            {
                successCount++;

                Console.WriteLine(
                    $"SUCCESS: {id} | " +
                    $"{planningCycleId} | " +
                    $"{(int)response.StatusCode} " +
                    $"{response.StatusCode}");
            }
            else
            {
                failureCount++;

                string responseContent = string.Empty;

                if (response.Content != null)
                {
                    using StreamReader reader =
                        new(response.Content);

                    responseContent =
                        await reader.ReadToEndAsync();
                }

                Console.WriteLine(
                    $"FAILED: {id} | {planningCycleId}");

                Console.WriteLine(
                    $"Status    : " +
                    $"{(int)response.StatusCode} " +
                    $"{response.StatusCode}");

                Console.WriteLine(
                    $"ActivityId: {response.Headers.ActivityId}");

                Console.WriteLine(
                    $"Response  : {responseContent}");

                Console.WriteLine();
            }
        }
        catch (Exception ex)
        {
            failureCount++;

            Console.WriteLine(
                $"FAILED: {id} | {planningCycleId}");

            Console.WriteLine(
                $"Error: {ex.Message}");

            Console.WriteLine();
        }
    }

    Console.WriteLine();
    Console.WriteLine(
        "=================================");

    Console.WriteLine(
        "Cosmos DB seed import completed");

    Console.WriteLine(
        $"Total     : {totalCount}");

    Console.WriteLine(
        $"Successful: {successCount}");

    Console.WriteLine(
        $"Failed    : {failureCount}");

    Console.WriteLine(
        "=================================");

    Environment.ExitCode =
        failureCount == 0 ? 0 : 1;
}
catch (CosmosException ex)
{
    Console.WriteLine(
        "COSMOS DB CONNECTION ERROR");

    Console.WriteLine(
        $"Status    : {(int)ex.StatusCode} " +
        $"{ex.StatusCode}");

    Console.WriteLine(
        $"SubStatus : {ex.SubStatusCode}");

    Console.WriteLine(
        $"ActivityId: {ex.ActivityId}");

    Console.WriteLine(
        $"Message   : {ex.Message}");

    Environment.ExitCode = 1;
}
catch (JsonException ex)
{
    Console.WriteLine("JSON ERROR");
    Console.WriteLine(ex.Message);

    Environment.ExitCode = 1;
}
catch (Exception ex)
{
    Console.WriteLine("GENERAL ERROR");
    Console.WriteLine(ex.ToString());

    Environment.ExitCode = 1;
}