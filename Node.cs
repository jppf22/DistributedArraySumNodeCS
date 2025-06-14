﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using NATS.Client.Core;
using NATS.Net;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using System.Text.Json;

namespace DistributedArraySumNodeCS
{
    public class PeersFile
    {
        public required List<PeersJSONClass> machines { get; set; }
    }


    public class PeersJSONClass
    {
        public required int id { get; set; }
        public required String name { get; set; }
        public required String owner { get; set; }
        public required String address { get; set; }
        public required String os { get; set; }
        public String? status { get; set; }
    }
    public class ArrayRequest
    {
        public required int[] numbers { get; set; }
    }



    public class Node
    {

        public int Id { get; set; }
        public string Address { get; set; }

        public CancellationToken GlobalCancellationToken { get; set; }

        private SortedDictionary<int, String> peers;
        private readonly String natsServerURl;
        public NatsClient nc;


        public int? currentCoordinatorId = null;
        private bool isElectionOngoing = false;
        private readonly object electionLock = new object();

        public bool IsElectionOngoing
        {
            get { lock (electionLock) { return isElectionOngoing; } }
        }

        public int[] dataArray = Array.Empty<int>();
        public List<int> active_workers = [];

        public readonly int timeout_general = 3; 

        public Node(int id, String address, String peers_file_path, String natsServerURl, CancellationTokenSource global_cts)
        {
            Id = id;
            Address = address;
            peers = new SortedDictionary<int, String>();
            GlobalCancellationToken = global_cts.Token;
            if (File.Exists(peers_file_path))
            {
                try
                {
                    String jsonString = File.ReadAllText(peers_file_path);
                    var peersFile = System.Text.Json.JsonSerializer.Deserialize<PeersFile>(jsonString);
                    if (peersFile?.machines != null)
                    {
                        Console.WriteLine($"Found {peersFile.machines.Count} peers in the file: {peers_file_path}");
                        foreach (var peer in peersFile.machines)
                        {
                            if(peer.id == Id)
                            {
                                continue; // Skip self
                            }
                            peers.Add(peer.id, peer.address);
                            Console.WriteLine($"Peer ID: {peer.id}, Address: {peer.address}, Name: {peer.name}, Owner: {peer.owner}, OS: {peer.os}"); //Missing status, but not needed for now
                        }
                    }

                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error reading peers file: {ex.Message}");
                }
            }
            this.natsServerURl = natsServerURl;
            nc = new NatsClient(natsServerURl);
        }
        public override string ToString()
        {
            return $"Node ID: {Id}, Address: {Address}";
        }

        public async Task startNodeAsync()
        {
            await nc.ConnectAsync();
        }

        public async Task startElectionAsync()
        {
            lock (electionLock)
            {
                if (isElectionOngoing) return;
                isElectionOngoing = true;
            }

            try
            {


                Console.WriteLine($"Node {Id} starting election...");
                dataArray = Array.Empty<int>(); // Reset data array
                active_workers.Clear(); // Clear active workers
                currentCoordinatorId = null; // Reset current coordinator ID

                var electionMessage = $"{Id}";
                var higherPeerIds = peers.Keys.Where(pid => pid > Id).ToList();

                // Combine the global cancellation token with the timeout
                var cts = CancellationTokenSource.CreateLinkedTokenSource(GlobalCancellationToken, new CancellationTokenSource(TimeSpan.FromSeconds(timeout_general)).Token);

                var ackReceived = false;

                var publishTasks = higherPeerIds.Select(async peerId =>
                {
                    Console.WriteLine($"Node {Id} publishing election message to Node {peerId}");
                    await nc.PublishAsync(
                        subject: $"election.{peerId}",
                        data: Encoding.UTF8.GetBytes(electionMessage),
                        cancellationToken: cts.Token
                    );
                }).ToList();

                await Task.WhenAll(publishTasks);

                // After publishing, must wait for reply from nodes
                try
                {
                    await foreach (NatsMsg<String> msg in nc.SubscribeAsync<String>($"election.ack.{Id}").WithCancellation(cts.Token))
                    {
                        Console.WriteLine($"Node {Id} received Election Ack from {msg.Data}.");
                        ackReceived = true;
                    }
                }
                catch (OperationCanceledException)
                {
                    // Timeout occurred, no ack received
                }


                if (!ackReceived)
                {
                    Console.WriteLine($"Node {Id} did not receive any acks, becoming coordinator.");
                    currentCoordinatorId = Id;
                    await announceCoordinator();
                }
                else
                {
                    // Not coordinator, clear active_workers list
                    active_workers.Clear();
                    Console.WriteLine($"Node {Id} received at least one ack, will not become coordinator.");
                }
            }
            finally
            {
                lock (electionLock)
                {
                    isElectionOngoing = false; // Reset election flag
                }
            }
        }

        public async Task announceCoordinator()
        {
            var coordinatorMessage = $"{Id}";

            // Combine the global cancellation token with the timeout
            var cts = CancellationTokenSource.CreateLinkedTokenSource(GlobalCancellationToken, new CancellationTokenSource(TimeSpan.FromSeconds(timeout_general)).Token);

            var publishTasks = peers.Keys.Select(async peerId =>
                {
                    await nc.PublishAsync(
                        subject: $"coordinator.{peerId}",
                        data: Encoding.UTF8.GetBytes(coordinatorMessage),
                        cancellationToken: cts.Token
                    );
                }).ToList();

            await Task.WhenAll(publishTasks);
            Console.WriteLine($"Node {Id} announced itself as coordinator to all peers.");
        }

        public async Task registerAtCoordinator()
        {
            var registerMessage = $"{Id}";

            // Combine the global cancellation token with the timeout
            var cts = CancellationTokenSource.CreateLinkedTokenSource(GlobalCancellationToken, new CancellationTokenSource(TimeSpan.FromSeconds(timeout_general)).Token);

            await nc.PublishAsync(
                    subject: $"register.{currentCoordinatorId}",
                    data: Encoding.UTF8.GetBytes(registerMessage),
                    cancellationToken: cts.Token
            );

            Console.WriteLine($"Node {Id} registered at coordinator {currentCoordinatorId}.");

            bool ackReceived = false;
            // Wait for acknowledgment from the coordinator
            try
            {
                await foreach (NatsMsg<String> msg in nc.SubscribeAsync<String>($"register.ack.{Id}").WithCancellation(cts.Token))
                {
                    Console.WriteLine($"Node {Id} received registration ack from coordinator {currentCoordinatorId}.");
                    ackReceived = true;
                    break; // Exit loop on successful ack
                }
                if (!ackReceived)
                {
                    Console.WriteLine($"Node {Id} did not receive registration ack from coordinator {currentCoordinatorId} (subscription ended without ack).");
                    await startElectionAsync();
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"Node {Id} did not receive registration ack from coordinator {currentCoordinatorId}.");
                await startElectionAsync(); // If no ack from coordinator, start a new election
            }    
        }

        public async Task handleElectionMessageAsync(INatsMsg<String> msg)
        {

            int peerId = int.Parse(msg.Data);
            Console.WriteLine($"Node {Id} received election message from Node {peerId}");

            // Acknowledge the election message
            await nc.PublishAsync($"election.ack.{peerId}", Encoding.UTF8.GetBytes($"{Id}"));

            currentCoordinatorId = null; // Reset current coordinator ID

            if (peerId < Id)
            {
                // Avoid election storm
                await Task.Delay(new Random().Next(100, 300));

                // If peers ID is lower, this node starts its own election
                await startElectionAsync();
            }

        }

        public async Task handleCoordinatorMessageAsync(INatsMsg<String> msg)
        {
            lock (electionLock)
            {
                isElectionOngoing = false; // Cancel any pending elections
            }


            int peerId = int.Parse(msg.Data);
            Console.WriteLine($"Node {Id} received coordinator message from Node {peerId}");
            currentCoordinatorId = peerId;

            await registerAtCoordinator();
        }

        public async Task handleRegisterMessageAsync(INatsMsg<String> msg)
        {
            int peerId = int.Parse(msg.Data);
            Console.WriteLine($"Node {Id} received register message from Node {peerId}");

            // Acknowledge the registration
            await nc.PublishAsync($"register.ack.{peerId}", Encoding.UTF8.GetBytes($"{Id}"));

            active_workers.Add(peerId);
        }

        public async Task handleArrayRequest(INatsMsg<byte[]> msg)
        {
            try
            {
                var payload = System.Text.Json.JsonSerializer.Deserialize<ArrayRequest>(msg.Data);
                dataArray = payload.numbers;
                Console.WriteLine($"Node {Id} received array request with data: {string.Join(", ", dataArray)}");
            }
            catch (JsonException ex)
            {
                Console.WriteLine($"Node {Id} failed to deserialize array request: {ex.Message}");
                return; // Exit if deserialization fails
            }

            if (active_workers.Count == 0)
            {
                Console.WriteLine($"Node {Id} has no active workers to distribute data. Calculating at coordinator...");
                int sum = dataArray.Sum();
                Console.WriteLine($"Node {Id} calculated sum at coordinator: {sum}");
                await msg.ReplyAsync(Encoding.UTF8.GetBytes($"{sum}"));
                return;
            }
            
            var workerChunks = SplitArrayPerWorkers(dataArray, active_workers);

            // Main loop: send to all, await in parallel, redistribute if needed
            var remainingChunks = new Dictionary<int, int[]>(workerChunks);
            var availableWorkers = new List<int>(active_workers);
            int totalSum = 0;

            while (remainingChunks.Count > 0 && availableWorkers.Count > 0)
            {
                var tasks = remainingChunks
                    .Where(kvp => availableWorkers.Contains(kvp.Key))
                    .Select(kvp =>
                    {
                        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeout_general));
                        return SendChunkAndAwaitReplyAsync(kvp.Key, kvp.Value, cts.Token);
                    })
                    .ToList();

                var results = await Task.WhenAll(tasks);

                // Collect successful results and failed workers
                var failedChunks = new List<int[]>();
                var succeededWorkers = new List<int>();
                foreach (var (workerId, sum) in results)
                {
                    if (sum.HasValue)
                    {
                        totalSum += sum.Value;
                        succeededWorkers.Add(workerId);
                    }
                    else
                    {
                        // Failed, add chunk for redistribution
                        failedChunks.Add(remainingChunks[workerId]);
                        availableWorkers.Remove(workerId);
                        active_workers.Remove(workerId);
                    }
                }

                // Remove succeeded workers' chunks
                foreach (var wid in succeededWorkers)
                    remainingChunks.Remove(wid);

                // Remove failed workers’ chunks before redistributing
                foreach (var failedWorker in remainingChunks.Keys.Except(availableWorkers).ToList())
                    remainingChunks.Remove(failedWorker);

                // Redistribute failed chunks among remaining workers
                if (failedChunks.Count > 0 && availableWorkers.Count > 0)
                {
                    // Flatten all failed chunks
                    var allFailedNumbers = failedChunks.SelectMany(x => x).ToArray();
                    int n = availableWorkers.Count;
                    int baseChunk = allFailedNumbers.Length / n;
                    int rem = allFailedNumbers.Length % n;
                    int idx = 0;
                    foreach (var wid in availableWorkers)
                    {
                        int sz = baseChunk + (rem-- > 0 ? 1 : 0);
                        if (sz == 0) break;
                        remainingChunks[wid] = allFailedNumbers.Skip(idx).Take(sz).ToArray();
                        idx += sz;
                    }
                }
                else
                {

                    // No available workers left, sum remaining chunks locally
                    foreach (var chunk in remainingChunks.Values)
                        totalSum += chunk.Sum();
                    
                    foreach(var chunk in failedChunks)
                        totalSum += chunk.Sum();

                    break;
                }
            }

            if(availableWorkers.Count == 0)
            {
                Console.WriteLine($"Node {Id} calculated sum at coordinator: {totalSum}");
            }
            else
            {
                Console.WriteLine($"Node {Id} calculated total sum: {totalSum} after distributing to workers: {string.Join(", ", availableWorkers)}");
            }

            await msg.ReplyAsync(Encoding.UTF8.GetBytes($"{totalSum}"));
        }

        public static Dictionary<int, int[]> SplitArrayPerWorkers(int[] dataArray, List<int> workerIds)
        {
            int totalWorkers = workerIds.Count;
            int chunkSize = dataArray.Length / totalWorkers;
            int remainder = dataArray.Length % totalWorkers;
            int start = 0;
            var workerChunks = new Dictionary<int, int[]>();
            for (int i = 0; i < totalWorkers; i++)
            {
                int currentChunkSize = chunkSize + (i < remainder ? 1 : 0);
                int[] chunk = dataArray.Skip(start).Take(currentChunkSize).ToArray();
                start += currentChunkSize;
                workerChunks[workerIds[i]] = chunk;
            }
            return workerChunks;
        }

        public async Task<(int workerId, int? sum)> SendChunkAndAwaitReplyAsync(int workerId, int[] chunk, CancellationToken token)
        {
            var chunkPayload = new ArrayRequest { numbers = chunk };
            byte[] chunkBytes = JsonSerializer.SerializeToUtf8Bytes(chunkPayload);
            string subject = $"data.{workerId}";
            string replySubject = $"data.reply.{workerId}";

            // Subscribe for this worker's reply
            var sub = nc.SubscribeAsync<byte[]>(replySubject, cancellationToken: token);

            await nc.PublishAsync<byte[]>(subject, chunkBytes, cancellationToken: token, replyTo: replySubject);
            Console.WriteLine($"Node {Id} sent chunk [{string.Join(", ", chunk)}] to worker {workerId}");

            try
            {
                await foreach (var workerMsg in sub.WithCancellation(token))
                {
                    int workerSum = int.Parse(Encoding.UTF8.GetString(workerMsg.Data));
                    Console.WriteLine($"Node {Id} received sum {workerSum} from worker {workerId}");
                    return (workerId, workerSum);
                }
            }
            catch (OperationCanceledException)
            {
            }
            Console.WriteLine($"Node {Id} timed out waiting for worker {workerId} reply.");
            return (workerId, null);
        }

        public async Task handleDataMessageAsync(INatsMsg<byte[]> msg)
        {
            try
            {
                var payload = System.Text.Json.JsonSerializer.Deserialize<ArrayRequest>(msg.Data);
                if (payload == null || payload.numbers == null)
                {
                    Console.WriteLine($"Node {Id} received invalid data message.");
                    return;
                }
                int sum = payload.numbers.Sum();
                Console.WriteLine($"Node {Id} calculated partial sum: {sum}");

                // Reply to reply subject
                await nc.PublishAsync<byte[]>($"data.reply.{Id}", data: Encoding.UTF8.GetBytes($"{sum}"));
            }
            catch (JsonException ex)
            {
                Console.WriteLine($"Node {Id} failed to deserialize data message: {ex.Message}");
            }
        }
    }
}
