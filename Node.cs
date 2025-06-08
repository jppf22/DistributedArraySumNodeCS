using System;
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
    internal class PeersFile
    {
        public required List<PeersJSONClass> machines { get; set; }
    }


    internal class PeersJSONClass
    {
        public required int id { get; set; }
        public required String name { get; set; }
        public required String owner { get; set; }
        public required String address { get; set; }
        public required String os { get; set; }
        public String? status { get; set; }
    }
    internal class ArrayRequest
    {
        public required int[] numbers { get; set; }
    }



    internal class Node
    {

        public int Id { get; set; }
        public string Address { get; set; }

        private SortedDictionary<int, String> peers;
        private List<int> active_workers = [];
        private readonly String natsServerURl;
        public NatsClient nc;
        public int? currentCoordinatorId = null;
        private int[] dataArray = Array.Empty<int>();
        public Node(int id, String address, String peers_file_path, String natsServerURl)
        {
            Id = id;
            Address = address;
            peers = new SortedDictionary<int, String>();
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
            Console.WriteLine($"Node {Id} starting election...");

            var electionMessage = $"{Id}";
            var higherPeerIds = peers.Keys.Where(pid => pid > Id).ToList();

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            var ackReceived = false;

            var publishTasks = higherPeerIds.Select(async peerId =>
            {
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
                    Console.WriteLine($"Node {Id} received Election Ack from {msg.Subject}\n");
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

        public async Task announceCoordinator()
        {
            var coordinatorMessage = $"{Id}";
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

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
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            await nc.PublishAsync(
                    subject: $"register.{currentCoordinatorId}",
                    data: Encoding.UTF8.GetBytes(registerMessage),
                    cancellationToken: cts.Token
            );

            Console.WriteLine($"Node {Id} registered at coordinator {currentCoordinatorId}.");


            // Wait for acknowledgment from the coordinator
            try
            {
                await foreach (NatsMsg<String> msg in nc.SubscribeAsync<String>($"register.ack.{Id}").WithCancellation(cts.Token))
                {
                    Console.WriteLine($"Node {Id} received registration ack from coordinator {currentCoordinatorId}.");
                    break; // Exit loop on successful ack
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

            if (peerId < Id)
            {
                // If peers ID is lower, this node starts its own election
                await startElectionAsync();
            }
        }

        public async Task handleCoordinatorMessageAsync(INatsMsg<String> msg)
        {
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


            // TESTING --- SUPPOSED TO DISH OUT TO REGISTERED WORKERS

            // Calculate the sum of the array
            int sum = dataArray.Sum();
            Console.WriteLine($"Node {Id} calculated sum: {sum}");
            // Publish the result to all active workers
            var resultMessage = Encoding.UTF8.GetBytes($"{sum}");

            await msg.ReplyAsync(resultMessage);
        }
    }
}
