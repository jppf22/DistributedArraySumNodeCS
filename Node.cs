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



    internal class Node
    {
        internal static readonly string[] PossibleSubjects = new[] {
            "election.*",
            "coordinator.*",
            "data.*",
            "result.*",
        };

        public int Id { get; set; }
        public string Address { get; set; }

        private SortedDictionary<int, String> peers;
        private readonly String natsServerURl;
        public NatsClient nc;
        public INatsJSContext? js = null;
        public INatsJSConsumer? consumer = null;
        private int? currentLeader = null;

        /*Note: program will always assume all possible nodes are on, 
         * however those that don't ack in return are clearly not, this way 
         * there's no need to track active nodes
        */

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
            js = nc.CreateJetStreamContext();
            await js.CreateStreamAsync(new StreamConfig(name: "DistributedSum", subjects: PossibleSubjects));

            // Create Consumer for this node
            ConsumerConfig durableConfig = new ConsumerConfig($"Node{Id}");
            durableConfig.FilterSubject = $"*.{Id}"; //Only receive messages targeted for this Node
            consumer = await js.CreateOrUpdateConsumerAsync(stream: "DistributedSum", durableConfig);
            Console.WriteLine($"Consumer created for Node {Id} with durable name: {consumer.Info.Config.DurableName}");

            // Wait for the subcriptions to start
            await Task.Delay(1000);

            // Start First Election (if no election has been started yet)
            await startElectionAsync();
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
                PubAckResponse pubAck = await js!.PublishAsync(
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
                currentLeader = Id;
                // Optionally, publish coordinator message here
                await announceCoordinator();
            }
            else
            {
                Console.WriteLine($"Node {Id} received ack, will not become coordinator.");
            }
        }

        public async Task announceCoordinator()
        {
            var coordinatorMessage = $"{Id}";
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            var publishTasks = peers.Keys.Select(async peerId =>
            {
                PubAckResponse pubAck = await js!.PublishAsync(
                    subject: $"coordinator.{peerId}",
                    data: Encoding.UTF8.GetBytes(coordinatorMessage),
                    cancellationToken: cts.Token
                );

            }).ToList();

            await Task.WhenAll(publishTasks);
            Console.WriteLine($"Node {Id} announced itself as coordinator to all peers.");
        }

        public async Task handleElectionMessageAsync(NatsJSMsg<string> msg)
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

        public async Task handleCoordinatorMessageAsync(NatsJSMsg<string> msg)
        {
            int peerId = int.Parse(msg.Data);
            Console.WriteLine($"Node {Id} received coordinator message from Node {peerId}");
            currentLeader = peerId;
        }
    }
}
