using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace DistributedArraySumNodeCS
{
    internal class DistributedSum
    {
        /*
        args:
        - this node's id
        - this node's address
        - The path to the peers file
         */
        static async Task Main(string[] args)
        {
            /*TODO: ADD Command Line arguments error handling*/
            int node_id = int.Parse(args[0]);
            string node_address = args[1];
            string peers_file_path = args[2];
            string natsServerURl = "100.82.241.104"; //Change this to your NATS server URL

            Node node = new Node(node_id, node_address, peers_file_path, natsServerURl);
            await node.startNodeAsync();

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) =>
            {
                Console.WriteLine("Shutting down...");
                cts.Cancel();
                e.Cancel = true;
            };

            // Start parallel tasks for each subscription
            Task election_subscription = Task.Run(async () =>
                {
                    await foreach (INatsMsg<String> msg in node.nc.SubscribeAsync<String>($"election.{node_id}", cancellationToken: cts.Token))
                    {
                        await node.handleElectionMessageAsync(msg);
                    }
                }
            );

            Task coordinator_subscription = Task.Run(async () =>
                {
                    await foreach (INatsMsg<String> msg in node.nc.SubscribeAsync<String>($"coordinator.{node_id}", cancellationToken: cts.Token))
                    {
                        await node.handleCoordinatorMessageAsync(msg);
                    }
                }
            );

            Task register_subscription = Task.Run(async () =>
                {
                    await foreach (INatsMsg<String> msg in node.nc.SubscribeAsync<String>($"register.{node_id}", cancellationToken: cts.Token))
                    {
                        await node.handleRegisterMessageAsync(msg);
                    }
                }
            );

            // TODO - IMPROVEMENT: Add a signal that fires whenever current node becomes coordinator and then subscribe to the array topic
            Task array_subscription = Task.Run(async () =>
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        if (node.Id == node.currentCoordinatorId) 
                        {
                            await foreach (INatsMsg<byte[]> msg in node.nc.SubscribeAsync<byte[]>("array", cancellationToken: cts.Token))
                            {
                                await node.handleArrayRequest(msg);
                            }
                        }
                        else
                        {
                            // Wait and check again after a short delay
                            await Task.Delay(100, cts.Token);
                        }
                    }
                }
            );

            await Task.Delay(1000); // Give some time for subscriptions to be established

            // Start the election process
            await node.startElectionAsync();

            // Wait for the tasks to complete (they won't, but this keeps the main thread alive)
            try
            {
                await Task.WhenAll(election_subscription, coordinator_subscription, array_subscription);
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation gracefully
                Console.WriteLine("Node shutdown complete.");
            }
        }
    }
}
