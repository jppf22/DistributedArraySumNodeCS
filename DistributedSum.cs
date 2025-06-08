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

            Task array_subscription = Task.Run(async () =>
            {
                CancellationTokenSource subscriptionCts;

                while (!cts.Token.IsCancellationRequested)
                {
                    if (node.Id == node.currentCoordinatorId)
                    {
                        // Create new cancellation source for this subscription session
                        subscriptionCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);

                        _ = Task.Run(async () =>  // Monitoring task
                        {
                            while (!subscriptionCts.Token.IsCancellationRequested)
                            {
                                await Task.Delay(100);
                                if (node.Id != node.currentCoordinatorId)
                                {
                                    subscriptionCts.Cancel();
                                    break;
                                }
                            }
                        }, subscriptionCts.Token);

                        try
                        {
                            await foreach (INatsMsg<byte[]> msg in node.nc.SubscribeAsync<byte[]>(
                                "array", cancellationToken: subscriptionCts.Token))
                            {
                                await node.handleArrayRequest(msg);
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            // Expected when coordinator changes
                        }
                        finally
                        {
                            subscriptionCts.Dispose();
                        }
                    }
                    else
                    {
                        // Wait before checking coordinator status again
                        await Task.Delay(100, cts.Token);
                    }
                }
            });

            Task data_subscription = Task.Run(async () =>
                {
                    await foreach (INatsMsg<byte[]> msg in node.nc.SubscribeAsync<byte[]>($"data.{node_id}", cancellationToken: cts.Token))
                    {
                        await node.handleDataMessageAsync(msg);
                    }
                }
            );

            Task heartbeat_task = Task.Run(async () =>
            {
                CancellationTokenSource subscriptionCts;

                while (!cts.Token.IsCancellationRequested)
                {
                    if (node.Id == node.currentCoordinatorId) //Coordinator
                    {
                        // Create new cancellation source for this subscription session
                        subscriptionCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);

                        _ = Task.Run(async () =>  // Monitoring task
                        {
                            while (!subscriptionCts.Token.IsCancellationRequested)
                            {
                                await Task.Delay(100);
                                if (node.Id != node.currentCoordinatorId)
                                {
                                    subscriptionCts.Cancel();
                                    break;
                                }
                            }
                        }, subscriptionCts.Token);

                        while (!subscriptionCts.Token.IsCancellationRequested)
                        {
                            try
                            {
                                Console.WriteLine("Sending heartbeat...");
                                await node.nc.PublishAsync($"heartbeat.{node_id}", Encoding.UTF8.GetBytes($"{node_id}"), cancellationToken: subscriptionCts.Token);
                                await Task.Delay(500, subscriptionCts.Token); // Wait before sending next heartbeat
                            }
                            catch (OperationCanceledException)
                            {
                                // Expected when coordinator changes
                                break;
                            }
                        }

                        subscriptionCts.Dispose();
                    }
                    else if(node.currentCoordinatorId.HasValue)// Worker
                    {
                        subscriptionCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
                        string heartbeatSubject = $"heartbeat.{node.currentCoordinatorId}";
                        DateTime lastHeartbeat = DateTime.UtcNow;

                        var heartbeatListener = Task.Run(async() =>
                        {
                            await foreach (var msg in node.nc.SubscribeAsync<byte[]>(heartbeatSubject, cancellationToken: subscriptionCts.Token))
                            {
                                lastHeartbeat = DateTime.UtcNow;
                                Console.WriteLine($"Received heartbeat from coordinator {node.currentCoordinatorId} at {lastHeartbeat}");
                            }
                        }, subscriptionCts.Token);

                        while (!subscriptionCts.Token.IsCancellationRequested)
                        {
                            if ((DateTime.UtcNow - lastHeartbeat).TotalSeconds > 1) // If no heartbeat received in 1 second (2 heartbeats missed)
                            {
                                Console.WriteLine($"No heartbeat from coordinator {node.currentCoordinatorId} for more than 1 second. Starting election.");
                                await node.startElectionAsync();
                                break; // Exit the loop to allow for a new election
                            }
                            await Task.Delay(1000, subscriptionCts.Token); // Wait before checking again
                        }

                        subscriptionCts.Cancel();
                        subscriptionCts.Dispose();
                    }
                    else
                    {
                        // If no coordinator is set, wait before checking again
                        await Task.Delay(100, cts.Token);
                    }
                }
            });

            await Task.Delay(1000); // Give some time for subscriptions to be established

            // Start the election process
            await node.startElectionAsync();

            // Wait for the tasks to complete (they won't, but this keeps the main thread alive)
            try
            {
                await Task.WhenAll(election_subscription, coordinator_subscription, array_subscription, data_subscription, heartbeat_task);
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation gracefully
                Console.WriteLine("Node shutdown complete.");
            }
        }
    }
}
