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
    public class DistributedSum
    {
        /*
        args:
        - this node's id
        - this node's address
        - The path to the peers file
         */
        static async Task Main(string[] args)
        {

            const int heartbeat_interval = 100; // Heartbeat interval in milliseconds
            const int heartbeat_check_interval = 100; // Interval to check for heartbeat in milliseconds
            const int heartbeat_timeout = 10; // Heartbeat timeout in seconds

            /*TODO: ADD Command Line arguments error handling*/
            int node_id = int.Parse(args[0]);
            string node_address = args[1];
            string peers_file_path = args[2];
            string natsServerURl = args[3]; //Change this to your NATS server URL

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) =>
            {
                Console.WriteLine("Shutting down...");
                cts.Cancel();
                e.Cancel = true;
            };

            Node node = new Node(node_id, node_address, peers_file_path, natsServerURl, cts);
            await node.startNodeAsync();

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

                while (!cts.Token.IsCancellationRequested)
                {
                    if (node.Id == node.currentCoordinatorId) // Coordinator
                    {
                        Console.WriteLine($"Node {node.Id} is the coordinator. Sending heartbeats...");
                        while (node.Id == node.currentCoordinatorId && !cts.Token.IsCancellationRequested)
                        {
                            try
                            {
                                await node.nc.PublishAsync($"heartbeat.{node.Id}", Encoding.UTF8.GetBytes($"{node.Id}"), cancellationToken: cts.Token);
                                await Task.Delay(heartbeat_interval, cts.Token);
                            }
                            catch (OperationCanceledException)
                            {
                                break;
                            }
                        }
                    }
                    else if (node.currentCoordinatorId.HasValue) // Worker
                    {
                        int? lastCoordinator = node.currentCoordinatorId;
                        string heartbeatSubject = $"heartbeat.{node.currentCoordinatorId}";
                        DateTime lastHeartbeat = DateTime.UtcNow;

                        using var heartbeatCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
                        var heartbeatTask = Task.Run(async () =>
                        {
                            await foreach (var msg in node.nc.SubscribeAsync<byte[]>(heartbeatSubject, cancellationToken: heartbeatCts.Token))
                            {
                                lastHeartbeat = DateTime.UtcNow;
                                // Optionally log: Console.WriteLine($"Received heartbeat from coordinator {node.currentCoordinatorId} at {lastHeartbeat}");
                            }
                        }, heartbeatCts.Token);

                        // Monitor for missed heartbeats
                        while (node.currentCoordinatorId == lastCoordinator && !cts.Token.IsCancellationRequested)
                        {
                            if ((DateTime.UtcNow - lastHeartbeat).TotalSeconds > heartbeat_timeout)
                            {
                                Console.WriteLine($"No heartbeat from coordinator {lastCoordinator} for more than {heartbeat_timeout} seconds. Starting election.");
                                heartbeatCts.Cancel();

                                if (!node.IsElectionOngoing)
                                {
                                    // Avoid election storm
                                    await Task.Delay(new Random().Next(100, 300));
                                    await node.startElectionAsync();
                                }
                                break;
                            }
                            await Task.Delay(heartbeat_check_interval, cts.Token);
                        }

                        heartbeatCts.Cancel();
                        try { await heartbeatTask; } catch { /* ignore */ }
                    }
                    else
                    {
                        // No coordinator known, wait and retry
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
