using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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

            await foreach (var msg in node.consumer.ConsumeAsync<String>().WithCancellation(cts.Token))
            {

                if (msg.Subject.StartsWith("election."))
                {
                    await node.handleElectionMessageAsync(msg);
                }
                else if (msg.Subject.StartsWith("coordinator."))
                {
                    await node.handleCoordinatorMessageAsync(msg);
                }

                await msg.AckAsync();
                // loop never ends unless there is a terminal error, cancellation or a break
            }
        }
    }
}
