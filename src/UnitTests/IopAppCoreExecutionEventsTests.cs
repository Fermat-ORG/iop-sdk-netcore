using System;
using IopAppCore.ExecutionEvents;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Xunit;
using IopCommon;
using System.Collections.Generic;
using System.Threading;

namespace UnitTests
{
  /// <summary>
  /// Tests for IopAppCore.ExecutionEvents.
  /// </summary>
  public class IopAppCoreExecutionEventsTests
  {
    /// <summary>Class logger.</summary>
    private static Logger log = new Logger("UnitTests.IopAppCoreExecutionEventsTests");

    /// <summary>Random number generator.</summary>
    private static Random rng = new Random();

    /// <summary>
    /// Tests if ExecutionEvents are correctly created.
    /// This test uses two producers and one consumer thread to create a series of different
    /// events and then checks whether the events were as expected.
    /// </summary>
    [Fact]
    public async void ProducerConsumerTest()
    {
      log.Trace("()");
      History.Clear();
      History.SetMaxContexts(1000);
      ContextEvents.SetMaxEventsPerContext(1000);

      ConcurrentQueue<string> queue = new ConcurrentQueue<string>();
      Task producer1 = ProducerConsumerTest_TaskProducer("Producer", queue);
      Task producer2 = ProducerConsumerTest_TaskProducer("Producer", queue);

      await producer1;
      await producer2;

      Task consumer = ProducerConsumerTest_TaskConsumer(queue);

      await consumer;

      ContextEvents producer1Events = History.GetContextEvents(Context.CreateId("Producer", 1));
      ContextEvents producer2Events = History.GetContextEvents(Context.CreateId("Producer", 2));
      ContextEvents consumerEvents = History.GetContextEvents(Context.CreateId("Consumer", 1));

      List<ContextEvents> producer1SubtasksEvents = new List<ContextEvents>();
      for (int i = 0; i < 5; i++)
        producer1SubtasksEvents.Add(History.GetContextEvents(Context.CreateId(producer1Events.Context.Id + " Subtask", i + 1)));

      List<ContextEvents> producer2SubtasksEvents = new List<ContextEvents>();
      for (int i = 0; i < 5; i++)
        producer2SubtasksEvents.Add(History.GetContextEvents(Context.CreateId(producer2Events.Context.Id + " Subtask", i + 1)));

      string producer1EventsString = producer1Events.ToString();
      string producer2EventsString = producer2Events.ToString();
      string consumerEventsString = consumerEvents.ToString();

      Context expectedProducer1Context = new Context("Producer", 1);

      ContextEvents expectedProducer1ContextEvents = new ContextEvents(expectedProducer1Context);
      expectedProducer1ContextEvents.AddEvent(new ExecutionEvent("Start"));
      for (int i = 0; i < 5; i++)
      {
        string subtaskId = Context.CreateId(expectedProducer1Context.Id + " Subtask", i + 1);
        expectedProducer1ContextEvents.AddEvent(new ExecutionEvent($"Produced {subtaskId}"));
      }
      expectedProducer1ContextEvents.AddEvent(new ExecutionEvent("End"));
      Assert.Equal(expectedProducer1ContextEvents.ToString(), producer1EventsString);
      log.Debug("Producer 1 events:\n{0}", producer1Events.ToString("T"));



      Context expectedProducer2Context = new Context("Producer", 2);

      ContextEvents expectedProducer2ContextEvents = new ContextEvents(expectedProducer2Context);
      expectedProducer2ContextEvents.AddEvent(new ExecutionEvent("Start"));
      for (int i = 0; i < 5; i++)
      {
        string subtaskId = Context.CreateId(expectedProducer2Context.Id + " Subtask", i + 1);
        expectedProducer2ContextEvents.AddEvent(new ExecutionEvent($"Produced {subtaskId}"));
      }
      expectedProducer2ContextEvents.AddEvent(new ExecutionEvent("End"));
      Assert.Equal(expectedProducer2ContextEvents.ToString(), producer2EventsString);
      log.Debug("Producer 2 events:\n{0}", producer2Events.ToString("T"));



      log.Debug("Consumer events:\n{0}", consumerEvents.ToString("T"));

      log.Debug("Producer 1 subtask events:");
      foreach (ContextEvents events in producer1SubtasksEvents)
        log.Debug("\n{0}", events.ToString("T"));

      log.Debug("Producer 2 subtask events:");
      foreach (ContextEvents events in producer2SubtasksEvents)
        log.Debug("\n{0}", events.ToString("T"));

      log.Trace("(-)");
    }

    /// <summary>
    /// Simulates producer of specific context name, which adds 5 items to the queue.
    /// </summary>
    /// <param name="Name">Name of the producer.</param>
    /// <param name="Queue">Queue for new items.</param>
    private async Task ProducerConsumerTest_TaskProducer(string Name, ConcurrentQueue<string> Queue)
    {
      log.Trace("(Name:{0})", Name);
      Context producerContext = Context.Create(Name);
      producerContext.AddEvent("Start");

      for (int i = 0; i < 5; i++)
      {
        Context subtaskContext = Context.Create(producerContext.Id + " Subtask");

        int delay = rng.Next(50);
        await Task.Delay(delay);

        subtaskContext.AddEvent("Produced");
        Queue.Enqueue(subtaskContext.Id);

        producerContext.AddEvent($"Produced {subtaskContext.Id}");
      }

      producerContext.AddEvent("End");
      log.Trace("(-)");
    }


    /// <summary>
    /// Simulates consumer of items in the queue.
    /// </summary>
    /// <param name="Queue">Queue of items.</param>
    private async Task ProducerConsumerTest_TaskConsumer(ConcurrentQueue<string> Queue)
    {
      log.Trace("()");
      Context consumerContext = Context.Create("Consumer");
      consumerContext.AddEvent("Start");

      int processedItems = 0;
      while (processedItems < 10)
      {
        string subtaskContextId;
        if (Queue.TryDequeue(out subtaskContextId))
        {
          Context ctx = Context.GetAndSetCurrentContext(subtaskContextId);
          await ProducerConsumerTest_ConsumeCurrent();
          consumerContext.AddEvent($"Consumed {ctx.Id}");
        }
        int delay = rng.Next(50);
        await Task.Delay(delay);
        processedItems++;
      }

      consumerContext.AddEvent("End");
      log.Trace("(-)");
    }


    /// <summary>
    /// Dummy processing function for each item.
    /// </summary>
    private async Task ProducerConsumerTest_ConsumeCurrent()
    {
      log.Trace("()");
      int delay = rng.Next(50);
      await Task.Delay(delay);

      Context ctx = Context.CurrentContext.Value;
      ctx.AddEvent("Consumed");
      log.Trace("(-)");
    }



    /// <summary>
    /// Tests if History.maxContexts and History.maxEventsPerContext are working correctly.
    /// The test first sets the new limit and then creates more contexts than the limit allows
    /// and checks that the final history is as expected. Similarly it tests the maximal number 
    /// of events per context.
    /// </summary>
    [Fact]
    public void LimitTest()
    {
      log.Trace("()");
      History.Clear();
      History.SetMaxContexts(3);
      ContextEvents.SetMaxEventsPerContext(4);

      Context context1 = Context.Create("First");
      context1.AddEvent("1");
      context1.AddEvent("2");
      context1.AddEvent("3");
      context1.AddEvent("4");
      context1.AddEvent("5");
      context1.AddEvent("6");
      context1.AddEvent("7");

      // First 3 events can not exist anymore, last event has to exist.
      ContextEvents context1Events = History.GetContextEvents(context1.Id);
      Assert.InRange(context1Events.Events.Count, 1, 4);
      bool lastExists = false;
      foreach (ExecutionEvent ee in context1Events.Events)
      {
        Assert.NotEqual(ee.Name, "1");
        Assert.NotEqual(ee.Name, "2");
        Assert.NotEqual(ee.Name, "3");
        lastExists = ee.Name == "7";
      }
      Assert.Equal(true, lastExists);

      Context context2 = Context.Create("Second");
      context2.AddEvent("1");
      context2.AddEvent("2");

      Context context3 = Context.Create("Third");
      context3.AddEvent("1");
      context3.AddEvent("2");
      context3.AddEvent("3");
      context3.AddEvent("4");

      context2.AddEvent("3");

      Context context4 = Context.Create("Fourth");
      context4.AddEvent("1");
      context4.AddEvent("2");
      context4.AddEvent("3");

      // First context should be erased by now.
      Context findContext1 = History.GetContext(context1.Id);
      Assert.Equal(null, findContext1);

      // But if we add another event to it, it should exist again and third context should be gone.
      context1.AddEvent("8");
      Assert.Equal(null, History.GetContext(context3.Id));
      Assert.Equal(context1, History.GetContext(context1.Id));
      Assert.Equal(context2, History.GetContext(context2.Id));
      Assert.Equal(context4, History.GetContext(context4.Id));

      context1Events = History.GetContextEvents(context1.Id);
      Assert.InRange(context1Events.Events.Count, 1, 4);

      log.Trace("(-)");
    }


    /// <summary>
    /// Tests a remote call functionality of execution events.
    /// <para>
    /// The code creates an execution flow that is running a series of tasks on a background.
    /// At any point of time a remote call can come and demand execution of a task, but only if 
    /// that task has not been executed yet. The remote call invoker then waits for the task to 
    /// be completed.
    /// </para>
    /// <para>
    /// The test simulates several remote calls in order to check that they work as expected 
    /// in all possible cases.
    /// </para>
    /// </summary>
    [Fact]
    public async void RemoteCallTest()
    {
      log.Trace("()");
      History.Clear();
      History.SetMaxContexts(1000);
      ContextEvents.SetMaxEventsPerContext(1000);

      // Once again we will have a producer and a consumer.
      // The producer will perform a task, which ends with a creation of item 
      // that goes to the queue. Once in the queue, the item can be consumed by the consumer,
      // or an RPC can occur to consume the task. If the RPC occurs after the item 
      // is consumed by the consumer, it must do nothing and consider the task as finished.
      // If the RPC occurs before the item is consumed by the consumer, then the RPC call 
      // is the one to consume the item. Then when the consumer gets the item from the queue,
      // it has to do nothing because the task has been consumed already.
      // Instead of having RPC, we simulate RPC by having another consumer and there will be 
      // two separated queues, one for RPC and one for the consumer.

      ConcurrentQueue<string> queueConsumer = new ConcurrentQueue<string>();
      ConcurrentQueue<string> queueRpc = new ConcurrentQueue<string>();
      Task producer = RemoteCallTest_TaskProducer("Producer", queueConsumer, queueRpc);

      Task consumer = RemoteCallTest_TaskConsumer("Consumer", queueConsumer);
      Task rpc = RemoteCallTest_TaskConsumer("RPC", queueRpc);

      for (int i = 0; i < 50; i++)
      {
        string taskId = Context.CreateId("Task", i + 1);

        log.Trace("Waiting for task context '{0}' to be created.", taskId);

        // First we have to wait until the context exists.
        Context taskContext = null;
        while (taskContext == null)
        {
          taskContext = History.GetContext(taskId);
          await Task.Delay(200); 
        }

        log.Trace("Waiting for task '{0}' to be consumed.", taskId);
        bool taskCompleted = await taskContext.WaitEvent("Consuming end", Timeout.Infinite, CancellationToken.None);
        Assert.Equal(true, taskCompleted);
      }

      ContextEvents producerEvents = History.GetContextEvents(Context.CreateId("Producer", 1));
      ContextEvents consumerEvents = History.GetContextEvents(Context.CreateId("Consumer", 1));
      ContextEvents rpcEvents = History.GetContextEvents(Context.CreateId("RPC", 1));

      log.Debug("Producer events:\n{0}", producerEvents.ToString("T"));
      log.Debug("Consumer events:\n{0}", consumerEvents.ToString("T"));
      log.Debug("RPC events:\n{0}", consumerEvents.ToString("T"));

      log.Trace("(-)");
    }


    /// <summary>
    /// Simulates producer of specific context name, which adds 50 items to the queue.
    /// </summary>
    /// <param name="Name">Name of the producer.</param>
    /// <param name="QueueConsumer">Queue for new items for the consumer.</param>
    /// <param name="QueueRpc">Queue for new items for the RPC.</param>
    private async Task RemoteCallTest_TaskProducer(string Name, ConcurrentQueue<string> QueueConsumer, ConcurrentQueue<string> QueueRpc)
    {
      log.Trace("(Name:{0})", Name);
      Context producerContext = Context.Create(Name);
      producerContext.AddEvent("Start");

      for (int i = 0; i < 50; i++)
      {
        Context subtaskContext = Context.Create($"Task");

        int delay = rng.Next(50);
        await Task.Delay(delay);

        subtaskContext.AddEvent("Produced");
        QueueConsumer.Enqueue(subtaskContext.Id);
        QueueRpc.Enqueue(subtaskContext.Id);

        producerContext.AddEvent($"Produced '{subtaskContext.Id}'");
      }

      producerContext.AddEvent("End");
      log.Trace("(-)");
    }

    /// <summary>
    /// Simulates consumer of items in the queue.
    /// </summary>
    /// <param name="Name">Name of the consumer.</param>
    /// <param name="Queue">Queue of items.</param>
    private async Task RemoteCallTest_TaskConsumer(string Name, ConcurrentQueue<string> Queue)
    {
      log.Trace("(Name:{0})", Name);
      Context consumerContext = Context.Create(Name);
      consumerContext.AddEvent("Start");

      int processedItems = 0;
      while (processedItems < 50)
      {
        string subtaskContextId;
        if (Queue.TryDequeue(out subtaskContextId))
        {
          Context ctx = Context.GetAndSetCurrentContext(subtaskContextId);
          await RemoteCallTest_ConsumeItem(Name);
          consumerContext.AddEvent($"Consumed '{ctx.Id}'");
        }
        int delay = rng.Next(50);
        await Task.Delay(delay);
        processedItems++;
      }

      consumerContext.AddEvent("End");
      log.Trace("(-)");
    }


    /// <summary>
    /// Dummy processing function for each item.
    /// Ensures that the task is only consumed if it has not been consumed already by the other consumer.
    /// </summary>
    /// <param name="Name">Name of the consumer.</param>
    private async Task RemoteCallTest_ConsumeItem(string ConsumerName)
    {
      log.Trace("(ConsumerName:{0})", ConsumerName);
      int delay = rng.Next(50);
      await Task.Delay(delay);

      Context ctx = Context.CurrentContext.Value;
      if (ctx.AddExclusiveEvent("Consuming start"))
      {
        ctx.AddEvent($"Consumed by {ConsumerName}");
        ctx.AddEvent("Consuming end");
      }
      else log.Trace("{} can not consume task '{0}' as it has been consumed already.", ConsumerName);

      log.Trace("(-)");
    }

  }
}