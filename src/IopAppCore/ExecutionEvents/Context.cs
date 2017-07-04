using IopCommon;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace IopAppCore.ExecutionEvents
{
  /// <summary>
  /// Execution context describes a series of execution events in a related execution flow.
  /// </summary>
  public class Context
  {
    /// <summary>Class logger.</summary>
    private static Logger clog = new Logger("IopAppCore.ExecutionEvents.Context");

    /// <summary>Maximum number of events in a single context.</summary>
    /// <remarks>If a context reaches the limit, adding new event causes erasing arbitrary number of older events.</remarks>
    private static int maxEventsPerContext = 1000;
    /// <summary>Maximum number of events in a single context.</summary>
    /// <remarks>If a context reaches the limit, adding new event causes erasing arbitrary number of older events.</remarks>
    public static int MaxEventsPerContext { get { return maxEventsPerContext; } }

    /// <summary>Lock object to protect access to contextNameCount.</summary>
    private static object contextNameCountLock = new object();

    /// <summary>
    /// List of existing context names and number of their instances.
    /// <para>With each new instance, the number is incremented and is used as a suffix of the identifier of the new instance.</para>
    /// </summary>
    private static Dictionary<string, int> contextNameCount = new Dictionary<string, int>(StringComparer.Ordinal);

    /// <summary>Async-safe execution context storage.</summary>
    public static AsyncLocal<Context> CurrentContext = new AsyncLocal<Context>();


    /// <summary>Instance logger.</summary>
    private Logger log;

    /// <summary>Unique context identifier.</summary>
    public string Id;

    /// <summary>Creation time of the context.</summary>
    private DateTime creationTime;


    /// <summary>Link to LRU list of contexts.</summary>
    private LinkedListNode<string> lruLink;
    /// <summary>Link to LRU list of contexts.</summary>
    public LinkedListNode<string> LruLink { get { return lruLink; } }


    /// <summary>Lock object to protect access to events and usedEventsNames.</summary>
    private object eventsLock = new object();

    /// <summary>List of events that occurred within this context.</summary>
    private List<ExecutionEvent> events;
    /// <summary>List of events that occurred within this context.</summary>
    public IReadOnlyList<ExecutionEvent> Events { get { return events.AsReadOnly(); } }

    /// <summary>List of event names that were added to the context already.</summary>
    /// <remarks>Note that when a new event is added and the maximal number of events per context is reached,
    /// events are deleted, but they are not removed from this list.</remarks>
    private HashSet<string> usedEventsNames;


    /// <summary>Lock object to protect access to eventWaiters.</summary>
    private object eventWaitersLock = new object();

    /// <summary>List of tasks that should be completed when a particular event is added to the context.</summary>
    private Dictionary<string, HashSet<TaskCompletionSource<bool>>> eventWaiters = new Dictionary<string, HashSet<TaskCompletionSource<bool>>>(StringComparer.Ordinal);



    /// <summary>
    /// Creates a new instance of the object.
    /// </summary>
    /// <param name="Name">Name of the context.</param>
    /// <param name="Index">Index number of the context that together with its name forms the context's identifier.</param>
    public Context(string Name, int Index)
    {
      Id = CreateId(Name, Index);
      log = new Logger("IopAppCore.ExecutionEvents.Context", $"[{Id}] ");

      log.Trace("(Name:'{0}',Index:{1})", Name, Index);

      creationTime = DateTime.UtcNow;
      events = new List<ExecutionEvent>();
      usedEventsNames = new HashSet<string>(StringComparer.Ordinal);

      log.Trace("(-)");
    }


    /// <summary>
    /// Clears static parts of the context.
    /// </summary>
    public static void Clear()
    {
      clog.Trace("()");

      contextNameCount.Clear();
      CurrentContext.Value = null;

      clog.Trace("(-)");
    }

    /// <summary>
    /// Creates new instance of an execution context.
    /// </summary>
    /// <param name="Name">Name of the context from which its ID will be constructed.</param>
    /// <returns>New context instance.</returns>
    public static Context Create(string Name)
    {
      clog.Trace("(Name:'{0}')", Name);

      int index;
      lock (contextNameCountLock)
      {
        if (!contextNameCount.TryGetValue(Name, out index)) index = 0;
        index++;
        contextNameCount[Name] = index;
      }

      Context res = new Context(Name, index);
      History.AddContext(res);
      CurrentContext.Value = res;

      clog.Trace("(-):*.Id='{0}'", res.Id);
      return res;
    }


    /// <summary>
    /// Constructs context ID given its name and index.
    /// </summary>
    /// <param name="Name">Name of the context.</param>
    /// <param name="Index">Unique index number of the context name.</param>
    /// <returns>Unique context identifier.</returns>
    public static string CreateId(string Name, int Index)
    {
      return string.Format("{0}/{1}", Name, Index);
    }


    /// <summary>
    /// Sets link to LRU list of contexts.
    /// </summary>
    /// <param name="Link">Link to LRU list of contexts.</param>
    public void SetLruLink(LinkedListNode<string> Link)
    {
      lruLink = Link;
    }



    /// <summary>
    /// Adds a new exclusive event to the context.
    /// </summary>
    /// <param name="Name">Name of the event to add.</param>
    /// <returns>true if the function succeeds, false otherwise.
    /// Adding exclusive event succeeds if the event was not added to this context before.</returns>
    public bool AddExclusiveEvent(string Name)
    {
      return AddEvent(new ExecutionEvent(Name, true));
    }

    /// <summary>
    /// Adds a new non-exclusive event to the context.
    /// </summary>
    /// <param name="Name">Name of the event to add.</param>
    /// <remarks>Adding non-exclusive event will always succeeds.</remarks>
    public void AddEvent(string Name)
    {
      AddEvent(new ExecutionEvent(Name, false));
    }


    /// <summary>
    /// Adds new event to the event list.
    /// </summary>
    /// <param name="Event">Event to add.</param>
    /// <returns>true if the function succeeds, false otherwise.
    /// Adding exclusive event succeeds if the event was not added to this context before.
    /// Adding non-exclusive event will always succeeds.</returns>
    public bool AddEvent(ExecutionEvent Event)
    {
      log.Trace("(Event.Name:'{0}')", Event.Name);

      bool res = false;
      History.UseContext(this);

      lock (eventsLock)
      {
        bool eventNameExists = usedEventsNames.Contains(Event.Name);

        // Check whether the exclusive event has already been added.
        if (Event.IsExclusive) res = !eventNameExists;
        else res = true;

        if (res)
        {
          events.Add(Event);
          if (!eventNameExists) usedEventsNames.Add(Event.Name);
          if (events.Count > maxEventsPerContext) events.RemoveRange(0, maxEventsPerContext / 2);
        }
      }

      if (res) NotifyWaiters(Event);

      log.Trace("(-):{0}", res);
      return res;
    }


    /// <summary>
    /// Finds context using its ID.
    /// </summary>
    /// <param name="Id">Context identifier.</param>
    /// <returns>Existing context or null if no context with the given identifier exists.</returns>
    public static Context GetContext(string Id)
    {
      return History.GetContext(Id);
    }

    /// <summary>
    /// Finds context using its ID and makes it the current context.
    /// </summary>
    /// <param name="Id">Context identifier.</param>
    /// <returns>Existing context or null if no context with the given identifier exists.</returns>
    public static Context GetAndSetCurrentContext(string Id)
    {
      clog.Trace("(Id:'{0}')", Id);

      Context res = History.GetContext(Id);
      CurrentContext.Value = res;

      clog.Trace("(-)");
      return res;
    }


    /// <summary>
    /// Installs a waitable task to the context so that it is possible to wait for a specific event to be created on the context.
    /// </summary>
    /// <param name="Name">Name of the event</param>
    /// <param name="TimeoutMs">Timeout in milliseconds after which the waiting fails.</param>
    /// <param name="CancellationToken">A cancellation token for the task.</param>
    /// <returns>true if the wait succeeds, false if the timeout occurs or if the cancellation token is cancelled.</returns>
    public async Task<bool> WaitEvent(string Name, int TimeoutMs, CancellationToken CancellationToken)
    {
      log.Trace("(Name:'{0}',TimeoutMs:{1})", Name, TimeoutMs);
      bool res = false;

      // Create timeout cancellation token source if the caller specified timeout.
      CancellationTokenSource timeoutTokenSource = TimeoutMs != Timeout.Infinite ? new CancellationTokenSource(TimeoutMs) : null;

      // Create a combined cancellation token source if there is at least one cancellation token - i.e. timeout cancellation, caller provided cancellation, or both.
      List<CancellationToken> cancellationTokens = new List<CancellationToken>();
      if (timeoutTokenSource != null) cancellationTokens.Add(timeoutTokenSource.Token);
      if (CancellationToken != CancellationToken.None) cancellationTokens.Add(CancellationToken);

      CancellationTokenSource cancellationTokenSource = null;
      if (cancellationTokens.Count > 0) cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationTokens.ToArray());

      bool removeWaiter = false;
      CancellationTokenRegistration? cancellationTokenRegistration = null;
      TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>();
      try
      {
        if (cancellationTokenSource != null) cancellationTokenRegistration = cancellationTokenSource.Token.Register(() => { tcs.TrySetResult(false); });

        if (AddEventWaiter(Name, tcs))
        {
          log.Trace("Waiter added and waiting for the task completion.");
          res = await tcs.Task;

          // Only remove the waiter if its task failed, otherwise the event was added, which caused
          // all its waiters to be removed.
          removeWaiter = !res;
        }
        else
        {
          log.Trace("Event exists already, no waiting.");
          res = true;
        }
      }
      catch (TaskCanceledException)
      {
        log.Debug("Waiting task for event '{0}' cancelled.");
      }
      catch (Exception e)
      {
        log.Error("Exception occurred: {0}", e.ToString());
      }

      if (removeWaiter) RemoveEventWaiter(Name, tcs);
      if (cancellationTokenRegistration != null) cancellationTokenRegistration.Value.Dispose();
      if (cancellationTokenSource != null) cancellationTokenSource.Dispose();
      if (timeoutTokenSource != null) timeoutTokenSource.Dispose();

      log.Trace("(-):{0}", res);
      return res;
    }



    /// <summary>
    /// Completes all tasks that waited for this event.
    /// </summary>
    /// <param name="Event">Event that was added.</param>
    internal void NotifyWaiters(ExecutionEvent Event)
    {
      log.Trace("(Event.Name:'{0}')", Event.Name);
      List<TaskCompletionSource<bool>> tasksToComplete = null;
      lock (eventWaitersLock)
      {
        HashSet<TaskCompletionSource<bool>> completionSources = null;
        if (eventWaiters.TryGetValue(Event.Name, out completionSources))
        {
          if (completionSources.Count > 0)
          {
            log.Trace("{0} waiters notified.", completionSources.Count);

            tasksToComplete = new List<TaskCompletionSource<bool>>(completionSources);

            // Remove all waiters from the list.
            completionSources.Clear();
          }
        }
      }

      if (tasksToComplete != null)
      {
        foreach (TaskCompletionSource<bool> tcs in tasksToComplete)
          tcs.TrySetResult(true);
      }

      log.Trace("(-)");
    }


    /// <summary>
    /// Adds waiter for a specific event.
    /// The waitier is added only if the event has not been added to the context already.
    /// </summary>
    /// <param name="EventName">Name of the event to wait for.</param>
    /// <param name="TaskCompletionSource">Task to be completed when the event is added.</param>
    /// <returns>true if the event has not been added to the context before and waiter was added, false otherwise.</returns>
    private bool AddEventWaiter(string EventName, TaskCompletionSource<bool> TaskCompletionSource)
    {
      log.Trace("(EventName:'{0}')", EventName);

      bool res = false;
      History.UseContext(this);

      lock (eventWaitersLock)
      {
        if (!usedEventsNames.Contains(EventName))
        {
          // Event has not been added to this context yet.
          HashSet<TaskCompletionSource<bool>> completionSources = null;
          if (!eventWaiters.TryGetValue(EventName, out completionSources))
          {
            completionSources = new HashSet<TaskCompletionSource<bool>>();
            eventWaiters.Add(EventName, completionSources);
          }
          completionSources.Add(TaskCompletionSource);
          res = true;
        }
        // else Event has been added to this context before, no waiter is added.
      }

      log.Trace("(-):{0}", res);
      return res;
    }


    /// <summary>
    /// Removes existing waiter for a specific event.
    /// </summary>
    /// <param name="EventName">Name of the event the waiter is waiting for.</param>
    /// <param name="TaskCompletionSource">Task of the waiter.</param>
    private void RemoveEventWaiter(string EventName, TaskCompletionSource<bool> TaskCompletionSource)
    {
      log.Trace("(EventName:'{0}')", EventName);

      lock (eventWaitersLock)
      {
        HashSet<TaskCompletionSource<bool>> completionSources = null;
        if (eventWaiters.TryGetValue(EventName, out completionSources))
        {
          if (!completionSources.Remove(TaskCompletionSource)) log.Error("Waiter not found.");
        }
      }

      log.Trace("(-)");
    }


    /// <summary>
    /// Sets new value for maximum number of events per context.
    /// </summary>
    /// <param name="Limit">Value to set.</param>
    public static void SetMaxEventsPerContext(int Limit)
    {
      clog.Trace("(Limit:{0})", Limit);

      maxEventsPerContext = Limit;

      clog.Trace("(-)");
    }


    public override string ToString()
    {
      return ToString("G");
    }


    /// <summary>
    /// Formats the value of the current instance using the specified format.
    /// </summary>
    /// <param name="Format">Type of format to use. Currently only "G" and "T" are supported.</param>
    /// <returns>Formatted string.</returns>
    public string ToString(string Format)
    {
      return ToString(Format, null);
    }

    /// <summary>
    /// Formats the value of the current instance using the specified format.
    /// </summary>
    /// <param name="Format">Type of format to use. Currently only "G" and "US" are supported.</param>
    /// <param name="Provider">The provider to use to format the value.</param>
    /// <returns>Formatted string.</returns>
    public string ToString(string Format, IFormatProvider Provider)
    {
      if (string.IsNullOrEmpty(Format)) Format = "G";
      Format = Format.Trim().ToUpperInvariant();
      if (Provider == null) Provider = CultureInfo.InvariantCulture;

      string res = "Invalid format";
      switch (Format)
      {
        case "G":
        case "T":
          bool wide = Format == "T";
          StringBuilder sb = new StringBuilder();
          sb.Append(Id);
          sb.Append(": ");
          if (wide) sb.Append("\n");
          if (events.Count > 0)
          {
            for (int i = 0; i < events.Count; i++)
            {
              ExecutionEvent ee = events[i];
              if (wide)
              {
                sb.AppendLine(string.Format(" -> {0} ({1})", ee.Name, ee.Timestamp.ToString("yyyy-MM-dd HH:mm:ss.ffff")));
              }
              else
              {
                sb.Append(ee.Name);
                if (i < events.Count - 1) sb.Append(" -> ");
              }
            }
          }
          else sb.Append("---");
          res = sb.ToString();
          break;

        default:
          res = "Invalid format";
          break;
      }

      return res;
    }
  }
}
