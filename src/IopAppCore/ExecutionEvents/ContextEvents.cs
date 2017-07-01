using IopCommon;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;

namespace IopAppCore.ExecutionEvents
{
  /// <summary>
  /// Execution context with list of its events.
  /// </summary>
  public class ContextEvents
  {
    /// <summary>Class logger.</summary>
    private static Logger clog = new Logger("IopAppCore.ExecutionEvents.ContextEvents");

    /// <summary>Maximum number of events in a single context.</summary>
    /// <remarks>If a context reaches the limit, adding new event causes erasing arbitrary number of older events.</remarks>
    private static int maxEventsPerContext = 1000;
    /// <summary>Maximum number of events in a single context.</summary>
    /// <remarks>If a context reaches the limit, adding new event causes erasing arbitrary number of older events.</remarks>
    public static int MaxEventsPerContext { get { return maxEventsPerContext; } }

    /// <summary>Instance logger.</summary>
    private Logger log;

    /// <summary>Execution context.</summary>
    public Context Context;

    /// <summary>Lock object to protect access to events and exclusiveEventsNames.</summary>
    private object eventsLock = new object();

    /// <summary>List of events that occurred within this context.</summary>
    private List<ExecutionEvent> events;
    /// <summary>List of events that occurred within this context.</summary>
    public IReadOnlyList<ExecutionEvent> Events { get { return events.AsReadOnly(); } }

    /// <summary>List of exclusive events.</summary>
    /// <remarks>Note that </remarks>
    private HashSet<string> exclusiveEventsNames;


    /// <summary>Lock object to protect access to eventWaiters.</summary>
    private object eventWaitersLock = new object();

    /// <summary>List of tasks that should be completed when a particular event is added to the context.</summary>
    private Dictionary<string, List<TaskCompletionSource<bool>>> eventWaiters = new Dictionary<string, List<TaskCompletionSource<bool>>>(StringComparer.Ordinal);




    /// <summary>
    /// Initializes a new instance of the object.
    /// </summary>
    /// <param name="Context">Execution context.</param>
    public ContextEvents(Context Context)
    {
      log = new Logger("IopAppCore.ExecutionEvents.ContextEvents", $"[{Context.Id}] ");
      log.Trace("()");

      this.Context = Context;
      events = new List<ExecutionEvent>();
      exclusiveEventsNames = new HashSet<string>(StringComparer.Ordinal);

      log.Trace("(-)");
    }

    /// <summary>
    /// Completes all tasks that waited for this event.
    /// </summary>
    /// <param name="Event">Event that was added.</param>
    internal void NotifyWaiters(ExecutionEvent Event)
    {
      log.Trace("(Event.Name:'{0}')", Event.Name);
      lock (eventWaitersLock)
      {
        List<TaskCompletionSource<bool>> completionSources = null;
        if (eventWaiters.TryGetValue(Event.Name, out completionSources))
        {
          if (completionSources.Count > 0) log.Trace("{0} waiters notified.", completionSources.Count);

          foreach (TaskCompletionSource<bool> tcs in completionSources)
            tcs.TrySetResult(true);

          // Remove all waiters from the list.
          completionSources.Clear();
        }
      }
      log.Trace("(-)");
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
      lock (eventsLock)
      {
        // Check whether the exclusive event has already been added.
        if (Event.IsExclusive) res = !exclusiveEventsNames.Contains(Event.Name);
        else res = true;

        if (res)
        {
          events.Add(Event);
          if (Event.IsExclusive) exclusiveEventsNames.Add(Event.Name);
          if (events.Count > maxEventsPerContext) events.RemoveRange(0, maxEventsPerContext / 2);
        }
      }

      log.Trace("(-):{0}", res);
      return res;
    }


    /// <summary>
    /// Adds waiter for a specific event.
    /// </summary>
    /// <param name="EventName">Name of the event to wait for.</param>
    /// <param name="TaskCompletionSource">Task to be completed when the event is added.</param>
    public void AddEventWaiter(string EventName, TaskCompletionSource<bool> TaskCompletionSource)
    {
      log.Trace("(EventName:'{0}')", EventName);

      lock (eventWaitersLock)
      {
        List<TaskCompletionSource<bool>> completionSources = null;
        if (!eventWaiters.TryGetValue(EventName, out completionSources))
        {
          completionSources = new List<TaskCompletionSource<bool>>();
          eventWaiters.Add(EventName, completionSources);
        }
        completionSources.Add(TaskCompletionSource);
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
          sb.Append(Context.Id);
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
