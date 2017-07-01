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
  /// History of execution contexts and their events.
  /// </summary>
  public static class History
  {
    /// <summary>Class logger.</summary>
    private static Logger log = new Logger("IopAppCore.ExecutionEvents.History");

    /// <summary>Maximum number of contexts to maintain.</summary>
    private static int maxContexts = 1000;
    /// <summary>Maximum number of contexts to maintain.</summary>
    public static int MaxContexts { get { return maxContexts; } }

    /// <summary>Lock object to protect access to history.</summary>
    private static object lockObject = new object();

    /// <summary>Recent historic events containing up to MaxHistoricEventLists mapped by their context ID.</summary>
    private static Dictionary<string, ContextEvents> historicEvents = new Dictionary<string, ContextEvents>(StringComparer.Ordinal);

    /// <summary>
    /// List of used context IDs.
    /// <para>
    /// The list contains at most MaxHistoricEventLists items. 
    /// Once it is full, inserting new item removes the oldest entry from both this list and HistoricEvents mapping.
    /// This is used solely to ensure HistoricEvents count does not exceed MaxHistoricEventLists.
    /// </para>    
    /// </summary>
    private static LinkedList<string> contextIdLruList = new LinkedList<string>();


    /// <summary>
    /// Sets new value for maximum number maintained contexts.
    /// </summary>
    /// <param name="Limit">Value to set.</param>
    public static void SetMaxContexts(int Limit)
    {
      log.Trace("(Limit:{0})", Limit);

      maxContexts = Limit;

      log.Trace("(-)");
    }


    /// <summary>
    /// Adds a new context to the history. This operation may remove an older context if the limit has been reached.
    /// </summary>
    /// <param name="Context">The new context to add.</param>
    /// <returns>New ContextEvents object created for the given context.</returns>
    public static ContextEvents AddContext(Context Context)
    {
      log.Trace("(Context.Id:'{0}')", Context.Id);
      ContextEvents res = null;

      lock (lockObject)
      {
        // If limit was reached, remove the first item (the one that was not used for the longest time).
        if (contextIdLruList.Count == maxContexts)
        {
          LinkedListNode<string> first = contextIdLruList.First;
          Context contextToRemove = historicEvents[first.Value].Context;
          RemoveContext(contextToRemove);
        }

        // Add new context as the last item to LRU list.
        LinkedListNode<string> lruLink = contextIdLruList.AddLast(Context.Id);
        Context.SetLruLink(lruLink);

        // Create new list of events in history for this context.
        res = new ContextEvents(Context);
        historicEvents.Add(Context.Id, res);
      }

      log.Trace("(-)");
      return res;
    }


    /// <summary>
    /// Removes existing context from the history.
    /// </summary>
    /// <param name="Context">Execution context to remove.</param>
    public static void RemoveContext(Context Context)
    {
      log.Trace("(Context.Id:'{0}')", Context.Id);

      lock (lockObject)
      {
        contextIdLruList.Remove(Context.LruLink);
        historicEvents.Remove(Context.Id);
        Context.SetLruLink(null);
      }

      log.Trace("(-)");
    }


    /// <summary>
    /// Adds new event to the context.
    /// </summary>
    /// <param name="Context">Execution context.</param>
    /// <param name="Event">Event to add.</param>
    /// <returns>true if the function succeeds, false otherwise.
    /// Adding exclusive event succeeds if the event was not added to this context before.
    /// Adding non-exclusive event will always succeeds.</returns>
    internal static bool AddEvent(Context Context, ExecutionEvent Event)
    {
      log.Trace("(Context.Id:'{0}',Event.Name:'{1}')", Context.Id, Event.Name);
      bool res = false;

      ContextEvents contextEvents = null;
      lock (lockObject)
      {
        bool existed = historicEvents.TryGetValue(Context.Id, out contextEvents);

        // If the context does not exist in history, it was probably removed due to inactivity and to add new event we have to create it again.
        if (!existed) contextEvents = AddContext(Context);

        res = contextEvents.AddEvent(Event);

        // If we did not just added the context (which means it is the last one in LRU), we want to make sure adding event puts the context to last position in LRU.
        if (existed)
        {
          contextIdLruList.Remove(Context.LruLink);
          contextIdLruList.AddLast(Context.LruLink);
        }
      }

      if (res) contextEvents.NotifyWaiters(Event);

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
      log.Trace("(Id:'{0}')", Id);

      Context res = null;
      ContextEvents contextEvents;
      if (historicEvents.TryGetValue(Id, out contextEvents)) res = contextEvents.Context;

      log.Trace("(-)");
      return res;
    }


    /// <summary>
    /// Finds context events for context of given ID.
    /// </summary>
    /// <param name="Id">Context identifier.</param>
    /// <returns>Existing context events or null if no context with the given identifier exists.</returns>
    public static ContextEvents GetContextEvents(string Id)
    {
      log.Trace("(Id:'{0}')", Id);

      ContextEvents res;
      if (!historicEvents.TryGetValue(Id, out res)) res = null;

      log.Trace("(-):{0}", res != null ? "ContextEvents" : "null");
      return res;
    }

    /// <summary>
    /// Obtains a copy of all context events.
    /// </summary>
    /// <returns>List of context events.</returns>
    public static List<ContextEvents> GetHistoricEvents()
    {
      return new List<ContextEvents>(historicEvents.Values);
    }

    /// <summary>
    /// Clears all history.
    /// </summary>
    public static void Clear()
    {
      log.Trace("()");

      Context.Clear();
      historicEvents.Clear();
      contextIdLruList.Clear();

      log.Trace("(-)");
    }


    /// <summary>
    /// Adds waiter for a specific event in a given context.
    /// </summary>
    /// <param name="Context">The context to wait for event in.</param>
    /// <param name="EventName">Name of the event to wait for.</param>
    /// <param name="TaskCompletionSource">Task to be completed when the event is added.</param>
    public static void AddEventWaiter(Context Context, string EventName, TaskCompletionSource<bool> TaskCompletionSource)
    {
      log.Trace("(Context.Id:'{0}',EventName:'{1}')", Context.Id, EventName);

      ContextEvents contextEvents = null;
      lock (lockObject)
      {
        bool existed = historicEvents.TryGetValue(Context.Id, out contextEvents);

        // If the context does not exist in history, it was probably removed due to inactivity and to add new event we have to create it again.
        if (!existed) contextEvents = AddContext(Context);
      }

      contextEvents.AddEventWaiter(EventName, TaskCompletionSource);

      log.Trace("(-)");
    }
  }
}
