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
    private static Dictionary<string, Context> historicEvents = new Dictionary<string, Context>(StringComparer.Ordinal);

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
    public static void AddContext(Context Context)
    {
      log.Trace("(Context.Id:'{0}')", Context.Id);

      lock (lockObject)
      {
        // If limit was reached, remove the first item (the one that was not used for the longest time).
        if (contextIdLruList.Count == maxContexts)
        {
          LinkedListNode<string> first = contextIdLruList.First;
          Context contextToRemove = historicEvents[first.Value];
          RemoveContextLocked(contextToRemove);
        }

        // Add new context as the last item to LRU list.
        LinkedListNode<string> lruLink = contextIdLruList.AddLast(Context.Id);
        Context.SetLruLink(lruLink);

        // Create new list of events in history for this context.
        historicEvents.Add(Context.Id, Context);
      }

      log.Trace("(-)");
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
        RemoveContextLocked(Context);
      }

      log.Trace("(-)");
    }


    /// <summary>
    /// Removes existing context from the history.
    /// </summary>
    /// <param name="Context">Execution context to remove.</param>
    /// <remarks>The caller is responsible for holding lockObject before calling this method.</remarks>
    public static void RemoveContextLocked(Context Context)
    {
      contextIdLruList.Remove(Context.LruLink);
      historicEvents.Remove(Context.Id);
      Context.SetLruLink(null);
    }


    /// <summary>
    /// Moves the context in the LRU to last position, which prevents its deletion for some time if there are too many contexts.
    /// </summary>
    /// <param name="Context">Execution context.</param>
    internal static void UseContext(Context Context)
    {
      log.Trace("(Context.Id:'{0}')", Context.Id);

      lock (lockObject)
      {
        bool existed = historicEvents.ContainsKey(Context.Id);

        // If the context does not exist in history, it was probably removed due to inactivity and to add new event we have to create it again.
        if (!existed) AddContext(Context);

        // If we did not just added the context (which means it is the last one in LRU), we want to make sure adding event puts the context to last position in LRU.
        if (existed)
        {
          contextIdLruList.Remove(Context.LruLink);
          contextIdLruList.AddLast(Context.LruLink);
        }
      }

      log.Trace("(-)");
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
      if (!historicEvents.TryGetValue(Id, out res)) res = null;

      log.Trace("(-)");
      return res;
    }


    /// <summary>
    /// Obtains a copy of all contexts.
    /// </summary>
    /// <returns>List of contexts.</returns>
    public static List<Context> GetHistoricContexts()
    {
      return new List<Context>(historicEvents.Values);
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
  }
}
